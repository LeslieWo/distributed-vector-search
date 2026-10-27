package dev.vectorsearch.ingest;

import dev.vectorsearch.common.chunk.Chunk;
import dev.vectorsearch.common.chunk.Chunker;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * {@code POST /documents} with {@code {"documents":[{"docId":"...","text":"..."}]}}: chunk every
 * document and put the chunks on raw-chunks. 202 Accepted = durable in Kafka, searchable once the
 * embed-worker has processed them (asynchronous).
 */
@RestController
public class DocumentController {
    public record Document(String docId, String text) {}

    public record IngestRequest(List<Document> documents) {}

    public record IngestResponse(int documents, int chunks, List<String> chunkIds) {}

    private final Chunker chunker = new Chunker();
    private final ChunkPublisher publisher;
    private final IngestProperties props;
    private final Counter chunksCounter;
    private final Counter docsCounter;

    public DocumentController(ChunkPublisher publisher, IngestProperties props, MeterRegistry registry) {
        this.publisher = publisher;
        this.props = props;
        this.chunksCounter = Counter.builder("vs.ingest.chunks").description("chunks published to raw-chunks").register(registry);
        this.docsCounter = Counter.builder("vs.ingest.documents").register(registry);
    }

    @PostMapping("/documents")
    public ResponseEntity<IngestResponse> ingest(@RequestBody IngestRequest req) {
        List<Document> docs = req == null || req.documents() == null ? List.of() : req.documents();
        if (docs.isEmpty()) throw new BadRequest("documents must be a non-empty list");
        if (docs.size() > props.maxDocumentsPerRequest()) throw new BadRequest("at most " + props.maxDocumentsPerRequest() + " documents per request");
        // Validate everything before publishing anything, so a bad document rejects the whole request.
        List<Chunk> chunks = new ArrayList<>();
        for (int i = 0; i < docs.size(); i++) {
            Document d = docs.get(i);
            if (d == null || d.docId() == null || d.docId().isBlank() || d.docId().contains("#")) {
                throw new BadRequest("documents[" + i + "].docId must be non-blank and must not contain '#'");
            }
            if (d.text() == null || d.text().isBlank()) throw new BadRequest("documents[" + i + "].text must be non-blank");
            if (d.text().length() > props.maxTextChars()) throw new BadRequest("documents[" + i + "].text longer than " + props.maxTextChars());
            chunks.addAll(chunker.chunk(d.docId(), d.text()));
        }
        publisher.publish(chunks);
        docsCounter.increment(docs.size());
        chunksCounter.increment(chunks.size());
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(new IngestResponse(docs.size(), chunks.size(), chunks.stream().map(c -> c.id().value()).toList()));
    }

    static class BadRequest extends RuntimeException {
        BadRequest(String msg) {
            super(msg);
        }
    }

    @ExceptionHandler(BadRequest.class)
    ResponseEntity<Map<String, String>> badRequest(BadRequest e) {
        return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
    }

    /** Kafka down or slow: nothing is acknowledged, the client may retry (re-sending is safe: same chunk ids). */
    @ExceptionHandler(ChunkPublisher.PublishException.class)
    ResponseEntity<Map<String, String>> unavailable(ChunkPublisher.PublishException e) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(Map.of("error", e.getMessage()));
    }
}
