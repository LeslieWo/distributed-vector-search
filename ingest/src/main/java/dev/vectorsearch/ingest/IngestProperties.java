package dev.vectorsearch.ingest;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "ingest")
public record IngestProperties(
        String bootstrapServers,
        short replicationFactor,
        int maxDocumentsPerRequest,
        int maxTextChars,
        Duration sendTimeout) {

    public IngestProperties {
        if (bootstrapServers == null || bootstrapServers.isBlank()) bootstrapServers = "localhost:9092";
        if (replicationFactor <= 0) replicationFactor = 1;
        if (maxDocumentsPerRequest <= 0) maxDocumentsPerRequest = 1000;
        if (maxTextChars <= 0) maxTextChars = 1_000_000;
        if (sendTimeout == null) sendTimeout = Duration.ofSeconds(30);
    }
}
