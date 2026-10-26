package dev.vectorsearch.common.dataset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TsvCorpusTest {

    @Test
    void readsIdTabTextRowsAndSkipsMalformedOnes(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("c.tsv");
        Files.writeString(file, "0\tThe presence of communication.\nbroken line\n7\t  Café au lait  \n9\t\n");
        try (Stream<Passage> s = TsvCorpus.stream(file)) {
            List<Passage> rows = s.toList();
            assertEquals(List.of(new Passage("0", "The presence of communication."), new Passage("7", "Café au lait")), rows);
        }
    }

    @Test
    void parseRejectsMissingIdOrText() {
        assertNull(TsvCorpus.parse("\ttext"));
        assertNull(TsvCorpus.parse("12\t"));
    }
}
