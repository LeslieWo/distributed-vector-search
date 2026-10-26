package dev.vectorsearch.common.dataset;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

/**
 * Reader for MS MARCO style {@code id<TAB>text} files: {@code collection.tsv} (passages)
 * and {@code queries.*.tsv} (queries) share the same layout.
 */
public final class TsvCorpus {
    private TsvCorpus() {}

    /** Lazily streams rows; the caller must close the stream. Malformed rows are skipped. */
    public static Stream<Passage> stream(Path file) {
        BufferedReader reader;
        try {
            reader = Files.newBufferedReader(file, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return reader.lines()
                .map(TsvCorpus::parse)
                .filter(p -> p != null)
                .onClose(() -> {
                    try {
                        reader.close();
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
    }

    static Passage parse(String line) {
        int tab = line.indexOf('\t');
        if (tab <= 0 || tab == line.length() - 1) return null;
        return new Passage(line.substring(0, tab), line.substring(tab + 1).strip());
    }
}
