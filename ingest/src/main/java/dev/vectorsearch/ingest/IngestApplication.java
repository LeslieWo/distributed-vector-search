package dev.vectorsearch.ingest;

import dev.vectorsearch.common.kafka.TopicInitializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
@EnableConfigurationProperties(IngestProperties.class)
public class IngestApplication {
    public static void main(String[] args) {
        SpringApplication.run(IngestApplication.class, args);
    }

    /** Topics are created before the producer exists, so the first request never hits a missing topic. */
    @Bean
    ChunkPublisher chunkPublisher(IngestProperties props) {
        TopicInitializer.ensure(props.bootstrapServers(), props.replicationFactor());
        return new ChunkPublisher(props);
    }
}
