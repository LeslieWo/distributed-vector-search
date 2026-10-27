package dev.vectorsearch.common.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Create-if-missing for our topics; also reports existing topics whose partition count differs. */
public final class TopicInitializer {
    private static final Logger log = LoggerFactory.getLogger(TopicInitializer.class);

    private TopicInitializer() {}

    /** @return problems found (empty = all topics exist as designed) */
    public static List<String> ensure(String bootstrapServers, short replicationFactor) {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrapServers, "request.timeout.ms", 10_000))) {
            return ensure(admin, replicationFactor);
        }
    }

    public static List<String> ensure(Admin admin, short replicationFactor) {
        List<NewTopic> wanted = Topics.all(replicationFactor);
        for (NewTopic t : wanted) {
            try {
                admin.createTopics(List.of(t)).all().get(15, TimeUnit.SECONDS);
                log.info("created topic {} ({} partitions, rf {})", t.name(), t.numPartitions(), t.replicationFactor());
            } catch (ExecutionException e) {
                if (!(e.getCause() instanceof TopicExistsException)) throw new IllegalStateException("cannot create " + t.name(), e.getCause());
            } catch (Exception e) {
                throw new IllegalStateException("cannot create " + t.name(), e);
            }
        }
        List<String> problems = new ArrayList<>();
        try {
            Map<String, TopicDescription> desc = describeWithRetry(admin, wanted.stream().map(NewTopic::name).toList());
            for (NewTopic t : wanted) {
                int actual = desc.get(t.name()).partitions().size();
                if (actual != t.numPartitions()) {
                    // Never "fix" this automatically: changing the partition count remaps keys to partitions.
                    problems.add(t.name() + " has " + actual + " partitions, design says " + t.numPartitions());
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("cannot describe topics", e);
        }
        problems.forEach(p -> log.warn("topic layout mismatch: {}", p));
        return problems;
    }

    /** Right after creation the metadata may not have propagated yet (UnknownTopicOrPartition). */
    private static Map<String, TopicDescription> describeWithRetry(Admin admin, List<String> names) throws Exception {
        for (int attempt = 1; ; attempt++) {
            try {
                return admin.describeTopics(names).allTopicNames().get(15, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                if (!(e.getCause() instanceof UnknownTopicOrPartitionException) || attempt >= 20) throw e;
                Thread.sleep(250);
            }
        }
    }

    public static void main(String[] args) {
        List<String> problems = ensure(args.length > 0 ? args[0] : "localhost:9092", (short) 1);
        System.out.println(problems.isEmpty() ? "topics OK" : "problems: " + problems);
    }
}
