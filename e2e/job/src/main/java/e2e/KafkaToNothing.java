package e2e;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * Reads one or more topics and throws the records away. A per-record sleep makes the job slow on
 * purpose, so that a burst of input leaves records pending in the source for a measurable while.
 * With --starting-offsets committed and --auto-commit-ms it is the stateless job ADR 15 is about:
 * no checkpoints, its position in the consumer group, and --out keeps every record each run
 * read in a topic, so a replay shows up as a duplicate.
 * Same source code on Flink 1.20 and 2.x; only the connector version differs.
 */
public final class KafkaToNothing {
    public static void main(String[] args) throws Exception {
        Map<String, String> a = new HashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) {
            a.put(args[i], args[i + 1]);
        }
        String bootstrap = a.getOrDefault("--bootstrap", "kafka:9092");
        String[] topics = a.getOrDefault("--topics", "e2e-in").split(",");
        String group = a.getOrDefault("--group", "e2e");
        long sleepMs = Long.parseLong(a.getOrDefault("--sleep-ms", "0"));
        // pendingRecords counts what the source has not fetched yet, and the reader prefetches a
        // couple of poll batches. A small batch keeps a burst of input visibly pending.
        String maxPoll = a.getOrDefault("--max-poll-records", "500");
        boolean fromGroup = "committed".equals(a.getOrDefault("--starting-offsets", "earliest"));
        long autoCommitMs = Long.parseLong(a.getOrDefault("--auto-commit-ms", "0"));
        String out = a.get("--out");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSourceBuilder<String> builder = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(Arrays.asList(topics))
                .setGroupId(group)
                .setStartingOffsets(fromGroup
                        ? OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST)
                        : OffsetsInitializer.earliest())
                .setProperty("max.poll.records", maxPoll)
                .setValueOnlyDeserializer(new SimpleStringSchema());
        if (autoCommitMs > 0) {
            builder.setProperty("enable.auto.commit", "true")
                    .setProperty("auto.commit.interval.ms", Long.toString(autoCommitMs));
        }
        KafkaSource<String> source = builder.build();
        DataStream<String> read = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .map(v -> {
                    if (sleepMs > 0) {
                        Thread.sleep(sleepMs);
                    }
                    return v;
                });
        if (out == null) {
            read.sinkTo(new DiscardingSink<>());
        } else {
            read.sinkTo(KafkaSink.<String>builder()
                    .setBootstrapServers(bootstrap)
                    .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                            .setTopic(out)
                            .setValueSerializationSchema(new SimpleStringSchema())
                            .build())
                    .build());
        }
        env.execute("kafka-to-nothing");
    }
}
