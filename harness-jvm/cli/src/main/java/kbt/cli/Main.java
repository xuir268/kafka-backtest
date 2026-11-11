package kbt.cli;

import java.io.IOException;
import java.io.InputStream;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import kbt.diff.Divergence;
import kbt.diff.SegmentComparer;
import kbt.fixture.Header;
import kbt.fixture.KafkaRecord;
import kbt.fixture.MalformedFixtureException;
import kbt.fixture.Segment;
import kbt.fixture.SegmentDecoder;
import kbt.fixture.SegmentEncoder;
import kbt.harness.InputSource;
import kbt.harness.OutputSink;
import kbt.harness.RecordProcessor;
import kbt.harness.ReplayRunner;
import kbt.streams.StreamsProcessor;
import kbt.streams.TopologyProvider;

/**
 * Command-line entry point. Parses arguments and calls the library;
 * it holds no fixture or diff logic of its own.
 *
 * Commands:
 *   kbt sample <dir>                       write a small example segment into dir
 *   kbt inspect <file>                     print a segment's records in replay order
 *   kbt compare <baseline> <candidate>     report where two segments differ
 *   kbt replay <input.seg> <topic> <dir>     replay one segment through the identity processor
 *   kbt capture <bootstrap> <topic> <partition> <file.seg> capture one Kafka partition
 *   kbt replay-streams <input.seg> <input-topic> <output-topic> <provider-class> <dir>
 *
 * Exit codes: 0 ok or identical, 1 divergent or failed, 2 usage,
 * 3 cannot read an input file.
 */
public final class Main {

    private static final String SAMPLE_FILE = "partition-0.seg";
    private static final int MAX_PRINTED_DIVERGENCES = 20;

    private Main() {
    }

    public static void main(String[] args) {
        if (args.length == 2 && "inspect".equals(args[0])) {
            System.exit(inspect(Path.of(args[1])));
        }
        if (args.length == 2 && "sample".equals(args[0])) {
            System.exit(sample(Path.of(args[1])));
        }
        if (args.length == 4 && "load-sample".equals(args[0])) {
            System.exit(loadSample(Integer.parseInt(args[1]), Integer.parseInt(args[2]), Path.of(args[3])));
        }
        if (args.length == 3 && "compare".equals(args[0])) {
            System.exit(compare(Path.of(args[1]), Path.of(args[2])));
        }
        if (args.length == 4 && "dashboard".equals(args[0])) {
            System.exit(dashboard(Path.of(args[1]), Path.of(args[2]), Path.of(args[3])));
        }
        if (args.length == 4 && "replay".equals(args[0])) {
            System.exit(replay(Path.of(args[1]), args[2], Path.of(args[3])));
        }
        if (args.length == 5 && "capture".equals(args[0])) {
            System.exit(capture(args[1], args[2], args[3], Path.of(args[4])));
        }
        if (args.length == 6 && "replay-streams".equals(args[0])) {
            System.exit(replayStreams(Path.of(args[1]), args[2], args[3], args[4], Path.of(args[5]), null));
        }
        if (args.length == 7 && "replay-streams".equals(args[0])) {
            System.exit(replayStreams(Path.of(args[1]), args[2], args[3], args[4], Path.of(args[5]), Path.of(args[6])));
        }
        System.err.println("usage: kbt sample <dir> | kbt load-sample <count> <value-bytes> <file.seg> | kbt inspect <file> | "
                + "kbt compare <baseline> <candidate> | kbt dashboard <run-a> <run-b> <file.html> | kbt replay <input.seg> <topic> <output-dir> | "
                + "kbt capture <bootstrap> <topic> <partition> <file.seg> | "
                + "kbt replay-streams <input.seg> <input-topic> <output-topic> <provider-class> <output-dir> [provider.jar]");
        System.exit(2);
    }

    /** Writes a four-record segment that exercises a timestamp tie, headers, and a tombstone. */
    static int sample(Path dir) {
        List<KafkaRecord> records = List.of(
                record(100, 1000, "k1", "first", List.of()),
                record(101, 1005, "k2", "second", List.of(
                        new Header(buf("source"), buf("web")),
                        new Header(buf("source"), buf("mobile")))),
                record(102, 1000, "k1", "tie with first", List.of()),
                new KafkaRecord(0, 103, 1010, false, buf("k1"), false,
                        ByteBuffer.allocate(0), true, List.of()));

        try {
            Files.createDirectories(dir);
            Path out = dir.resolve(SAMPLE_FILE);
            if (Files.exists(out)) {
                System.err.println("refusing to overwrite " + out);
                return 1;
            }
            Files.write(out, SegmentEncoder.encode(0, records));
            System.out.println("wrote " + out + " (" + records.size() + " records)");
            return 0;
        } catch (IOException e) {
            System.err.println("cannot write sample: " + e.getMessage());
            return 1;
        }
    }

    /** Creates a repeatable one-partition load fixture with four alternating keys. */
    static int loadSample(int count, int valueBytes, Path file) {
        if (count < 1 || valueBytes < 1 || valueBytes > 65_536) {
            System.err.println("count must be positive and value-bytes must be 1..65536");
            return 2;
        }
        if (Files.exists(file)) {
            System.err.println("refusing to overwrite " + file);
            return 1;
        }
        List<KafkaRecord> records = new ArrayList<>(count);
        byte[] payload = new byte[valueBytes];
        Arrays.fill(payload, (byte) 'x');
        for (int i = 0; i < count; i++) {
            records.add(new KafkaRecord(0, i, 1_000 + i, false,
                    buf("key-" + (i % 4)), false, ByteBuffer.wrap(payload), false, List.of()));
        }
        try {
            Path parent = file.toAbsolutePath().getParent();
            Files.createDirectories(parent);
            Files.write(file, SegmentEncoder.encode(0, records));
            System.out.println("wrote " + count + " records to " + file);
            return 0;
        } catch (IOException error) {
            System.err.println("cannot write load sample: " + error.getMessage());
            return 1;
        }
    }

    /** Prints a segment's records in replay order. Returns the process exit code. */
    static int inspect(Path file) {
        byte[] bytes;
        try {
            bytes = Files.readAllBytes(file);
        } catch (IOException e) {
            System.err.println("cannot read " + file + ": " + e.getMessage());
            return 1;
        }

        try {
            Segment seg = SegmentDecoder.decode(ByteBuffer.wrap(bytes));
            System.out.printf("partition %d, first offset %d, %d records%n",
                    seg.partition(), seg.firstOffset(), seg.records().size());
            for (KafkaRecord r : seg.records()) {
                System.out.printf("ts=%d partition=%d offset=%d key=%s value=%s headers=%d%n",
                        r.timestampMs(), r.partition(), r.offset(),
                        show(r.keyNull(), r.key()), show(r.valueNull(), r.value()),
                        r.headers().size());
            }
            return 0;
        } catch (MalformedFixtureException e) {
            System.err.println("malformed fixture: " + e.getMessage());
            return 1;
        }
    }

    /** Replays one input segment through the identity processor. */
    static int replay(Path inputFile, String topic, Path outputDir) {
        try {
            if (Files.exists(outputDir)) {
                System.err.println("refusing to overwrite output directory " + outputDir);
                return 1;
            }
            InputSource source = new InputSource(inputFile);
            OutputSink sink = new OutputSink(topic);
            return reportReplay(ReplayRunner.run(source, RecordProcessor.identity(), sink, outputDir), outputDir,
                    "identity", "not recorded", "not recorded", inputFile, source, "not recorded", topic);
        } catch (IOException error) {
            System.err.println("cannot read " + inputFile + ": " + error.getMessage());
            return 3;
        } catch (MalformedFixtureException | IllegalArgumentException error) {
            System.err.println("cannot replay: " + error.getMessage());
            return 1;
        }
    }

    /** Replays a segment through a selected Kafka Streams topology version. */
    static int replayStreams(Path inputFile, String inputTopic, String outputTopic,
                             String providerClass, Path outputDir) {
        return replayStreams(inputFile, inputTopic, outputTopic, providerClass, outputDir, null);
    }

    static int replayStreams(Path inputFile, String inputTopic, String outputTopic,
                             String providerClass, Path outputDir, Path providerJar) {
        try {
            if (Files.exists(outputDir)) {
                System.err.println("refusing to overwrite output directory " + outputDir);
                return 1;
            }
            try (URLClassLoader loader = providerJar == null ? null : new URLClassLoader(
                    new java.net.URL[]{providerJar.toUri().toURL()}, Main.class.getClassLoader())) {
                Class<?> loaded = Class.forName(providerClass, true,
                        loader == null ? Main.class.getClassLoader() : loader);
                TopologyProvider provider = (TopologyProvider) loaded.getDeclaredConstructor().newInstance();
                InputSource source = new InputSource(inputFile);
                OutputSink sink = new OutputSink(outputTopic);
                try (StreamsProcessor processor = new StreamsProcessor(provider, inputTopic, outputTopic)) {
                    return reportReplay(ReplayRunner.run(source, processor, sink, outputDir), outputDir,
                            providerClass, classHash(loaded), providerJar == null ? "not recorded" : sha256(Files.readAllBytes(providerJar)),
                            inputFile, source, inputTopic, outputTopic);
                }
            }
        } catch (IOException error) {
            System.err.println("cannot read " + inputFile + ": " + error.getMessage());
            return 3;
        } catch (ReflectiveOperationException | RuntimeException error) {
            System.err.println("Streams replay failed: " + error.getMessage());
            return 1;
        }
    }

    private static int reportReplay(ReplayRunner.Result result, Path outputDir,
                                    String providerClass, String classSha256, String artifactSha256,
                                    Path inputFile, InputSource source, String inputTopic, String outputTopic) {
        try {
            Files.createDirectories(outputDir);
            ReplayRunner.Performance p = result.performance();
            long failures = result.writeResult() == null ? 0 : result.writeResult().failureCount();
            List<KafkaRecord> inputs = source.records();
            String sourceInfo = captureSource(inputFile);
            String json = "{\n  \"outcome\": \"" + result.outcome().name().toLowerCase() + "\",\n"
                    + "  \"input_fixture\": \"" + jsonEscape(inputFile.toAbsolutePath().toString()) + "\",\n"
                    + "  \"source_bootstrap\": \"" + jsonEscape(sourceInfo) + "\",\n"
                    + "  \"producer_identity\": \"not recorded\",\n"
                    + "  \"input_topic\": \"" + jsonEscape(inputTopic) + "\",\n"
                    + "  \"input_partition\": " + (inputs.isEmpty() ? -1 : inputs.getFirst().partition()) + ",\n"
                    + "  \"input_offset_start\": " + (inputs.isEmpty() ? -1 : inputs.getFirst().offset()) + ",\n"
                    + "  \"input_offset_end_exclusive\": " + (inputs.isEmpty() ? -1 : inputs.getLast().offset() + 1) + ",\n"
                    + "  \"consumer\": \"" + jsonEscape(providerClass) + "\",\n"
                    + "  \"output_topic\": \"" + jsonEscape(outputTopic) + "\",\n"
                    + "  \"output_offset_type\": \"synthetic, per-partition sequence from zero\",\n"
                    + "  \"topics_created\": false,\n"
                    + "  \"broker_restart_ms\": null,\n"
                    + "  \"memory_alignment_bytes\": null,\n"
                    + "  \"provider_class\": \"" + jsonEscape(providerClass) + "\",\n"
                    + "  \"class_sha256\": \"" + jsonEscape(classSha256) + "\",\n"
                    + "  \"artifact_sha256\": \"" + jsonEscape(artifactSha256) + "\",\n"
                    + "  \"vcs_revision\": \"" + jsonEscape(System.getProperty("kbt.vcsRevision", "not recorded")) + "\",\n"
                    + "  \"jvm_version\": \"" + jsonEscape(System.getProperty("java.version")) + "\",\n"
                    + "  \"jvm_vendor\": \"" + jsonEscape(System.getProperty("java.vendor")) + "\",\n"
                    + "  \"jvm_max_heap_bytes\": " + Runtime.getRuntime().maxMemory() + ",\n"
                    + "  \"available_processors\": " + Runtime.getRuntime().availableProcessors() + ",\n"
                    + "  \"input_records_delivered\": " + result.inputRecordsDelivered() + ",\n"
                    + "  \"input_records_processed\": " + result.inputRecordsProcessed() + ",\n"
                    + "  \"output_records_accepted\": " + result.outputRecordsAccepted() + ",\n"
                    + "  \"output_records_written\": " + result.outputRecordsWritten() + ",\n"
                    + "  \"sink_write_failures\": " + failures + ",\n"
                    + "  \"elapsed_ns\": " + p.elapsedNanos() + ",\n"
                    + "  \"throughput_records_per_second\": "
                    + (p.elapsedNanos() == 0 ? 0 : result.inputRecordsProcessed() * 1_000_000_000.0 / p.elapsedNanos()) + ",\n"
                    + "  \"gc_collection_ms\": " + p.gcCollectionMillis() + ",\n"
                    + "  \"gc_collections\": " + p.gcCollections() + ",\n"
                    + "  \"peak_observed_heap_bytes\": " + p.peakObservedHeapBytes() + ",\n"
                    + "  \"mean_service_ns\": " + p.meanServiceNanos() + ",\n"
                    + "  \"service_cv\": " + p.serviceCv() + "\n}\n";
            Files.writeString(outputDir.resolve("metrics.json"), json);
        } catch (IOException error) {
            System.err.println("cannot write metrics: " + error.getMessage());
            return 1;
        }
        if (result.processingFailure() != null) {
            System.err.println("processing failed: " + result.processingFailure());
            return 1;
        }
        if (result.writeResult() != null) {
            for (OutputSink.WriteFailure failure : result.writeResult().failures()) {
                System.err.println("cannot write " + failure.file() + ": " + failure.cause().getMessage());
            }
        }
        if (result.outcome() == ReplayRunner.Outcome.FAILED) {
            return 1;
        }
        System.out.printf("replayed %d input records; wrote %d output records to %s%n",
                result.inputRecordsProcessed(), result.outputRecordsWritten(), outputDir);
        return 0;
    }

    private static String captureSource(Path inputFile) {
        Path sidecar = inputFile.resolveSibling(inputFile.getFileName() + ".source.json");
        if (!Files.exists(sidecar)) return "not recorded";
        try {
            java.util.regex.Matcher matcher = java.util.regex.Pattern.compile("\\\"bootstrap_servers\\\"\\s*:\\s*\\\"([^\\\"]+)\\\"")
                    .matcher(Files.readString(sidecar));
            return matcher.find() ? matcher.group(1) : "not recorded";
        } catch (IOException error) {
            return "not recorded";
        }
    }

    private static String classHash(Class<?> type) throws IOException {
        try (InputStream in = type.getResourceAsStream(type.getSimpleName() + ".class")) {
            if (in == null) throw new IOException("cannot read provider bytecode: " + type.getName());
            return sha256(in.readAllBytes());
        }
    }

    private static String sha256(byte[] bytes) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(bytes);
            return java.util.HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static String jsonEscape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
    }

    static int dashboard(Path firstRun, Path secondRun, Path output) {
        try {
            Dashboard.write(firstRun, secondRun, output);
            System.out.println("wrote dashboard to " + output);
            return 0;
        } catch (IOException error) {
            System.err.println("cannot create dashboard: " + error.getMessage());
            return 1;
        }
    }

    /** Captures the records currently available in one Kafka partition. */
    static int capture(String bootstrap, String topic, String partitionText, Path outputFile) {
        try {
            int count = KafkaCapture.capture(bootstrap, topic, Integer.parseInt(partitionText), outputFile);
            System.out.printf("captured %d records to %s%n", count, outputFile);
            return 0;
        } catch (IOException | RuntimeException error) {
            System.err.println("capture failed: " + error.getMessage());
            return 1;
        }
    }

    /** Compares two segments and prints divergences. Returns the process exit code. */
    static int compare(Path baselineFile, Path candidateFile) {
        Segment baseline;
        Segment candidate;
        try {
            baseline = read(baselineFile);
            candidate = read(candidateFile);
        } catch (IOException e) {
            System.err.println("cannot read input: " + e.getMessage());
            return 3;
        } catch (MalformedFixtureException e) {
            System.err.println("malformed fixture: " + e.getMessage());
            return 1;
        }

        List<Divergence> diffs = SegmentComparer.compare(baseline, candidate);
        if (diffs.isEmpty()) {
            System.out.println("identical: " + baseline.records().size() + " records");
            return 0;
        }

        System.out.println(diffs.size() + " divergence(s)");
        int shown = Math.min(diffs.size(), MAX_PRINTED_DIVERGENCES);
        for (Divergence d : diffs.subList(0, shown)) {
            System.out.println(describe(d));
        }
        if (diffs.size() > shown) {
            System.out.println("... " + (diffs.size() - shown) + " more not shown");
        }
        return 1;
    }

    private static Segment read(Path file) throws IOException {
        return SegmentDecoder.decode(ByteBuffer.wrap(Files.readAllBytes(file)));
    }

    private static String describe(Divergence d) {
        return switch (d.kind()) {
            case FIELD -> "index " + d.index() + " " + d.field()
                    + ": baseline=" + d.baseline() + " candidate=" + d.candidate();
            case MISSING_IN_CANDIDATE -> "index " + d.index() + " missing in candidate: " + d.baseline();
            case EXTRA_IN_CANDIDATE -> "index " + d.index() + " extra in candidate: " + d.candidate();
        };
    }

    private static KafkaRecord record(long offset, long ts, String key, String value, List<Header> headers) {
        return new KafkaRecord(0, offset, ts, false, buf(key), false, buf(value), false, headers);
    }

    private static ByteBuffer buf(String s) {
        return ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8));
    }

    private static String show(boolean isNull, ByteBuffer bytes) {
        if (isNull) {
            return "null";
        }
        ByteBuffer copy = bytes.duplicate();
        byte[] raw = new byte[copy.remaining()];
        copy.get(raw);
        return new String(raw, StandardCharsets.UTF_8);
    }
}
