package kbt.harness;

import java.nio.file.Path;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Objects;
import kbt.fixture.KafkaRecord;

/** Runs one input segment through a processor and writes its output segments. */
public final class ReplayRunner {

    private ReplayRunner() {
    }

    /**
     * Runs on the caller's thread. A processing failure stops the replay before
     * segment writing; a write failure is returned with its path and cause.
     */
    public static Result run(InputSource source, RecordProcessor processor,
                             OutputSink sink, Path outputDir) {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(processor, "processor");
        Objects.requireNonNull(sink, "sink");
        Objects.requireNonNull(outputDir, "outputDir");

        long delivered = 0;
        long processed = 0;
        long started = System.nanoTime();
        long gcBefore = gcMillis();
        long gcCountBefore = gcCount();
        long heapPeak = heapUsed();
        double serviceMean = 0;
        double serviceM2 = 0;
        try {
            for (KafkaRecord input : source.records()) {
                delivered++;
                long serviceStarted = System.nanoTime();
                List<KafkaRecord> outputs = Objects.requireNonNull(processor.process(input),
                        "processor returned null outputs");
                for (KafkaRecord output : outputs) {
                    sink.accept(output);
                }
                processed++;
                double serviceNanos = System.nanoTime() - serviceStarted;
                double delta = serviceNanos - serviceMean;
                serviceMean += delta / processed;
                serviceM2 += delta * (serviceNanos - serviceMean);
                heapPeak = Math.max(heapPeak, heapUsed());
            }
        } catch (RuntimeException error) {
            return new Result(Outcome.FAILED, delivered, processed,
                    sink.recordsAccepted(), sink.recordsWritten(), null, error,
                    new Performance(System.nanoTime() - started, Math.max(0, gcMillis() - gcBefore),
                            Math.max(0, gcCount() - gcCountBefore),
                            Math.max(heapPeak, heapUsed()), serviceMean,
                            serviceMean == 0 ? 0 : Math.sqrt(serviceM2 / processed) / serviceMean));
        }

        OutputSink.WriteResult writeResult = sink.writeSegments(outputDir);
        Outcome outcome = writeResult.succeeded() ? Outcome.COMPLETED : Outcome.FAILED;
        return new Result(outcome, delivered, processed,
                sink.recordsAccepted(), sink.recordsWritten(), writeResult, null,
                new Performance(System.nanoTime() - started, Math.max(0, gcMillis() - gcBefore),
                        Math.max(0, gcCount() - gcCountBefore),
                        Math.max(heapPeak, heapUsed()), serviceMean,
                        serviceMean == 0 ? 0 : Math.sqrt(serviceM2 / processed) / serviceMean));
    }

    private static long gcMillis() {
        long total = 0;
        for (GarbageCollectorMXBean collector : ManagementFactory.getGarbageCollectorMXBeans()) {
            long time = collector.getCollectionTime();
            if (time > 0) total += time;
        }
        return total;
    }

    private static long heapUsed() {
        return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
    }

    private static long gcCount() {
        long total = 0;
        for (GarbageCollectorMXBean collector : ManagementFactory.getGarbageCollectorMXBeans()) {
            long count = collector.getCollectionCount();
            if (count > 0) total += count;
        }
        return total;
    }

    public enum Outcome {
        COMPLETED,
        FAILED
    }

    /** writeResult is null when processing fails before any segment write. */
    public record Result(Outcome outcome, long inputRecordsDelivered, long inputRecordsProcessed,
                         long outputRecordsAccepted, long outputRecordsWritten,
                         OutputSink.WriteResult writeResult, RuntimeException processingFailure,
                         Performance performance) {
    }

    public record Performance(long elapsedNanos, long gcCollectionMillis,
                              long gcCollections, long peakObservedHeapBytes,
                              double meanServiceNanos, double serviceCv) {
    }
}
