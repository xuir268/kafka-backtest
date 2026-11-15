# Retrospective code walkthrough

This is a **reading and review sequence**, not a reconstruction of daily Git
commits. The author's pre-Git work period was August 1–September 18, 2026;
individual file dates within that period were not recorded. See the
[project history](project-history.md) for the distinction between that work
period and the repository's actual commits.

## 1. Java implementation

Start with the fixture model: [KafkaRecord](../harness-jvm/fixture-io/src/main/java/kbt/fixture/KafkaRecord.java),
[Header](../harness-jvm/fixture-io/src/main/java/kbt/fixture/Header.java), and
[Segment](../harness-jvm/fixture-io/src/main/java/kbt/fixture/Segment.java).
Then follow the bytes through [SegmentEncoder](../harness-jvm/fixture-io/src/main/java/kbt/fixture/SegmentEncoder.java),
[SegmentDecoder](../harness-jvm/fixture-io/src/main/java/kbt/fixture/SegmentDecoder.java),
and [DeliveryOrder](../harness-jvm/fixture-io/src/main/java/kbt/fixture/DeliveryOrder.java).
The layout they implement is documented in the
[fixture format](../spec/fixture-format.md).

Next, trace a replay through [InputSource](../harness-jvm/harness-core/src/main/java/kbt/harness/InputSource.java)
→ [RecordProcessor](../harness-jvm/harness-core/src/main/java/kbt/harness/RecordProcessor.java)
→ [ReplayRunner](../harness-jvm/harness-core/src/main/java/kbt/harness/ReplayRunner.java)
→ [OutputSink](../harness-jvm/harness-core/src/main/java/kbt/harness/OutputSink.java).
[SegmentComparer](../harness-jvm/diff-engine/src/main/java/kbt/diff/SegmentComparer.java)
checks baseline and candidate outputs by key. The
[StreamsProcessor](../harness-jvm/harness-kafka-streams/src/main/java/kbt/streams/StreamsProcessor.java)
adapts Kafka Streams jobs; [Main](../harness-jvm/cli/src/main/java/kbt/cli/Main.java)
exposes the CLI, and [Dashboard](../harness-jvm/cli/src/main/java/kbt/cli/Dashboard.java)
renders the run comparison.

## 2. Java tests

Read [SegmentDecoderTest](../harness-jvm/fixture-io/src/test/java/kbt/fixture/SegmentDecoderTest.java)
first for valid frames, null/empty values, headers, order, and malformed input.
Then read [InputSourceTest](../harness-jvm/harness-core/src/test/java/kbt/harness/InputSourceTest.java),
[ReplayRunnerTest](../harness-jvm/harness-core/src/test/java/kbt/harness/ReplayRunnerTest.java),
[OutputSinkTest](../harness-jvm/harness-core/src/test/java/kbt/harness/OutputSinkTest.java),
and [SegmentComparerTest](../harness-jvm/diff-engine/src/test/java/kbt/diff/SegmentComparerTest.java).
The [CLI tests](../harness-jvm/cli/src/test/java/kbt/cli/MainTest.java)
cover the command surface and dashboard. Run them from `harness-jvm` with
`./gradlew test`.

## 3. C++ implementation

The [C ABI header](../core-native/include/kbt_core.h) is the boundary intended
for both native callers and a future Java Panama binding. Read its ownership
rule before the [segment reader](../core-native/src/segment.cpp): opening a
segment copies the input, and returned byte slices live until the handle is
closed. The reader validates the header CRC, lengths, offsets, flags, headers,
and padding before exposing records. [CMake](../core-native/CMakeLists.txt)
builds the shared library.

## 4. C++ tests

[segment_test.cpp](../core-native/tests/segment_test.cpp) exercises a valid
segment, an empty segment, null fields, and hostile lengths or metadata. Run
`make native-test` from the repository root. A Java-generated sample segment
has also been decoded by the native library. Cross-segment native merge,
Panama binding, and the native replay harness are still future work.
