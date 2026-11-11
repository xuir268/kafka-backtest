package kbt.cli;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import kbt.fixture.KafkaRecord;
import kbt.fixture.SegmentDecoder;

/** Writes a self-contained HTML comparison of two replay metric files. */
final class Dashboard {
    private Dashboard() { }

    static void write(Path firstRun, Path secondRun, Path output) throws IOException {
        if (Files.exists(output)) throw new IOException("refusing to overwrite " + output);
        Metrics a = read(firstRun.resolve("metrics.json"));
        Metrics b = read(secondRun.resolve("metrics.json"));
        List<KafkaRecord> recordsA = outputRecords(firstRun);
        List<KafkaRecord> recordsB = outputRecords(secondRun);
        String nameA = escape(firstRun.getFileName().toString());
        String nameB = escape(secondRun.getFileName().toString());
        String html = """
                <!doctype html>
                <html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
                <title>Kafka Backtest | Run comparison</title>
                <style>
                :root{color-scheme:dark;font-family:Inter,ui-sans-serif,system-ui,sans-serif;background:#111217;color:#e8eef8;--baseline:#73bf69;--candidate:#ff9830}
                *{box-sizing:border-box}body{margin:0;min-height:100vh;background:radial-gradient(circle at 15% 0%,#1e3652 0%,#101827 45%)}
                main{max-width:1120px;margin:auto;padding:48px 24px 80px}.eyebrow{color:#6ce0c3;text-transform:uppercase;letter-spacing:.16em;font-size:12px;font-weight:800}
                h1{font-size:clamp(32px,5vw,54px);line-height:1.05;margin:12px 0}p{color:#aebdd0;line-height:1.55}.intro{max-width:760px}
                .runs,.grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:16px}.runs{margin:36px 0 22px}
                .run,.card,.note{background:#182438;border:1px solid #2b405b;border-radius:18px;padding:22px}.run small,.label{color:#9aacc2;font-size:13px}.run strong{display:block;margin-top:8px;font-size:20px;overflow-wrap:anywhere}
                .run.a{border-left:4px solid var(--baseline)}.run.b{border-left:4px solid var(--candidate)}.grid{grid-template-columns:repeat(3,minmax(0,1fr))}
                .card h2{font-size:14px;font-weight:600;color:#aebdd0;margin:0 0 20px}.values{display:flex;justify-content:space-between;gap:12px;font-variant-numeric:tabular-nums}
                .values div{min-width:0}.values small{display:block;color:#8fa3bb;margin-bottom:5px}.values strong{font-size:24px;white-space:nowrap}.values div:first-child strong{color:var(--baseline)}.values div:last-child strong{color:var(--candidate)}
                .track{height:7px;background:#28384d;border-radius:99px;margin-top:12px;overflow:hidden}.fill{height:100%;border-radius:99px}.fill.a{background:var(--baseline)}.fill.b{background:var(--candidate)}
                .note{margin-top:16px}.note strong{color:#e8eef8}.note p{margin-bottom:0}.footer{font-size:13px;margin-top:24px}
                .meta{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:16px;margin:18px 0 26px}.meta-panel{background:#182438;border:1px solid #2b405b;border-radius:18px;padding:22px;min-width:0}.meta-panel h3{margin:0 0 14px}.meta-row{padding:8px 0;border-top:1px solid #2b405b}.meta-row small{display:block;color:#9aacc2;margin-bottom:4px}.meta-row code{display:block;color:#e8eef8;overflow-wrap:anywhere;font-size:13px}
                .records-title{font-size:28px;margin:42px 0 8px}.records{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:16px}.record-panel{background:#182438;border:1px solid #2b405b;border-radius:18px;padding:22px;min-width:0}.record-panel h3{margin:0 0 5px}.record-panel p{margin:0 0 18px;font-size:13px}.table-wrap{overflow-x:auto}table{width:100%;border-collapse:collapse;font-size:13px}th{text-align:left;color:#9aacc2;font-weight:600}th,td{padding:9px 10px;border-bottom:1px solid #2b405b}td{color:#e8eef8;font-family:ui-monospace,monospace;max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.ellipsis td{color:#9aacc2;text-align:center}
                .section-title{font-size:28px;margin:42px 0 8px}.flow{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:16px}.flow-panel,.stage{background:#182438;border:1px solid #2b405b;border-radius:18px;padding:22px}.flow-panel h3{margin:0 0 16px}.path{display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin:18px 0;color:#6ce0c3;font-weight:bold}.path span{background:#24374d;padding:8px 11px;border-radius:9px}.status{color:#ffd17d}.stage ul{color:#aebdd0;line-height:1.7;margin-bottom:0}
                .queue-form{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin-top:18px}.queue-form label{color:#9aacc2;font-size:13px}.queue-form input{display:block;width:100%;margin-top:7px;padding:10px;background:#101827;color:#e8eef8;border:1px solid #3d5875;border-radius:8px;font:inherit}.queue-result{margin-top:16px;padding:14px;border-radius:9px;background:#24374d;color:#e8eef8}
                @media(max-width:800px){.grid,.records{grid-template-columns:repeat(2,minmax(0,1fr))}.queue-form{grid-template-columns:repeat(2,minmax(0,1fr))}}@media(max-width:700px){.records,.meta,.flow{grid-template-columns:1fr}}@media(max-width:560px){.grid,.runs,.queue-form{grid-template-columns:1fr}}
                </style></head><body><main>
                <div class="eyebrow">Kafka Backtest / Performance</div><h1>Run comparison</h1>
                <p class="intro">Two replay runs, measured in their own JVM processes. Compare traffic and resource use together; total time alone does not identify a regression. <a href="{{METRICS_FILE}}" style="color:#6ce0c3">Prometheus snapshot</a></p>
                <section class="runs"><div class="run a"><small>Run A · baseline · green</small><strong>{{NAME_A}}</strong></div><div class="run b"><small>Run B · candidate · orange</small><strong>{{NAME_B}}</strong></div></section>
                <section class="meta">{{METADATA}}</section>
                <section class="grid">{{CARDS}}</section>
                <section class="note"><strong>Reading the result</strong><p>{{INTERPRETATION}}</p></section>
                <h2 class="section-title">Kafka path &amp; partition safety</h2>
                <p>These are offline replays. A fixture may be Kafka-captured or locally generated; no broker topic or partition is created by the harness.</p>
                <section class="flow">{{FLOW}}</section>
                <section class="stage"><strong>Staging gates before serving traffic</strong><ul>
                <li>Capture every source partition and compare per-partition record and byte rates, key concentration, lag, and broker load before adding partitions or creating a new topic.</li>
                <li>Run normal, burst, and skewed-key traffic with a bounded queue. Kingman’s G/G/1 estimate needs utilization, mean service time, and arrival/service variation: Wq ≈ [ρ/(1−ρ)] · [(ca²+cs²)/2] · E[S]. This replay does not collect those inputs.</li>
                <li>Measure broker restart, leader election, consumer rebalance, and Streams state restoration on a staging cluster. This offline replay cannot estimate those times.</li>
                <li>Check GC pause distribution, allocation rate, heap and direct memory, RocksDB state, disk I/O, and rollback behaviour. Object or cache-line alignment has not been measured here.</li>
                </ul></section>
                <section class="stage" style="margin-top:16px"><strong>Kingman per-partition queue estimate</strong>
                <p>Scenario values below use candidate replay service measurements and assume arrival CV = 1. Arrival rates are hypothetical 50% and 80% utilization, not observed producer traffic. This G/G/1 approximation describes one serial queue, not broker or cluster capacity.</p>
                <div class="queue-result">{{KINGMAN_SCENARIOS}}</div>
                <p>Adjust the inputs with measured staging values:</p>
                <div class="queue-form">
                <label>Arrival rate λ (records/s)<input id="arrival" type="number" min="0" step="any" value="{{DEFAULT_ARRIVAL}}" placeholder="e.g. 80"></label>
                <label>Mean service time (ms)<input id="service" type="number" min="0" step="any" value="{{DEFAULT_SERVICE}}" placeholder="e.g. 8"></label>
                <label>Arrival CV ca<input id="arrival-cv" type="number" min="0" step="any" value="{{DEFAULT_CA}}" placeholder="e.g. 1"></label>
                <label>Service CV cs<input id="service-cv" type="number" min="0" step="any" value="{{DEFAULT_CS}}" placeholder="e.g. 1"></label>
                </div><div class="queue-result" id="queue-result" aria-live="polite">Enter all four measurements to calculate.</div></section>
                <h2 class="records-title">Output records</h2><p>First five and last five records from each run, in output offset order.</p>
                <section class="records">{{RECORDS}}</section>
                <p class="footer">GC time is JVM-reported collection time, not a per-record pause. Peak heap is sampled at record boundaries. The current sink buffers outputs in memory. Use repeated, warmed runs on representative data for performance decisions.</p>
                </main><script>
                const ids=['arrival','service','arrival-cv','service-cv'];
                const result=document.getElementById('queue-result');
                function updateQueue(){
                  const values=ids.map(id=>document.getElementById(id).value);
                  if(values.some(value=>value==='')){result.textContent='Enter all four measurements to calculate.';return;}
                  const parsed=values.map(Number);
                  const [lambda,service,ca,cs]=parsed;
                  if(!parsed.every(Number.isFinite)||lambda<0||service<=0||ca<0||cs<0){result.textContent='Use non-negative values and a service time above zero.';return;}
                  const rho=lambda*service/1000;
                  if(rho>=1){result.textContent='Utilization '+(rho*100).toFixed(1)+'%: arrival rate meets or exceeds service capacity. No finite steady-state wait estimate.';return;}
                  const waitMs=(rho/(1-rho))*((ca*ca+cs*cs)/2)*service;
                  result.textContent='Utilization '+(rho*100).toFixed(1)+'% · estimated mean queue wait '+waitMs.toFixed(2)+' ms · spare capacity '+((1-rho)*100).toFixed(1)+'%. Validate with observed p99 latency and lag.';
                }
                ids.forEach(id=>document.getElementById(id).addEventListener('input',updateQueue));
                updateQueue();
                </script></body></html>
                """;
        String cards = card("Input records", count(a.records), count(b.records), a.records, b.records)
                + card("Throughput", rate(a.throughput) + " /s", rate(b.throughput) + " /s", a.throughput, b.throughput)
                + card("Elapsed time", seconds(a.elapsedNs), seconds(b.elapsedNs), a.elapsedNs, b.elapsedNs)
                + card("GC collections", count(a.gcCollections), count(b.gcCollections), a.gcCollections, b.gcCollections)
                + card("GC collection time", a.gcMillis + " ms", b.gcMillis + " ms", a.gcMillis, b.gcMillis)
                + card("Peak observed heap", megabytes(a.heapBytes), megabytes(b.heapBytes), a.heapBytes, b.heapBytes)
                + (a.meanServiceNs >= 0 && b.meanServiceNs >= 0
                    ? card("Mean service time", rate(a.meanServiceNs / 1_000_000.0) + " ms", rate(b.meanServiceNs / 1_000_000.0) + " ms", a.meanServiceNs, b.meanServiceNs)
                    : "")
                + (a.serviceCv >= 0 && b.serviceCv >= 0
                    ? card("Service time variation (CV)", precise(a.serviceCv), precise(b.serviceCv), a.serviceCv, b.serviceCv)
                    : "");
        String interpretation = b.records > a.records
                ? "Run B processed " + rate((double) b.records / a.records) + "× as many records. "
                    + "Its throughput changed by " + rate((b.throughput / a.throughput - 1) * 100) + "% relative to Run A. "
                    + (b.heapBytes > a.heapBytes ? "Observed heap rose with the larger workload. " : "")
                    + "GC used " + rate(b.gcMillis * 1_000_000.0 / b.elapsedNs * 100) + "% of Run B wall time."
                : "Traffic was equal or lower in Run B. Inspect throughput and GC together; this report does not assign a cause to differences.";
        html = html.replace("{{NAME_A}}", nameA).replace("{{NAME_B}}", nameB)
                .replace("{{METRICS_FILE}}", escape(output.getFileName() + ".metrics.txt"))
                .replace("{{METADATA}}", metadataPanel("Run A", a) + metadataPanel("Run B", b))
                .replace("{{CARDS}}", cards).replace("{{INTERPRETATION}}", interpretation)
                .replace("{{KINGMAN_SCENARIOS}}", kingmanScenarios(b))
                .replace("{{DEFAULT_ARRIVAL}}", b.meanServiceNs <= 0 ? "" : precise(800_000_000.0 / b.meanServiceNs))
                .replace("{{DEFAULT_SERVICE}}", b.meanServiceNs <= 0 ? "" : precise(b.meanServiceNs / 1_000_000.0))
                .replace("{{DEFAULT_CA}}", b.meanServiceNs <= 0 ? "" : "1")
                .replace("{{DEFAULT_CS}}", b.serviceCv < 0 ? "" : precise(b.serviceCv))
                .replace("{{FLOW}}", flowPanel("Run A", a, recordsA) + flowPanel("Run B", b, recordsB))
                .replace("{{RECORDS}}", recordPanel("Run A", recordsA) + recordPanel("Run B", recordsB));
        Path parent = output.toAbsolutePath().getParent();
        Files.createDirectories(parent);
        Files.writeString(output, html);
        PrometheusExport.write(firstRun, secondRun,
                output.resolveSibling(output.getFileName() + ".metrics.txt"));
    }

    private static Metrics read(Path file) throws IOException {
        String json = Files.readString(file);
        return new Metrics(number(json, "input_records_processed"), number(json, "throughput_records_per_second"),
                number(json, "elapsed_ns"), number(json, "gc_collections"), number(json, "gc_collection_ms"),
                number(json, "peak_observed_heap_bytes"), string(json, "provider_class"),
                string(json, "class_sha256"), string(json, "artifact_sha256"), string(json, "vcs_revision"),
                string(json, "jvm_version"), string(json, "jvm_vendor"), optionalNumber(json, "jvm_max_heap_bytes"),
                string(json, "source_bootstrap"), string(json, "producer_identity"), string(json, "input_fixture"),
                string(json, "input_topic"), optionalNumber(json, "input_partition"),
                optionalNumber(json, "input_offset_start"), optionalNumber(json, "input_offset_end_exclusive"),
                string(json, "output_topic"), optionalNumber(json, "mean_service_ns"),
                optionalNumber(json, "service_cv"));
    }

    private static String kingmanScenarios(Metrics m) {
        if (m.meanServiceNs <= 0 || m.serviceCv < 0) return "Per-record service measurements are unavailable for this run.";
        double serviceMs = m.meanServiceNs / 1_000_000.0;
        double capacity = 1000 / serviceMs;
        double variation = (1 + m.serviceCv * m.serviceCv) / 2;
        double lowWait = variation * serviceMs;
        double highWait = 4 * variation * serviceMs;
        return "Measured mean service " + rate(serviceMs) + " ms/record; measured service CV " + precise(m.serviceCv)
                + "; implied single-worker service rate " + rate(capacity) + " records/s. "
                + "At assumed arrivals " + rate(capacity * .5) + " records/s (50% utilization), mean queue wait ≈ "
                + rate(lowWait) + " ms. At " + rate(capacity * .8) + " records/s (80% utilization), wait ≈ "
                + rate(highWait) + " ms. These are modeled means, not p99 latency.";
    }

    private static String flowPanel(String label, Metrics m, List<KafkaRecord> records) {
        long partitions = records.stream().map(KafkaRecord::partition).distinct().count();
        String range = m.offsetStart < 0 ? "not recorded"
                : count(m.offsetStart) + "–" + count(m.offsetEnd - 1)
                        + (m.sourceBootstrap.equals("not recorded") ? " (fixture offsets)" : " (captured broker offsets)");
        String route = m.sourceBootstrap.equals("not recorded")
                ? "<span>Input fixture</span>→<span>Streams job</span>→<span>Output segments</span>"
                : "<span>Producer</span>→<span>Kafka source</span>→<span>Input fixture</span>→<span>Streams job</span>→<span>Output segments</span>";
        return "<article class=\"flow-panel\"><h3>" + label + " · Record route</h3>"
                + "<div class=\"path\">" + route + "</div>"
                + metadataRow("Producer client", m.producerIdentity)
                + metadataRow("Captured broker", m.sourceBootstrap)
                + metadataRow("Input fixture", m.inputFixture)
                + metadataRow("Input topic / partition", m.inputTopic + " / " + (m.inputPartition < 0 ? "not recorded" : count(m.inputPartition)))
                + metadataRow("Captured offset range", range)
                + metadataRow("Consumer / topology", m.providerClass)
                + metadataRow("Output topic / partitions", m.outputTopic + " / " + partitions + " observed")
                + metadataRow("Output offsets", "synthetic, starting at 0 per partition")
                + metadataRow("Created Kafka topics or partitions", "No — offline replay only")
                + metadataRow("Hot-partition verdict", "Unmeasured — only one source partition captured")
                + metadataRow("Kingman queue delay", "Unmeasured — arrival/service variability absent")
                + metadataRow("Broker or cluster restart", "Unmeasured — no live restart performed")
                + "<p class=\"status\">Recommendation: do not add a topic or partition based on this replay alone. Measure all partitions and test routing in staging.</p></article>";
    }

    private static String metadataPanel(String label, Metrics m) {
        return "<article class=\"meta-panel\"><h3>" + label + " · Job &amp; JVM</h3>"
                + metadataRow("Provider", m.providerClass)
                + metadataRow("Provider bytecode SHA-256", m.classSha256)
                + metadataRow("Job JAR SHA-256", m.artifactSha256)
                + metadataRow("Git revision", m.vcsRevision)
                + metadataRow("JVM", m.jvmVersion + " · " + m.jvmVendor)
                + metadataRow("Max heap", m.maxHeapBytes < 0 ? "not recorded" : megabytes(m.maxHeapBytes))
                + "</article>";
    }

    private static String metadataRow(String title, String value) {
        return "<div class=\"meta-row\"><small>" + title + "</small><code>" + escape(value) + "</code></div>";
    }

    private static String string(String json, String key) {
        Matcher match = Pattern.compile("\\\"" + key + "\\\"\\s*:\\s*\\\"([^\\\"]*)\\\"").matcher(json);
        return match.find() ? match.group(1) : "not recorded";
    }

    private static double optionalNumber(String json, String key) {
        Matcher match = Pattern.compile("\\\"" + key + "\\\"\\s*:\\s*(-?[0-9]+(?:\\.[0-9]+)?(?:[eE][+-]?[0-9]+)?)").matcher(json);
        return match.find() ? Double.parseDouble(match.group(1)) : -1;
    }

    private static List<KafkaRecord> outputRecords(Path run) throws IOException {
        List<KafkaRecord> all = new ArrayList<>();
        try (var files = Files.list(run)) {
            for (Path file : files.filter(path -> path.getFileName().toString().endsWith(".seg"))
                    .sorted(Comparator.comparing(Path::toString)).toList()) {
                all.addAll(SegmentDecoder.decode(ByteBuffer.wrap(Files.readAllBytes(file))).records());
            }
        }
        return all;
    }

    private static String recordPanel(String label, List<KafkaRecord> records) {
        StringBuilder body = new StringBuilder("<article class=\"record-panel\"><h3>")
                .append(label).append("</h3><p>").append(count(records.size()))
                .append(" output records</p><div class=\"table-wrap\"><table><thead><tr><th>Partition</th><th>Offset</th><th>Key</th><th>Value</th></tr></thead><tbody>");
        int head = Math.min(5, records.size());
        for (int i = 0; i < head; i++) appendRecord(body, records.get(i));
        if (records.size() > 10) body.append("<tr class=\"ellipsis\"><td colspan=\"4\">… ")
                .append(count(records.size() - 10)).append(" more records …</td></tr>");
        for (int i = Math.max(head, records.size() - 5); i < records.size(); i++) appendRecord(body, records.get(i));
        return body.append("</tbody></table></div></article>").toString();
    }

    private static void appendRecord(StringBuilder body, KafkaRecord record) {
        body.append("<tr><td>").append(record.partition()).append("</td><td>").append(record.offset())
                .append("</td><td title=\"").append(escape(text(record.key(), record.keyNull())))
                .append("\">").append(escape(text(record.key(), record.keyNull())))
                .append("</td><td title=\"").append(escape(text(record.value(), record.valueNull())))
                .append("\">").append(escape(text(record.value(), record.valueNull()))).append("</td></tr>");
    }

    private static String text(ByteBuffer data, boolean isNull) {
        if (isNull) return "null";
        return StandardCharsets.UTF_8.decode(data.duplicate()).toString();
    }

    private static double number(String json, String key) throws IOException {
        Matcher match = Pattern.compile("\\\"" + key + "\\\"\\s*:\\s*([0-9]+(?:\\.[0-9]+)?(?:[eE][+-]?[0-9]+)?)").matcher(json);
        if (!match.find()) throw new IOException("missing metric: " + key);
        return Double.parseDouble(match.group(1));
    }

    private static String card(String title, String left, String right, double a, double b) {
        double maximum = Math.max(a, b);
        double widthA = maximum == 0 ? 0 : Math.min(100, a / maximum * 100);
        double widthB = maximum == 0 ? 0 : Math.min(100, b / maximum * 100);
        return "<article class=\"card\"><h2>" + title + "</h2><div class=\"values\"><div><small>Run A</small><strong>"
                + left + "</strong></div><div><small>Run B</small><strong>" + right
                + "</strong></div></div><div class=\"track\"><div class=\"fill a\" style=\"width:"
                + rate(widthA) + "%\"></div></div><div class=\"track\"><div class=\"fill b\" style=\"width:"
                + rate(widthB) + "%\"></div></div></article>";
    }

    private static String escape(String value) {
        return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                .replace("\"", "&quot;").replace("'", "&#39;");
    }

    private static String count(double value) { return String.format(Locale.ROOT, "%,.0f", value); }
    private static String rate(double value) { return String.format(Locale.ROOT, "%.1f", value); }
    private static String precise(double value) { return String.format(Locale.ROOT, "%.4f", value); }
    private static String seconds(double ns) { return rate(ns / 1_000_000_000.0) + " s"; }
    private static String megabytes(double bytes) { return rate(bytes / 1_000_000.0) + " MB"; }

    private record Metrics(double records, double throughput, double elapsedNs,
                           double gcCollections, double gcMillis, double heapBytes,
                           String providerClass, String classSha256, String artifactSha256,
                           String vcsRevision, String jvmVersion, String jvmVendor, double maxHeapBytes,
                           String sourceBootstrap, String producerIdentity, String inputFixture,
                           String inputTopic, double inputPartition, double offsetStart, double offsetEnd,
                           String outputTopic, double meanServiceNs, double serviceCv) { }
}
