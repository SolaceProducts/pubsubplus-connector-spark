package com.solacecoe.connectors.spark;

import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkStreamingProperties;
import com.solacesystems.jcsmp.JCSMPProperties;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.solace.Service;
import org.testcontainers.solace.SolaceContainer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Test helpers for integration tests.
 */
public final class SparkTestUtils {

    private static final Logger LOG = LoggerFactory.getLogger(SparkTestUtils.class);

    private SparkTestUtils() {
    }

    /**
     * Resets Spark between tests <em>without</em> the expensive full container reboot that
     * {@code @AfterEach} previously did ({@code stop()/start()} on both master and worker,
     * ~every test). Instead it, in-place:
     * <ol>
     *   <li>kills the background {@code spark-submit} driver from the previous test,</li>
     *   <li>wipes the fixed checkpoint dir {@code /opt/spark/checkpoint/*} — this is the
     *       isolation the reboot silently provided; without it the next streaming query would
     *       resume from committed offsets instead of reprocessing, breaking the record-count
     *       assertions, and</li>
     *   <li>truncates {@code /tmp/spark.log}, which the assertions parse.</li>
     * </ol>
     * Containers are (re)started first in case a test deliberately left them stopped, so the
     * previous "always running for the next test" guarantee is preserved.
     *
     * <p>The kill uses {@code pkill} when present and falls back to a {@code /proc} scan so it
     * works regardless of which utilities the Spark image ships.
     */
    public static void resetSparkBetweenTests(GenericContainer<?> sparkMaster, GenericContainer<?> sparkWorker) {
        if (!sparkMaster.isRunning()) {
            sparkMaster.start();
        }
        if (sparkWorker != null && !sparkWorker.isRunning()) {
            sparkWorker.start();
        }
        try {
            sparkMaster.execInContainer(
                    "sh", "-c",
                    "pkill -9 -f SparkSubmit 2>/dev/null; "
                            + "for d in /proc/[0-9]*; do grep -aqs SparkSubmit \"$d/cmdline\" && kill -9 \"${d##*/}\" 2>/dev/null; done; "
                            + "rm -rf /opt/spark/checkpoint/* 2>/dev/null; "
                            + ": > /tmp/spark.log 2>/dev/null; true");
        } catch (Exception e) {
            throw new RuntimeException("Failed to reset Spark container state between tests", e);
        }
    }

    /**
     * Baseline source options for in-JVM (local[*]) validation tests, mirroring the baseline dict in
     * {@code src/test/resources/SolaceSparkSource.py} so converted tests exercise the same config.
     *
     * <p>Two additions are required for local mode:
     * <ul>
     *   <li>{@code sparkRuntimePlatform=OTHER} — the option defaults to {@code DATABRICKS}, which makes
     *       the connector probe {@code SparkSession.active()} for Databricks cluster tags and do a naive
     *       {@code contains("Volumes")} check on the checkpoint path.</li>
     *   <li>{@code partitions=1} — {@code partitions=0} means "one consumer per executor", which resolves
     *       to 0 partitions in local[*] and the query would silently process nothing.</li>
     * </ul>
     *
     * <p>Returns a mutable map, so a test can express the old env-var sentinels naturally:
     * {@code remove(key)} for {@code __DELETE__}, {@code put(key, null)} for {@code NULL},
     * {@code put(key, "")} for the empty-string case.
     */
    public static Map<String, String> baseSourceOptions(SolaceContainer container) {
        Map<String, String> options = new LinkedHashMap<>();
        options.put(SolaceSparkStreamingProperties.HOST, container.getOrigin(Service.SMF));
        options.put(SolaceSparkStreamingProperties.VPN, container.getVpn());
        options.put(SolaceSparkStreamingProperties.USERNAME, container.getUsername());
        options.put(SolaceSparkStreamingProperties.PASSWORD, container.getPassword());
        options.put(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0");
        options.put(SolaceSparkStreamingProperties.SOLACE_CONNECT_RETRIES, "2");
        options.put(SolaceSparkStreamingProperties.SOLACE_RECONNECT_RETRIES, "2");
        options.put(SolaceSparkStreamingProperties.BATCH_SIZE, "50");
        options.put(SolaceSparkStreamingProperties.QUEUE_RECEIVE_WAIT_TIMEOUT, "1000");
        options.put(SolaceSparkStreamingProperties.PARTITIONS, "1");
        options.put(SolaceSparkStreamingProperties.SPARK_RUNTIME_PLATFORM, "OTHER");
        return options;
    }

    /**
     * Starts a {@code solace} readStream with the given options against a local in-JVM SparkSession and
     * returns the resulting failure as a flattened message string, for substring assertions.
     *
     * <p>This replaces the old "spark-submit into a container, then poll /tmp/spark.log for a literal
     * string" mechanism for validation tests. The connector <em>throws</em> its validation messages (e.g.
     * {@code SolaceInvalidPropertyException("SolaceSparkConnector - Please provide Solace Host name in
     * configuration options")}), so asserting on the exception chain is a stricter check than a log grep.
     *
     * <p>A validation error can surface at three different points depending on which check trips, so all
     * three are captured: {@code load()} (schema inference / early property validation), {@code start()},
     * and {@code awaitTermination()} (wrapped in a {@code StreamingQueryException}).
     *
     * <p>Options are applied one at a time rather than via {@code options(Map)} so that a {@code null}
     * value is passed through to the connector as a real null — matching what PySpark did for the old
     * {@code NULL} sentinel.
     *
     * @return the concatenated messages of the whole exception chain, or an empty string if the query
     *         unexpectedly ran to the timeout without failing (which will fail the caller's assertion).
     */
    public static String captureStreamFailure(SparkSession spark,
                                              Map<String, String> options,
                                              Path checkpointDir,
                                              long timeoutMillis) {
        StreamingQuery query = null;
        try {
            Dataset<Row> dataset = buildReader(spark, options).load();

            DataStreamWriter<Row> writer = dataset.writeStream()
                    .format("noop")
                    .outputMode("append")
                    .option("checkpointLocation", checkpointDir.toUri().toString());

            query = writer.start();
            if (!query.awaitTermination(timeoutMillis)) {
                LOG.warn("Streaming query did not fail within {} ms — expected a validation failure.", timeoutMillis);
            }
            // Reached only if the query terminated without throwing, i.e. no validation error occurred.
            return "";
        } catch (Throwable t) {
            return flattenMessages(t);
        } finally {
            stopQuietly(query);
        }
    }

    /**
     * Baseline OAuth options, mirroring the baseline dict in
     * {@code src/test/resources/SolaceSparkSourceOAuth.py}.
     *
     * <p>No container is needed by callers of this: every OAuth option check lives in
     * {@code SolaceUtils.validateCommonProperties}, which {@code SolaceMicroBatch} calls at line 74 —
     * well before {@code new SolaceBroker(...)} at line 156. So the broker host below is never dialed
     * and neither Solace nor Keycloak has to be running. (Tests whose errors come from
     * {@code OAuthClient}, e.g. the invalid-TLS-version and invalid-realm cases, do connect and
     * therefore stay on the container path in {@code SolaceSparkStreamingOAuthIT}.)
     */
    public static Map<String, String> baseOAuthOptions() {
        Map<String, String> options = baseAuthSchemeOptions(JCSMPProperties.AUTHENTICATION_SCHEME_OAUTH2);
        options.put(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.SSL_VALIDATE_CERTIFICATE, "false");
        options.put(SolaceSparkStreamingProperties.OAUTH_CLIENT_CLIENT_ID, "solace");
        options.put(SolaceSparkStreamingProperties.OAUTH_CLIENT_CREDENTIALS_CLIENTSECRET, "solace-secret");
        options.put(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_SSL_VALIDATE_CERTIFICATE, "false");
        options.put(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_URL,
                "https://keycloak:8443/realms/solace/protocol/openid-connect/token");
        return options;
    }

    /**
     * Baseline options with basic authentication explicitly selected — equivalent to the old
     * {@code auth_scheme_basic=true} flag in {@code src/test/resources/SolaceSparkSourceTLS.py},
     * which switched the scheme to BASIC and removed every SSL property.
     *
     * <p>The explicit scheme matters: {@code SolaceUtils.validateCommonProperties} only checks
     * username/password when an {@code AUTHENTICATION_SCHEME} key is present and set to BASIC.
     * As with {@link #baseOAuthOptions()}, validation precedes connection so no container is needed.
     */
    public static Map<String, String> baseBasicAuthOptions() {
        return baseAuthSchemeOptions(JCSMPProperties.AUTHENTICATION_SCHEME_BASIC);
    }

    private static Map<String, String> baseAuthSchemeOptions(String authenticationScheme) {
        Map<String, String> options = new LinkedHashMap<>();
        // Never connected to — validation throws first. Keeps these tests container-free.
        options.put(SolaceSparkStreamingProperties.HOST, "tcps://localhost:55443");
        options.put(SolaceSparkStreamingProperties.VPN, "default");
        options.put(SolaceSparkStreamingProperties.USERNAME, "user");
        options.put(SolaceSparkStreamingProperties.PASSWORD, "pass");
        options.put(SolaceSparkStreamingProperties.QUEUE, "integration-test-queue");
        options.put(SolaceSparkStreamingProperties.SOLACE_CONNECT_RETRIES, "2");
        options.put(SolaceSparkStreamingProperties.SOLACE_RECONNECT_RETRIES, "2");
        options.put(SolaceSparkStreamingProperties.BATCH_SIZE, "50");
        options.put(SolaceSparkStreamingProperties.QUEUE_RECEIVE_WAIT_TIMEOUT, "1000");
        options.put(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.AUTHENTICATION_SCHEME,
                authenticationScheme);
        options.put(SolaceSparkStreamingProperties.PARTITIONS, "1");
        options.put(SolaceSparkStreamingProperties.SPARK_RUNTIME_PLATFORM, "OTHER");
        return options;
    }

    /**
     * Baseline <em>sink</em> (write-side) options, mirroring the {@code common_options} + {@code
     * write_options} dicts in {@code src/test/resources/SolaceSparkSink.py}.
     */
    public static Map<String, String> baseSinkOptions(SolaceContainer container) {
        Map<String, String> options = new LinkedHashMap<>();
        options.put(SolaceSparkStreamingProperties.HOST, container.getOrigin(Service.SMF));
        options.put(SolaceSparkStreamingProperties.VPN, container.getVpn());
        options.put(SolaceSparkStreamingProperties.USERNAME, container.getUsername());
        options.put(SolaceSparkStreamingProperties.PASSWORD, container.getPassword());
        options.put(SolaceSparkStreamingProperties.TOPIC, "test/topic");
        options.put(SolaceSparkStreamingProperties.SOLACE_CONNECT_RETRIES, "2");
        options.put(SolaceSparkStreamingProperties.SOLACE_RECONNECT_RETRIES, "2");
        options.put(SolaceSparkStreamingProperties.BATCH_SIZE, "50");
        options.put(SolaceSparkStreamingProperties.SPARK_RUNTIME_PLATFORM, "OTHER");
        return options;
    }

    /**
     * Sink validation via the <strong>streaming</strong> write path
     * ({@code writeStream().format("solace")} → {@code SolaceStreamingWrite}).
     *
     * <p>Mirrors the old {@code only_write=true} + {@code SolaceSparkSink.py} tests: the read side gets
     * valid options while the write side gets the broken config under test. {@code SolaceStreamingWrite}
     * validates in its constructor at query start, so no data has to flow.
     */
    public static String captureStreamingSinkFailure(SparkSession spark,
                                                     Map<String, String> readOptions,
                                                     Map<String, String> writeOptions,
                                                     Path checkpointDir,
                                                     long timeoutMillis) {
        StreamingQuery query = null;
        try {
            Dataset<Row> dataset = buildReader(spark, readOptions).load().drop("TimeStamp");

            DataStreamWriter<Row> writer = dataset.writeStream()
                    .format("solace")
                    .outputMode("append")
                    .option("checkpointLocation", checkpointDir.toUri().toString());
            for (Map.Entry<String, String> option : writeOptions.entrySet()) {
                writer = writer.option(option.getKey(), option.getValue());
            }

            query = writer.start();
            if (!query.awaitTermination(timeoutMillis)) {
                LOG.warn("Streaming sink query did not fail within {} ms — expected a validation failure.", timeoutMillis);
            }
            return "";
        } catch (Throwable t) {
            return flattenMessages(t);
        } finally {
            stopQuietly(query);
        }
    }

    /**
     * Sink validation via the <strong>batch</strong> write path — a {@code foreachBatch} that calls
     * {@code write().format("solace")} → {@code SolaceBatchWrite}, mirroring
     * {@code SolaceSparkSink_ForEachBatch.py}.
     *
     * <p>Note {@code SolaceBatchWrite} validates username/password itself, whereas the streaming path
     * defers to JCSMP — which is why the two families assert different messages for the same broken
     * option. Unlike the streaming path this needs a non-empty batch, so the caller must have published
     * messages to the source queue.
     *
     * <p>{@code coalesce(1)} avoids the empty-partition case: {@code SolaceDataWriter} never completes its
     * ack future when a partition receives zero rows, which would surface as a
     * {@code SolacePublishAckTimeoutException} instead of the expected validation error.
     */
    public static String captureBatchSinkFailure(SparkSession spark,
                                                 Map<String, String> readOptions,
                                                 Map<String, String> writeOptions,
                                                 Path checkpointDir,
                                                 long timeoutMillis) {
        StreamingQuery query = null;
        try {
            Dataset<Row> dataset = buildReader(spark, readOptions).load().drop("TimeStamp");

            query = dataset.writeStream()
                    .option("checkpointLocation", checkpointDir.toUri().toString())
                    .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (batchDataset, batchId) -> {
                        org.apache.spark.sql.DataFrameWriter<Row> batchWriter =
                                batchDataset.coalesce(1).write().format("solace").mode("append");
                        for (Map.Entry<String, String> option : writeOptions.entrySet()) {
                            batchWriter = batchWriter.option(option.getKey(), option.getValue());
                        }
                        batchWriter.save();
                    })
                    .start();

            if (!query.awaitTermination(timeoutMillis)) {
                LOG.warn("Batch sink query did not fail within {} ms — expected a validation failure.", timeoutMillis);
            }
            return "";
        } catch (Throwable t) {
            return flattenMessages(t);
        } finally {
            stopQuietly(query);
        }
    }

    private static org.apache.spark.sql.streaming.DataStreamReader buildReader(SparkSession spark,
                                                                              Map<String, String> options) {
        org.apache.spark.sql.streaming.DataStreamReader reader = spark.readStream().format("solace");
        for (Map.Entry<String, String> option : options.entrySet()) {
            reader = reader.option(option.getKey(), option.getValue());
        }
        return reader;
    }

    private static void stopQuietly(StreamingQuery query) {
        if (query != null) {
            try {
                query.stop();
            } catch (Exception e) {
                LOG.debug("Ignoring error while stopping query: {}", e.toString());
            }
        }
    }

    /**
     * Collects the messages of a throwable and its full cause/suppressed chain into one string, so a test
     * can assert on a message raised anywhere in the chain (Spark wraps connector and JCSMP exceptions
     * several layers deep). Guards against cyclic cause chains.
     */
    public static String flattenMessages(Throwable throwable) {
        StringBuilder sb = new StringBuilder();
        Map<Throwable, Boolean> seen = new IdentityHashMap<>();
        collectMessages(throwable, sb, seen);
        return sb.toString();
    }

    private static void collectMessages(Throwable throwable, StringBuilder sb, Map<Throwable, Boolean> seen) {
        if (throwable == null || seen.put(throwable, Boolean.TRUE) != null) {
            return;
        }
        sb.append(throwable).append(System.lineSeparator());
        for (Throwable suppressed : throwable.getSuppressed()) {
            collectMessages(suppressed, sb, seen);
        }
        collectMessages(throwable.getCause(), sb, seen);
    }

    /**
     * Best-effort delete of a throwaway temp file. On Windows, Docker Desktop / the JVM can keep
     * a handle on a (bind-mounted) file for a short window after a container stops, so a plain
     * delete in {@code @AfterAll} throws "The process cannot access the file because it is being
     * used by another process" and fails the build even though every test passed. This retries a
     * few times, then logs a warning and moves on — never throwing, since the OS reclaims temp
     * files anyway.
     */
    public static void deleteQuietlyWithRetry(Path path) {
        if (path == null) {
            return;
        }
        for (int attempt = 1; attempt <= 5; attempt++) {
            try {
                Files.deleteIfExists(path);
                return;
            } catch (IOException e) {
                if (attempt == 5) {
                    LOG.warn("Could not delete temp file {} after {} attempts ({}); leaving it for OS temp cleanup.",
                            path, attempt, e.toString());
                    return;
                }
                try {
                    Thread.sleep(500L);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }
}
