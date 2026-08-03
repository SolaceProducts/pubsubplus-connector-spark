package com.solacecoe.connectors.spark.base;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.TestWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Logs every JUnit 5 lifecycle transition so that, when an integration test appears to
 * hang, the <em>last line printed</em> pinpoints the exact phase and method it is stuck in
 * (e.g. class {@code @BeforeAll} container startup vs. a specific {@code @Test} body).
 *
 * <p>Registered automatically for every test — no per-class annotation needed — via:
 * <ul>
 *   <li>{@code src/test/resources/META-INF/services/org.junit.jupiter.api.extension.Extension}
 *       (declares this class as a service), and</li>
 *   <li>{@code junit.jupiter.extensions.autodetection.enabled=true} in
 *       {@code src/test/resources/junit-platform.properties}.</li>
 * </ul>
 *
 * <p>Verbosity is controlled by {@code src/test/resources/log4j2.xml}; these lines log at
 * INFO under the logger name {@code IT.lifecycle}.
 */
public class TestLifecycleLogger implements
        BeforeAllCallback, AfterAllCallback,
        BeforeEachCallback, AfterEachCallback,
        BeforeTestExecutionCallback, AfterTestExecutionCallback,
        TestWatcher {

    private static final Logger LOG = LoggerFactory.getLogger("IT.lifecycle");
    private static final Namespace NS = Namespace.create(TestLifecycleLogger.class);
    private static final String START_NANOS = "startNanos";

    private static String cls(ExtensionContext ctx) {
        return ctx.getTestClass().map(Class::getSimpleName).orElseGet(ctx::getDisplayName);
    }

    @Override
    public void beforeAll(ExtensionContext ctx) {
        LOG.info("===> [{}] class setup (@BeforeAll) START  (thread={})",
                cls(ctx), Thread.currentThread().getName());
    }

    @Override
    public void afterAll(ExtensionContext ctx) {
        LOG.info("<=== [{}] class DONE", cls(ctx));
    }

    @Override
    public void beforeEach(ExtensionContext ctx) {
        LOG.info("   -> [{}] @BeforeEach START  '{}'", cls(ctx), ctx.getDisplayName());
    }

    @Override
    public void afterEach(ExtensionContext ctx) {
        LOG.info("   <- [{}] @AfterEach DONE   '{}'", cls(ctx), ctx.getDisplayName());
    }

    @Override
    public void beforeTestExecution(ExtensionContext ctx) {
        ctx.getStore(NS).put(START_NANOS, System.nanoTime());
        LOG.info("   >> START  {}.{}", cls(ctx), ctx.getDisplayName());
    }

    @Override
    public void afterTestExecution(ExtensionContext ctx) {
        Long start = ctx.getStore(NS).remove(START_NANOS, Long.class);
        long ms = (start == null) ? -1L : (System.nanoTime() - start) / 1_000_000L;
        LOG.info("   << END    {}.{}  ({} ms)", cls(ctx), ctx.getDisplayName(), ms);
    }

    @Override
    public void testSuccessful(ExtensionContext ctx) {
        LOG.info("   ++ PASS   {}.{}", cls(ctx), ctx.getDisplayName());
    }

    @Override
    public void testFailed(ExtensionContext ctx, Throwable cause) {
        LOG.error("   !! FAIL   {}.{}  -> {}", cls(ctx), ctx.getDisplayName(), String.valueOf(cause));
    }

    @Override
    public void testAborted(ExtensionContext ctx, Throwable cause) {
        LOG.warn("   ~~ ABORT  {}.{}  -> {}", cls(ctx), ctx.getDisplayName(), String.valueOf(cause));
    }

    @Override
    public void testDisabled(ExtensionContext ctx, Optional<String> reason) {
        LOG.info("   .. SKIP   {}.{}  ({})", cls(ctx), ctx.getDisplayName(), reason.orElse("no reason given"));
    }
}
