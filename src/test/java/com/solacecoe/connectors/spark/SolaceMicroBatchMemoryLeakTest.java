package com.solacecoe.connectors.spark;

import com.solacecoe.connectors.spark.streaming.SolaceMicroBatch;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * DATAGO-149324 Finding 1: SolaceMicroBatch.lastKnownMessageIds accumulated one checkpoint message
 * id per micro-batch trigger, onto a single String field, for the entire lifetime of the query, via
 * String.join - and was never read anywhere in the codebase. Once the accumulated string crossed the
 * G1 humongous-allocation threshold, every subsequent trigger allocated a full copy of it, causing
 * unbounded driver memory growth. The field and its 3 append call-sites were deleted entirely since
 * deletion is behavior-neutral (confirmed: no code path ever reads lastKnownMessageIds).
 *
 * This is a plain unit test (no broker/container required) that guards against the field being
 * reintroduced by a future change.
 */
class SolaceMicroBatchMemoryLeakTest {
    @Test
    void lastKnownMessageIdsFieldMustNotBeReintroduced() {
        boolean fieldExists = Arrays.stream(SolaceMicroBatch.class.getDeclaredFields())
                .map(Field::getName)
                .anyMatch("lastKnownMessageIds"::equals);

        assertFalse(fieldExists,
                "SolaceMicroBatch.lastKnownMessageIds must not exist - see DATAGO-149324 Finding 1. " +
                        "This field is never read anywhere and previously caused unbounded driver memory " +
                        "growth by accumulating one checkpoint message id per micro-batch trigger, forever, " +
                        "via String.join. If you need to track the last known message id for some new " +
                        "purpose, bound its size and confirm it is actually read before reintroducing it.");
    }
}
