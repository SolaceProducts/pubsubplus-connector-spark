package com.solacecoe.connectors.spark.streaming.properties;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SolaceSparkHeadersMeta<T> implements HeaderMeta<T> {
    public static final Map<String, SolaceSparkHeadersMeta<?>> META = Stream.of(new Object[][] {
            {SolaceSparkHeaders.PARTITION_KEY, new SolaceSparkHeadersMeta<>(String.class, false, true, Scope.WIRE)},
            {SolaceSparkHeaders.MESSAGE_VERSION, new SolaceSparkHeadersMeta<>(Integer.class, true, false, Scope.WIRE)},
            {SolaceSparkHeaders.SERIALIZED_PAYLOAD, new SolaceSparkHeadersMeta<>(Boolean.class, false, false, Scope.WIRE)},
            {SolaceSparkHeaders.SERIALIZED_HEADERS, new SolaceSparkHeadersMeta<>(String.class, false, false, Scope.WIRE)},
            {SolaceSparkHeaders.SERIALIZED_HEADERS_ENCODING, new SolaceSparkHeadersMeta<>(String.class, false, false, Scope.WIRE)},
//            {SolaceSparkHeaders.CONFIRM_CORRELATION, new SolaceSparkHeadersMeta<>(CorrelationData.class, false, true, Scope.LOCAL)},
            {SolaceSparkHeaders.NULL_PAYLOAD, new SolaceSparkHeadersMeta<>(Boolean.class, true, false, Scope.LOCAL)},
            {SolaceSparkHeaders.BATCHED_HEADERS, new SolaceSparkHeadersMeta<>(List.class, true, true, Scope.LOCAL)},
            {SolaceSparkHeaders.TARGET_DESTINATION_TYPE, new SolaceSparkHeadersMeta<>(String.class, false, true, Scope.LOCAL)}
    }).collect(Collectors.toMap(d -> (String) d[0], d -> (SolaceSparkHeadersMeta<?>) d[1]));

    private final Class<T> type;
    private final boolean readable;
    private final boolean writable;
    private final Scope scope;

    public SolaceSparkHeadersMeta(Class<T> type, boolean readable, boolean writable, Scope scope) {
        this.type = type;
        this.readable = readable;
        this.writable = writable;
        this.scope = scope;
    }

    @Override
    public Class<T> getType() {
        return type;
    }

    /**
     * The readable property is only used by tests and doesn't necessarily reflect whether a header can be read by an application or not
     */
    @Override
    public boolean isReadable() {
        return readable;
    }

    /**
     * The writable property is only used by tests and doesn't necessarily reflect whether a header can be written by an application or not
     */
    @Override
    public boolean isWritable() {
        return writable;
    }

    @Override
    public Scope getScope() {
        return scope;
    }
}
