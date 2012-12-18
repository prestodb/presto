package com.facebook.presto.event.scribe.nectar;

import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import io.airlift.json.JsonCodec;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Builder for Nectar data payloads
 * NOTE: newlines are not permitted
 */
public class NectarPayloadBuilder
{
    private static final char FIELD_SEPARATOR = '\001';
    private static final CharMatcher NEWLINE = CharMatcher.is('\n');

    private final JsonCodec<Map<String, Object>> jsonCodec;

    private String appEventType; // Type of event
    private final Map<String, Object> appSection = new HashMap<>(); // Event data

    public NectarPayloadBuilder(JsonCodec<Map<String, Object>> jsonCodec)
    {
        this.jsonCodec = checkNotNull(jsonCodec, "jsonCodec is null");
    }

    public NectarPayloadBuilder setAppEventType(String appEventType)
    {
        checkNotNull(appEventType, "appEventType is null");
        checkArgument(NEWLINE.matchesNoneOf(appEventType), "appEventType can not contain newlines");
        this.appEventType = appEventType;
        return this;
    }

    public NectarPayloadBuilder addAppData(String key, Object value)
    {
        checkNotNull(key, "key is null");
        checkNotNull(value, "value is null");
        // The key should NOT contain any capital characters. Otherwise it won't be parsed into top level column during the ETL process.
        checkArgument(CharMatcher.JAVA_UPPER_CASE.matchesNoneOf(key), "appData key must be entirely lower case");
        checkArgument(NEWLINE.matchesNoneOf(key), "key can not contain newlines");
        checkArgument(appSection.put(key, value) == null, "overlapping key: %s", key);
        return this;
    }

    public String build()
    {
        checkState(appEventType != null, "appEventType not initialized");
        checkState(!appSection.isEmpty(), "appSection not initialized");

        String encodedAppSection = jsonCodec.toJson(appSection);

        checkArgument(NEWLINE.matchesNoneOf(encodedAppSection), "encodedAppSection can not contain newlines");

        return Joiner.on(FIELD_SEPARATOR).join(appEventType, encodedAppSection);
    }
}
