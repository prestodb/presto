package com.facebook.presto.event.scribe.payload;

import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import io.airlift.json.JsonCodec;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Builder for Scribe data payloads by serializing events into Scribe with the following message format:
 * <p/>
 * {@code <app_event_type>\001<json_encoded_property_map>}
 * <p/>
 * NOTE: newlines are not permitted in the payload
 */
public class PayloadBuilder
{
    private static final char FIELD_SEPARATOR = '\001';
    private static final CharMatcher NEWLINE = CharMatcher.is('\n');

    private final JsonCodec<Map<String, Object>> jsonCodec;

    private String appEventType; // Type of event
    private final Map<String, Object> appSection = new HashMap<>(); // Event data

    public PayloadBuilder(JsonCodec<Map<String, Object>> jsonCodec)
    {
        this.jsonCodec = checkNotNull(jsonCodec, "jsonCodec is null");
    }

    public PayloadBuilder setAppEventType(String appEventType)
    {
        checkNotNull(appEventType, "appEventType is null");
        checkArgument(NEWLINE.matchesNoneOf(appEventType), "appEventType can not contain newlines");
        this.appEventType = appEventType;
        return this;
    }

    public PayloadBuilder addAppData(String key, Object value)
    {
        checkNotNull(key, "key is null");
        checkNotNull(value, "value is null");
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
