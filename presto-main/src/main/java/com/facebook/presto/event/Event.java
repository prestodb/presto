package com.facebook.presto.event;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;

import javax.annotation.concurrent.Immutable;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Normalized version of event information from Event annotated classes
 */
@Immutable
public class Event
{
    private final String type;
    private final String uuid;
    private final String host;
    private final DateTime timestamp;
    private final Map<String, ?> data;

    @JsonCreator
    public Event(@JsonProperty("type") String type,
            @JsonProperty("uuid") String uuid,
            @JsonProperty("host") String host,
            @JsonProperty("timestamp") DateTime timestamp,
            @JsonProperty("data") Map<String, ?> data)
    {
        this.type = checkNotNull(type, "type is null");
        this.uuid = checkNotNull(uuid, "uuid is null");
        this.host = checkNotNull(host, "host is null");
        this.timestamp = checkNotNull(timestamp, "timestamp is null");
        this.data = ImmutableMap.copyOf(checkNotNull(data, "data is null"));
    }

    @JsonProperty
    @NotNull(message = "is missing")
    @Pattern(regexp = "[A-Za-z][A-Za-z0-9]*", message = "must be alphanumeric")
    public String getType()
    {
        return type;
    }

    @JsonProperty
    @NotNull(message = "is missing")
    public String getUuid()
    {
        return uuid;
    }

    @JsonProperty
    @NotNull(message = "is missing")
    public String getHost()
    {
        return host;
    }

    @JsonProperty
    @NotNull(message = "is missing")
    public DateTime getTimestamp()
    {
        return timestamp;
    }

    @JsonProperty
    @NotNull(message = "is missing")
    public Map<String, ?> getData()
    {
        return data;
    }
}
