/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.annotation.Nullable;

import java.util.Objects;

/**
 * An immutable completed span of a runtime metric. Span identifiers describe parent-child
 * relationships across metric names. Start and end timestamps are expressed in nanoseconds
 * since the Unix epoch, but their absolute precision is limited by the millisecond-resolution
 * wall-clock anchor captured when the query trace starts. Relative offsets and durations are
 * measured with a monotonic clock.
 */
public final class RuntimeMetricEvent
{
    static final long UNKNOWN_THREAD_ID = -1;

    private final long spanId;
    private final long parentSpanId;
    private final long startTimeNanos;
    private final long endTimeNanos;
    private final long startThreadId;
    private final long endThreadId;
    @Nullable
    private final String startThreadName;
    @Nullable
    private final String endThreadName;
    private final boolean failed;

    @JsonCreator
    public RuntimeMetricEvent(
            @JsonProperty("spanId") long spanId,
            @JsonProperty("parentSpanId") long parentSpanId,
            @JsonProperty("startTimeNanos") long startTimeNanos,
            @JsonProperty("endTimeNanos") long endTimeNanos,
            @JsonProperty("startThreadId") long startThreadId,
            @JsonProperty("endThreadId") long endThreadId,
            @JsonProperty("startThreadName") @Nullable String startThreadName,
            @JsonProperty("endThreadName") @Nullable String endThreadName,
            @JsonProperty("failed") boolean failed)
    {
        this.spanId = spanId;
        this.parentSpanId = parentSpanId;
        this.startTimeNanos = startTimeNanos;
        this.endTimeNanos = endTimeNanos;
        this.startThreadId = startThreadId;
        this.endThreadId = endThreadId;
        this.startThreadName = startThreadName;
        this.endThreadName = endThreadName;
        this.failed = failed;
    }

    @JsonProperty
    public long getSpanId()
    {
        return spanId;
    }

    @JsonProperty
    public long getParentSpanId()
    {
        return parentSpanId;
    }

    @JsonProperty
    public long getStartTimeNanos()
    {
        return startTimeNanos;
    }

    @JsonProperty
    public long getEndTimeNanos()
    {
        return endTimeNanos;
    }

    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    public long getDurationNanos()
    {
        return endTimeNanos - startTimeNanos;
    }

    /**
     * Returns {@code -1} when the recorder did not supply the thread that started the interval.
     */
    @JsonProperty
    public long getStartThreadId()
    {
        return startThreadId;
    }

    @JsonProperty
    public long getEndThreadId()
    {
        return endThreadId;
    }

    /**
     * Returns {@code null} when the recorder did not supply the thread that started the interval.
     */
    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    public String getStartThreadName()
    {
        return startThreadName;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    public String getEndThreadName()
    {
        return endThreadName;
    }

    @JsonProperty
    public boolean isFailed()
    {
        return failed;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof RuntimeMetricEvent)) {
            return false;
        }
        RuntimeMetricEvent other = (RuntimeMetricEvent) obj;
        return spanId == other.spanId &&
                parentSpanId == other.parentSpanId &&
                startTimeNanos == other.startTimeNanos &&
                endTimeNanos == other.endTimeNanos &&
                startThreadId == other.startThreadId &&
                endThreadId == other.endThreadId &&
                Objects.equals(startThreadName, other.startThreadName) &&
                Objects.equals(endThreadName, other.endThreadName) &&
                failed == other.failed;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(spanId, parentSpanId, startTimeNanos, endTimeNanos, startThreadId, endThreadId, startThreadName, endThreadName, failed);
    }
}
