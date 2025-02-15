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
package com.facebook.presto.spi.telemetry;

import com.facebook.presto.common.ErrorCode;

import java.util.Map;
import java.util.Optional;

/**
 * The interface Telemetry tracing.
 *
 * @param <T> the type parameter
 * @param <U> the type parameter
 */
public interface TelemetryTracing<T extends BaseSpan, U extends BaseSpan>
{
    /**
     * Load configured open telemetry.
     */
    void loadConfiguredOpenTelemetry();

    /**
     * Gets current context wrap.
     *
     * @param runnable the runnable
     * @return the current context wrap
     */
    Runnable getCurrentContextWrap(Runnable runnable);

    /**
     * Is recording boolean.
     *
     * @return the boolean
     */
    boolean isRecording();

    /**
     * Gets headers map.
     *
     * @param span the span
     * @return the headers map
     */
    Map<String, String> getHeadersMap(T span);

    /**
     * End span on error.
     *
     * @param querySpan the query span
     * @param throwable the throwable
     */
    void endSpanOnError(T querySpan, Throwable throwable);

    /**
     * Add event.
     *
     * @param span      the span
     * @param eventName the event name
     */
    void addEvent(T span, String eventName);

    /**
     * Add event.
     *
     * @param querySpan  the query span
     * @param eventName  the event name
     * @param eventState the event state
     */
    void addEvent(T querySpan, String eventName, String eventState);

    /**
     * Sets attributes.
     *
     * @param span       the span
     * @param attributes the attributes
     */
    void setAttributes(T span, Map<String, String> attributes);

    /**
     * Record exception.
     *
     * @param querySpan        the query span
     * @param message          the message
     * @param runtimeException the runtime exception
     * @param errorCode        the error code
     */
    void recordException(T querySpan, String message, RuntimeException runtimeException, ErrorCode errorCode);

    /**
     * Sets success.
     *
     * @param querySpan the query span
     */
    void setSuccess(T querySpan);

    //GetSpans

    /**
     * Gets invalid span.
     *
     * @return the invalid span
     */
    T getInvalidSpan();

    /**
     * Gets root span.
     *
     * @return the root span
     */
    T getRootSpan();

    /**
     * Gets span.
     *
     * @param spanName the span name
     * @return the span
     */
    T getSpan(String spanName);

    /**
     * Gets span.
     *
     * @param traceParent the trace parent
     * @param spanName    the span name
     * @return the span
     */
    T getSpan(String traceParent, String spanName);

    /**
     * Gets span.
     *
     * @param parentSpan the parent span
     * @param spanName   the span name
     * @param attributes the attributes
     * @return the span
     */
    T getSpan(T parentSpan, String spanName, Map<String, String> attributes);

    /**
     * Span string optional.
     *
     * @param span the span
     * @return the optional
     */
    Optional<String> spanString(T span);

    //scoped spans

    /**
     * Scoped span u.
     *
     * @param name     the name
     * @param skipSpan the skip span
     * @return the u
     */
    U scopedSpan(String name, Boolean... skipSpan);

    /**
     * Scoped span u.
     *
     * @param span     the span
     * @param skipSpan the skip span
     * @return the u
     */
    U scopedSpan(T span, Boolean... skipSpan);

    /**
     * Scoped span u.
     *
     * @param parentSpan the parent span
     * @param spanName   the span name
     * @param attributes the attributes
     * @param skipSpan   the skip span
     * @return the u
     */
    U scopedSpan(T parentSpan, String spanName, Map<String, String> attributes, Boolean... skipSpan);

    /**
     * Scoped span u.
     *
     * @param parentSpan the parent span
     * @param spanName   the span name
     * @param skipSpan   the skip span
     * @return the u
     */
    U scopedSpan(T parentSpan, String spanName, Boolean... skipSpan);

    /**
     * Scoped span u.
     *
     * @param spanName   the span name
     * @param attributes the attributes
     * @param skipSpan   the skip span
     * @return the u
     */
    U scopedSpan(String spanName, Map<String, String> attributes, Boolean... skipSpan);
}
