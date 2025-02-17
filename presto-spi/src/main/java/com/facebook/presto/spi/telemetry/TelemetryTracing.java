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
 * The SPI TelemetryTracing is implemented by the required serviceability framework.
 *
 * @param <T> the type parameter
 * @param <U> the type parameter
 */
public interface TelemetryTracing<T extends BaseSpan, U extends BaseSpan>
{
    /**
     * Instantiates required Telemetry instances after loading the plugin implementation.
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
     * Returns true if this Span records tracing events.
     *
     * @return the boolean
     */
    boolean isRecording();

    /**
     * Returns headers map from the input span.
     *
     * @param span the span
     * @return the headers map
     */
    Map<String, String> getHeadersMap(T span);

    /**
     * Ends span by updating the status to error and record input exception.
     *
     * @param querySpan the query span
     * @param throwable the throwable
     */
    void endSpanOnError(T querySpan, Throwable throwable);

    /**
     * Add the input event to the input span.
     *
     * @param span      the span
     * @param eventName the event name
     */
    void addEvent(T span, String eventName);

    /**
     * Add the input event to the input span.
     *
     * @param span  the query span
     * @param eventName  the event name
     * @param eventState the event state
     */
    void addEvent(T span, String eventName, String eventState);

    /**
     * Sets the attributes map to the input span.
     *
     * @param span       the span
     * @param attributes the attributes
     */
    void setAttributes(T span, Map<String, String> attributes);

    /**
     * Records exception to the input span with error code and message.
     *
     * @param span        the query span
     * @param message          the message
     * @param runtimeException the runtime exception
     * @param errorCode        the error code
     */
    void recordException(T span, String message, RuntimeException runtimeException, ErrorCode errorCode);

    /**
     * Sets the status of the input span to success.
     *
     * @param span the query span
     */
    void setSuccess(T span);

    //GetSpans

    /**
     * Returns an invalid Span. An invalid Span is used when tracing is disabled.
     *
     * @return the invalid span
     */
    T getInvalidSpan();

    /**
     * Creates and returns the root span.
     *
     * @return the root span
     */
    T getRootSpan();

    /**
     * Creates and returns the span with input name.
     *
     * @param spanName the span name
     * @return the span
     */
    T getSpan(String spanName);

    /**
     * Creates and returns the span with input name and parent context from input.
     *
     * @param traceParent the trace parent
     * @param spanName    the span name
     * @return the span
     */
    T getSpan(String traceParent, String spanName);

    /**
     * Creates and returns the span with input name, attributes and parent span as the input span.
     *
     * @param parentSpan the parent span
     * @param spanName   the span name
     * @param attributes the attributes
     * @return the span
     */
    T getSpan(T parentSpan, String spanName, Map<String, String> attributes);

    /**
     * Returns the span info as string.
     *
     * @param span the span
     * @return the optional
     */
    Optional<String> spanString(T span);

    //scoped spans

    /**
     * Creates and returns the ScopedSpan with input name.
     *
     * @param name     the name
     * @param skipSpan the skip span
     * @return the u
     */
    U scopedSpan(String name, Boolean... skipSpan);

    /**
     * Creates and returns the ScopedSpan with parent span as input span.
     *
     * @param span     the span
     * @param skipSpan the skip span
     * @return the u
     */
    U scopedSpan(T span, Boolean... skipSpan);

    /**
     * Creates and returns the ScopedSpan with input name, attributes and parent span as the input span.
     *
     * @param parentSpan the parent span
     * @param spanName   the span name
     * @param attributes the attributes
     * @param skipSpan   the skip span
     * @return the u
     */
    U scopedSpan(T parentSpan, String spanName, Map<String, String> attributes, Boolean... skipSpan);

    /**
     * Creates and returns the ScopedSpan with input name and the parent span as input span.
     *
     * @param parentSpan the parent span
     * @param spanName   the span name
     * @param skipSpan   the skip span
     * @return the u
     */
    U scopedSpan(T parentSpan, String spanName, Boolean... skipSpan);

    /**
     * Creates and returns the ScopedSpan with input name and attributes.
     *
     * @param spanName   the span name
     * @param attributes the attributes
     * @param skipSpan   the skip span
     * @return the u
     */
    U scopedSpan(String spanName, Map<String, String> attributes, Boolean... skipSpan);
}
