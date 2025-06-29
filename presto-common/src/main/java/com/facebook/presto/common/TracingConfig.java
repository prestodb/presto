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
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

/**
 * POJO to use with TelemetryResource for the dynamically enable/disable the trace endpoint.
 */
@Immutable
public class TracingConfig
{
    private boolean tracingEnabled;

    /**
     * Instantiates a new Tracing config.
     *
     * @param tracingEnabled the tracing enabled
     */
    @JsonCreator
    public TracingConfig(
            @JsonProperty("tracingEnabled") boolean tracingEnabled)
    {
        this.tracingEnabled = tracingEnabled;
    }

    @JsonProperty
    public boolean isTracingEnabled()
    {
        return tracingEnabled;
    }
}
