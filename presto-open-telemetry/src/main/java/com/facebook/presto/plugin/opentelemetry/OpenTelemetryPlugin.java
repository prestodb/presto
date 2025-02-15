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
package com.facebook.presto.plugin.opentelemetry;

import com.facebook.presto.opentelemetry.tracing.OpenTelemetryFactoryImpl;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.telemetry.TelemetryFactory;
import com.facebook.presto.testing.TestingOpenTelemetryFactoryImpl;
import com.google.common.collect.ImmutableList;

/**
 * The type Open telemetry plugin.
 */
public class OpenTelemetryPlugin
        implements Plugin
{
    @Override
    public Iterable<TelemetryFactory> getTelemetryFactories()
    {
        return ImmutableList.of(new OpenTelemetryFactoryImpl(), new TestingOpenTelemetryFactoryImpl());
    }
}
