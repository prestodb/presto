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
package com.facebook.presto.opentelemetry.tracing;

import com.facebook.presto.common.TelemetryConfig;
import io.opentelemetry.api.OpenTelemetry;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;

public class TestScopedSpan
{
    @Test
    public void testScopedSpan()
    {
        assertNotNull(ScopedSpan.scopedSpan(OpenTelemetry.noop().getTracer("no-op"), "no-op"));

        TelemetryConfig.getTelemetryConfig().setTracingEnabled(true);
        assertNotNull(ScopedSpan.scopedSpan(OpenTelemetry.noop().getTracer("no-op"), "no-op"));
    }
}
