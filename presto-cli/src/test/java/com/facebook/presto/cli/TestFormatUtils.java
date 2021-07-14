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
package com.facebook.presto.cli;

import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;

public class TestFormatUtils
{
    @Test
    public void testFormatTime()
    {
        assertEquals(FormatUtils.formatTime(Duration.succinctNanos(100L)), "0ms");
        assertEquals(FormatUtils.formatTime(Duration.succinctDuration(1.1, TimeUnit.MILLISECONDS)), "1ms");
        assertEquals(FormatUtils.formatTime(Duration.succinctDuration(1.1, TimeUnit.SECONDS)), "0:01");
        assertEquals(FormatUtils.formatTime(Duration.succinctDuration(1.5, TimeUnit.MINUTES)), "1:30");
        assertEquals(FormatUtils.formatTime(Duration.succinctDuration(1.5, TimeUnit.HOURS)), "90:00");
    }
}
