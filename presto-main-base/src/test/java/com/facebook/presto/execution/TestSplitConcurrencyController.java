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
package com.facebook.presto.execution;

import io.airlift.units.Duration;
import org.testng.annotations.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;

public class TestSplitConcurrencyController
{
    @Test
    public void testRampup()
    {
        SplitConcurrencyController controller = new SplitConcurrencyController(1, new Duration(1, SECONDS));
        for (int i = 0; i < 10; i++) {
            controller.update(SECONDS.toNanos(2), 0, i + 1);
            assertEquals(controller.getTargetConcurrency(), i + 2);
        }
    }

    @Test
    public void testRampdown()
    {
        SplitConcurrencyController controller = new SplitConcurrencyController(10, new Duration(1, SECONDS));
        for (int i = 0; i < 9; i++) {
            controller.update(SECONDS.toNanos(2), 1, 10 - i);
            controller.splitFinished(SECONDS.toNanos(30), 1, 10 - i);
            assertEquals(controller.getTargetConcurrency(), 10 - i - 1);
        }
    }

    @Test
    public void testRapidAdjustForQuickSplits()
    {
        SplitConcurrencyController controller = new SplitConcurrencyController(10, new Duration(1, SECONDS));
        for (int i = 0; i < 9; i++) {
            controller.update(MILLISECONDS.toNanos(200), 1, 10 - i);
            controller.splitFinished(MILLISECONDS.toNanos(100), 1, 10 - i);
            assertEquals(controller.getTargetConcurrency(), 10 - i - 1);
        }
        controller.update(SECONDS.toNanos(30), 0, 1);
        for (int i = 0; i < 10; i++) {
            controller.update(SECONDS.toNanos(200), 0, i + 1);
            controller.splitFinished(MILLISECONDS.toNanos(100), 0, i + 1);
            assertEquals(controller.getTargetConcurrency(), i + 2);
        }
    }
}
