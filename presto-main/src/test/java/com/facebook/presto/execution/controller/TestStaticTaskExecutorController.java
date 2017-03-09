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
package com.facebook.presto.execution.controller;

import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Optional;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;

public class TestStaticTaskExecutorController
{
    @Test
    public void testStaticTaskExecutorController()
    {
        TaskExectorController controller = new StaticTaskExecutorController(37);
        for (int i = 0; i < 10; i++) {
            TaskExecutorStatistics statistics = null;
            if (i % 2 == 1) {
                statistics = new TaskExecutorStatistics(
                        new Duration(2 * i + 1, SECONDS),
                        new Duration(i, SECONDS),
                        i + 23,
                        3 * 100);
            }
            assertEquals(37, controller.getNextRunnerThreads(Optional.ofNullable(statistics)));
        }
    }
}
