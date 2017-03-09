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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;

public class TestTaskExecutorStatistics
{
    @Test
    public void testTaskExecutorStatistics()
    {
        int cpus = Runtime.getRuntime().availableProcessors();
        double expectedCpuUtilization = 0.67;
        int expectedWallTimeSeconds = 123;
        TaskExecutorStatistics statistics = new TaskExecutorStatistics(
                new Duration(expectedWallTimeSeconds * cpus * expectedCpuUtilization, SECONDS),
                new Duration(expectedWallTimeSeconds, SECONDS),
                123,
                456);
        assertEquals(statistics.getCpuTime().getValue(SECONDS), expectedWallTimeSeconds * cpus * expectedCpuUtilization);
        assertEquals(statistics.getWallTime().getValue(SECONDS), (double) expectedWallTimeSeconds);
        assertEquals(statistics.getCpuUtilization(), expectedCpuUtilization);
        assertEquals(statistics.getRunnerThreads(), 123);
        assertEquals(statistics.getTotalSplits(), 456);
    }
}
