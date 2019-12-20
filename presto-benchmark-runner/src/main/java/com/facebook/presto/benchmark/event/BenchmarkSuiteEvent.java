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
package com.facebook.presto.benchmark.event;

import com.facebook.airlift.event.client.EventField;
import com.facebook.airlift.event.client.EventType;

import javax.annotation.concurrent.Immutable;

import static com.facebook.presto.benchmark.event.BenchmarkSuiteEvent.Status.COMPLETED_WITH_FAILURES;
import static com.facebook.presto.benchmark.event.BenchmarkSuiteEvent.Status.FAILED;
import static com.facebook.presto.benchmark.event.BenchmarkSuiteEvent.Status.SUCCEEDED;
import static java.util.Objects.requireNonNull;

@Immutable
@EventType("BenchmarkSuite")
public class BenchmarkSuiteEvent
{
    public enum Status
    {
        SUCCEEDED,
        COMPLETED_WITH_FAILURES,
        FAILED
    }

    private final String name;
    private final String status;

    public BenchmarkSuiteEvent(
            String name,
            Status status)
    {
        this.name = requireNonNull(name, "name is null");
        this.status = requireNonNull(status, "status is null").name();
    }

    public static BenchmarkSuiteEvent succeeded(String name)
    {
        return new BenchmarkSuiteEvent(name, SUCCEEDED);
    }

    public static BenchmarkSuiteEvent failed(String name)
    {
        return new BenchmarkSuiteEvent(name, FAILED);
    }

    public static BenchmarkSuiteEvent completedWithFailures(String name)
    {
        return new BenchmarkSuiteEvent(name, COMPLETED_WITH_FAILURES);
    }

    @EventField
    public String getName()
    {
        return name;
    }

    @EventField
    public String getStatus()
    {
        return status;
    }
}
