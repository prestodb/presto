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

import java.util.Optional;

import static com.facebook.presto.benchmark.event.BenchmarkPhaseEvent.Status.COMPLETED_WITH_FAILURES;
import static com.facebook.presto.benchmark.event.BenchmarkPhaseEvent.Status.FAILED;
import static com.facebook.presto.benchmark.event.BenchmarkPhaseEvent.Status.SUCCEEDED;
import static java.util.Objects.requireNonNull;

@Immutable
@EventType("BenchmarkPhase")
public class BenchmarkPhaseEvent
{
    public enum Status
    {
        SUCCEEDED,
        COMPLETED_WITH_FAILURES,
        FAILED
    }

    private final String name;
    private final String status;
    private final String errorMessage;

    public BenchmarkPhaseEvent(
            String name,
            Status status,
            Optional<String> errorMessage)
    {
        this.name = requireNonNull(name, "name is null");
        this.status = requireNonNull(status, "status is null").name();
        this.errorMessage = requireNonNull(errorMessage, "errorMessage is null").orElse(null);
    }

    public static BenchmarkPhaseEvent succeeded(String name)
    {
        return new BenchmarkPhaseEvent(name, SUCCEEDED, Optional.empty());
    }

    public static BenchmarkPhaseEvent failed(String name, String errorMessage)
    {
        return new BenchmarkPhaseEvent(name, FAILED, Optional.of(errorMessage));
    }

    public static BenchmarkPhaseEvent completedWithFailures(String name, String errorMessage)
    {
        return new BenchmarkPhaseEvent(name, COMPLETED_WITH_FAILURES, Optional.of(errorMessage));
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

    @EventField
    public String getErrorMessage()
    {
        return errorMessage;
    }

    public Status getEventStatus()
    {
        return Status.valueOf(status);
    }
}
