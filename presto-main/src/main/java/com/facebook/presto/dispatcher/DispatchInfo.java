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
package com.facebook.presto.dispatcher;

import com.facebook.presto.execution.ExecutionFailureInfo;
import io.airlift.units.Duration;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DispatchInfo
{
    private final Optional<CoordinatorLocation> coordinatorLocation;
    private final Optional<ExecutionFailureInfo> failureInfo;
    private final Duration elapsedTime;
    private final Duration queuedTime;

    public static DispatchInfo queued(Duration elapsedTime, Duration queuedTime)
    {
        return new DispatchInfo(Optional.empty(), Optional.empty(), elapsedTime, queuedTime);
    }

    public static DispatchInfo dispatched(CoordinatorLocation coordinatorLocation, Duration elapsedTime, Duration queuedTime)
    {
        requireNonNull(coordinatorLocation, "coordinatorLocation is null");
        return new DispatchInfo(Optional.of(coordinatorLocation), Optional.empty(), elapsedTime, queuedTime);
    }

    public static DispatchInfo failed(ExecutionFailureInfo failureInfo, Duration elapsedTime, Duration queuedTime)
    {
        requireNonNull(failureInfo, "coordinatorLocation is null");
        return new DispatchInfo(Optional.empty(), Optional.of(failureInfo), elapsedTime, queuedTime);
    }

    private DispatchInfo(Optional<CoordinatorLocation> coordinatorLocation, Optional<ExecutionFailureInfo> failureInfo, Duration elapsedTime, Duration queuedTime)
    {
        this.coordinatorLocation = requireNonNull(coordinatorLocation, "coordinatorLocation is null");
        this.failureInfo = requireNonNull(failureInfo, "failureInfo is null");
        this.elapsedTime = requireNonNull(elapsedTime, "elapsedTime is null");
        this.queuedTime = requireNonNull(queuedTime, "queuedTime is null");
    }

    public Optional<CoordinatorLocation> getCoordinatorLocation()
    {
        return coordinatorLocation;
    }

    public Optional<ExecutionFailureInfo> getFailureInfo()
    {
        return failureInfo;
    }

    public Duration getElapsedTime()
    {
        return elapsedTime;
    }

    public Duration getQueuedTime()
    {
        return queuedTime;
    }
}
