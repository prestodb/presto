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
package io.prestosql.spi.eventlistener;

import java.time.Duration;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class SplitStatistics
{
    private final Duration cpuTime;
    private final Duration wallTime;
    private final Duration queuedTime;
    private final Duration completedReadTime;

    private final long completedPositions;
    private final long completedDataSizeBytes;

    private final Optional<Duration> timeToFirstByte;
    private final Optional<Duration> timeToLastByte;

    public SplitStatistics(
            Duration cpuTime,
            Duration wallTime,
            Duration queuedTime,
            Duration completedReadTime,
            long completedPositions,
            long completedDataSizeBytes,
            Optional<Duration> timeToFirstByte,
            Optional<Duration> timeToLastByte)
    {
        this.cpuTime = requireNonNull(cpuTime, "cpuTime is null");
        this.wallTime = requireNonNull(wallTime, "wallTime is null");
        this.queuedTime = requireNonNull(queuedTime, "queuedTime is null");
        this.completedReadTime = requireNonNull(completedReadTime, "completedReadTime is null");
        this.completedPositions = completedPositions;
        this.completedDataSizeBytes = completedDataSizeBytes;
        this.timeToFirstByte = requireNonNull(timeToFirstByte, "timeToFirstByte is null");
        this.timeToLastByte = requireNonNull(timeToLastByte, "timeToLastByte is null");
    }

    public Duration getCpuTime()
    {
        return cpuTime;
    }

    public Duration getWallTime()
    {
        return wallTime;
    }

    public Duration getQueuedTime()
    {
        return queuedTime;
    }

    public Duration getCompletedReadTime()
    {
        return completedReadTime;
    }

    public long getCompletedPositions()
    {
        return completedPositions;
    }

    public long getCompletedDataSizeBytes()
    {
        return completedDataSizeBytes;
    }

    public Optional<Duration> getTimeToFirstByte()
    {
        return timeToFirstByte;
    }

    public Optional<Duration> getTimeToLastByte()
    {
        return timeToLastByte;
    }
}
