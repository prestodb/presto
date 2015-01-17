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

import io.airlift.stats.CounterStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import static com.google.common.base.Preconditions.checkNotNull;

public final class SqlTaskIoStats
{
    private final CounterStat inputDataSize;
    private final CounterStat inputPositions;
    private final CounterStat outputDataSize;
    private final CounterStat outputPositions;

    public SqlTaskIoStats()
    {
        this(new CounterStat(), new CounterStat(), new CounterStat(), new CounterStat());
    }

    public SqlTaskIoStats(CounterStat inputDataSize, CounterStat inputPositions, CounterStat outputDataSize, CounterStat outputPositions)
    {
        this.inputDataSize = checkNotNull(inputDataSize, "inputDataSize is null");
        this.inputPositions = checkNotNull(inputPositions, "inputPositions is null");
        this.outputDataSize = checkNotNull(outputDataSize, "outputDataSize is null");
        this.outputPositions = checkNotNull(outputPositions, "outputPositions is null");
    }

    @Managed
    @Nested
    public CounterStat getInputDataSize()
    {
        return inputDataSize;
    }

    @Managed
    @Nested
    public CounterStat getInputPositions()
    {
        return inputPositions;
    }

    @Managed
    @Nested
    public CounterStat getOutputDataSize()
    {
        return outputDataSize;
    }

    @Managed
    @Nested
    public CounterStat getOutputPositions()
    {
        return outputPositions;
    }

    public void merge(SqlTaskIoStats ioStats)
    {
        inputDataSize.merge(ioStats.inputDataSize);
        inputPositions.merge(ioStats.inputPositions);
        outputDataSize.merge(ioStats.outputDataSize);
        outputPositions.merge(ioStats.outputPositions);
    }

    @SuppressWarnings("deprecation")
    public void resetTo(SqlTaskIoStats ioStats)
    {
        inputDataSize.resetTo(ioStats.inputDataSize);
        inputPositions.resetTo(ioStats.inputPositions);
        outputDataSize.resetTo(ioStats.outputDataSize);
        outputPositions.resetTo(ioStats.outputPositions);
    }
}
