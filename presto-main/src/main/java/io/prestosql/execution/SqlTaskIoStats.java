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
package io.prestosql.execution;

import io.airlift.stats.CounterStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import static java.util.Objects.requireNonNull;

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
        this.inputDataSize = requireNonNull(inputDataSize, "inputDataSize is null");
        this.inputPositions = requireNonNull(inputPositions, "inputPositions is null");
        this.outputDataSize = requireNonNull(outputDataSize, "outputDataSize is null");
        this.outputPositions = requireNonNull(outputPositions, "outputPositions is null");
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
