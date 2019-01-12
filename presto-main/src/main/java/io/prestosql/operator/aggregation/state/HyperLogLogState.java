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
package io.prestosql.operator.aggregation.state;

import io.airlift.stats.cardinality.HyperLogLog;
import io.prestosql.spi.function.AccumulatorState;
import io.prestosql.spi.function.AccumulatorStateMetadata;

@AccumulatorStateMetadata(stateSerializerClass = HyperLogLogStateSerializer.class, stateFactoryClass = HyperLogLogStateFactory.class)
public interface HyperLogLogState
        extends AccumulatorState
{
    int NUMBER_OF_BUCKETS = 4096;

    HyperLogLog getHyperLogLog();

    void setHyperLogLog(HyperLogLog value);

    void addMemoryUsage(int value);
}
