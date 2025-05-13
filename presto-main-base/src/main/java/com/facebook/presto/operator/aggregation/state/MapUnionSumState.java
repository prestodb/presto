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
package com.facebook.presto.operator.aggregation.state;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.aggregation.Adder;
import com.facebook.presto.operator.aggregation.MapUnionSumResult;
import com.facebook.presto.spi.function.AccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateMetadata;

@AccumulatorStateMetadata(stateFactoryClass = MapUnionSumStateFactory.class, stateSerializerClass = MapUnionSumStateSerializer.class)
public interface MapUnionSumState
        extends AccumulatorState
{
    MapUnionSumResult get();

    void set(MapUnionSumResult value);

    void addMemoryUsage(long memory);

    Type getKeyType();

    Type getValueType();

    Adder getAdder();
}
