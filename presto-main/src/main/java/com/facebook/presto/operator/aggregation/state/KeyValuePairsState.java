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

import com.facebook.presto.operator.aggregation.KeyValuePairs;
import com.facebook.presto.spi.function.AccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateMetadata;
import com.facebook.presto.spi.type.Type;

@AccumulatorStateMetadata(stateFactoryClass = KeyValuePairsStateFactory.class, stateSerializerClass = KeyValuePairStateSerializer.class)
public interface KeyValuePairsState
    extends AccumulatorState
{
    KeyValuePairs get();

    void set(KeyValuePairs value);

    void addMemoryUsage(long memory);

    Type getKeyType();

    Type getValueType();
}
