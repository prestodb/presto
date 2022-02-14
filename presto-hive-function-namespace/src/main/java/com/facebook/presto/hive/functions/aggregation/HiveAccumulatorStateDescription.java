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

package com.facebook.presto.hive.functions.aggregation;

import com.facebook.presto.spi.function.AccumulatorStateDescription;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;

import static java.util.Objects.requireNonNull;

public class HiveAccumulatorStateDescription
        implements AccumulatorStateDescription
{
    private final Class<?> stateInterface;
    private final AccumulatorStateSerializer<?> stateSerializer;
    private final AccumulatorStateFactory<?> stateFactory;

    public HiveAccumulatorStateDescription(
            Class<?> stateInterface,
            AccumulatorStateSerializer<?> stateSerializer,
            AccumulatorStateFactory<?> stateFactory)
    {
        this.stateInterface = requireNonNull(stateInterface);
        this.stateSerializer = requireNonNull(stateSerializer);
        this.stateFactory = requireNonNull(stateFactory);
    }

    @Override
    public Class<?> getAccumulatorStateInterface()
    {
        return stateInterface;
    }

    @Override
    public AccumulatorStateSerializer<?> getAccumulatorStateSerializer()
    {
        return stateSerializer;
    }

    @Override
    public AccumulatorStateFactory<?> getAccumulatorStateFactory()
    {
        return stateFactory;
    }
}
