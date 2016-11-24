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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.bytecode.DynamicClassLoader;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

import java.util.List;
import java.util.Optional;

public class LazyAccumulatorFactoryBinder
        implements AccumulatorFactoryBinder
{
    private final Supplier<AccumulatorFactoryBinder> binder;

    public LazyAccumulatorFactoryBinder(AggregationMetadata metadata, DynamicClassLoader classLoader)
    {
        binder = Suppliers.memoize(() -> AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader));
    }

    @Override
    public AccumulatorFactory bind(List<Integer> argumentChannels, Optional<Integer> maskChannel)
    {
        return binder.get().bind(argumentChannels, maskChannel);
    }
}
