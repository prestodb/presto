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
package io.prestosql.operator.aggregation;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import io.airlift.bytecode.DynamicClassLoader;
import io.prestosql.Session;
import io.prestosql.operator.PagesIndex;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.gen.JoinCompiler;

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

    @VisibleForTesting
    public GenericAccumulatorFactoryBinder getGenericAccumulatorFactoryBinder()
    {
        return (GenericAccumulatorFactoryBinder) binder.get();
    }

    @Override
    public AccumulatorFactory bind(
            List<Integer> argumentChannels,
            Optional<Integer> maskChannel,
            List<Type> sourceTypes,
            List<Integer> orderByChannels,
            List<SortOrder> orderings,
            PagesIndex.Factory pagesIndexFactory,
            boolean distinct,
            JoinCompiler joinCompiler,
            List<LambdaProvider> lambdaProviders,
            Session session)
    {
        return binder.get().bind(argumentChannels, maskChannel, sourceTypes, orderByChannels, orderings, pagesIndexFactory, distinct, joinCompiler, lambdaProviders, session);
    }
}
