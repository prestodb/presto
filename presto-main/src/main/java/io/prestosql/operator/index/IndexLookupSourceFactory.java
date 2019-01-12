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
package io.prestosql.operator.index;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.DataSize;
import io.prestosql.operator.LookupSourceFactory;
import io.prestosql.operator.LookupSourceProvider;
import io.prestosql.operator.OuterPositionIterator;
import io.prestosql.operator.PagesIndex;
import io.prestosql.operator.StaticLookupSourceProvider;
import io.prestosql.operator.TaskContext;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.gen.JoinCompiler;
import io.prestosql.sql.planner.Symbol;

import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

public class IndexLookupSourceFactory
        implements LookupSourceFactory
{
    private final List<Type> outputTypes;
    private final Map<Symbol, Integer> layout;
    private final Supplier<IndexLoader> indexLoaderSupplier;
    private TaskContext taskContext;
    private final SettableFuture<?> whenTaskContextSet = SettableFuture.create();

    public IndexLookupSourceFactory(
            Set<Integer> lookupSourceInputChannels,
            List<Integer> keyOutputChannels,
            OptionalInt keyOutputHashChannel,
            List<Type> outputTypes,
            Map<Symbol, Integer> layout,
            IndexBuildDriverFactoryProvider indexBuildDriverFactoryProvider,
            DataSize maxIndexMemorySize,
            IndexJoinLookupStats stats,
            boolean shareIndexLoading,
            PagesIndex.Factory pagesIndexFactory,
            JoinCompiler joinCompiler)
    {
        this.outputTypes = ImmutableList.copyOf(requireNonNull(outputTypes, "outputTypes is null"));
        this.layout = ImmutableMap.copyOf(requireNonNull(layout, "layout is null"));

        if (shareIndexLoading) {
            IndexLoader shared = new IndexLoader(lookupSourceInputChannels, keyOutputChannels, keyOutputHashChannel, outputTypes, indexBuildDriverFactoryProvider, 10_000, maxIndexMemorySize, stats, pagesIndexFactory, joinCompiler);
            this.indexLoaderSupplier = () -> shared;
        }
        else {
            this.indexLoaderSupplier = () -> new IndexLoader(lookupSourceInputChannels, keyOutputChannels, keyOutputHashChannel, outputTypes, indexBuildDriverFactoryProvider, 10_000, maxIndexMemorySize, stats, pagesIndexFactory, joinCompiler);
        }
    }

    @Override
    public List<Type> getTypes()
    {
        return outputTypes;
    }

    @Override
    public List<Type> getOutputTypes()
    {
        return outputTypes;
    }

    @Override
    public Map<Symbol, Integer> getLayout()
    {
        return layout;
    }

    @Override
    public void setTaskContext(TaskContext taskContext)
    {
        this.taskContext = taskContext;
        whenTaskContextSet.set(null);
    }

    @Override
    public ListenableFuture<LookupSourceProvider> createLookupSourceProvider()
    {
        checkState(taskContext != null, "taskContext not set");

        IndexLoader indexLoader = indexLoaderSupplier.get();
        indexLoader.setContext(taskContext);
        return Futures.immediateFuture(new StaticLookupSourceProvider(new IndexLookupSource(indexLoader)));
    }

    @Override
    public ListenableFuture<?> whenBuildFinishes()
    {
        return Futures.transformAsync(
                whenTaskContextSet,
                ignored -> transform(
                        this.createLookupSourceProvider(),
                        lookupSourceProvider -> {
                            // Close the lookupSourceProvider we just created.
                            // The only reason we created it is to wait until lookup source is ready.
                            lookupSourceProvider.close();
                            return null;
                        },
                        directExecutor()),
                directExecutor());
    }

    @Override
    public int partitions()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public OuterPositionIterator getOuterPositionIterator()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void destroy()
    {
        // nothing to do
    }
}
