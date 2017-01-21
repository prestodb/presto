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
package com.facebook.presto.operator.index;

import com.facebook.presto.operator.LookupSource;
import com.facebook.presto.operator.LookupSourceFactory;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class IndexLookupSourceFactory
        implements LookupSourceFactory
{
    private final List<Type> outputTypes;
    private final Map<Symbol, Integer> layout;
    private final Supplier<IndexLoader> indexLoaderSupplier;
    private TaskContext taskContext;

    public IndexLookupSourceFactory(
            Set<Integer> lookupSourceInputChannels,
            List<Integer> keyOutputChannels,
            Optional<Integer> keyOutputHashChannel,
            List<Type> outputTypes,
            Map<Symbol, Integer> layout,
            IndexBuildDriverFactoryProvider indexBuildDriverFactoryProvider,
            DataSize maxIndexMemorySize,
            IndexJoinLookupStats stats,
            boolean shareIndexLoading)
    {
        this.outputTypes = ImmutableList.copyOf(requireNonNull(outputTypes, "outputTypes is null"));
        this.layout = ImmutableMap.copyOf(requireNonNull(layout, "layout is null"));

        if (shareIndexLoading) {
            IndexLoader shared = new IndexLoader(lookupSourceInputChannels, keyOutputChannels, keyOutputHashChannel, outputTypes, indexBuildDriverFactoryProvider, 10_000, maxIndexMemorySize, stats);
            this.indexLoaderSupplier = () -> shared;
        }
        else {
            this.indexLoaderSupplier = () -> new IndexLoader(lookupSourceInputChannels, keyOutputChannels, keyOutputHashChannel, outputTypes, indexBuildDriverFactoryProvider, 10_000, maxIndexMemorySize, stats);
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
    }

    @Override
    public ListenableFuture<LookupSource> createLookupSource()
    {
        checkState(taskContext != null, "taskContext not set");

        IndexLoader indexLoader = indexLoaderSupplier.get();
        indexLoader.setContext(taskContext);
        return Futures.immediateFuture(new IndexLookupSource(indexLoader));
    }

    @Override
    public void destroy()
    {
        // nothing to do
    }
}
