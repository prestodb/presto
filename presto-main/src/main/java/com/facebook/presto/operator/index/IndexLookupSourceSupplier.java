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
import com.facebook.presto.operator.LookupSourceSupplier;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class IndexLookupSourceSupplier
        implements LookupSourceSupplier
{
    private final List<Type> outputTypes;
    private final Supplier<IndexLoader> indexLoaderSupplier;

    public IndexLookupSourceSupplier(
            Set<Integer> lookupSourceInputChannels,
            List<Integer> keyOutputChannels,
            Optional<Integer> keyOutputHashChannel,
            List<Type> outputTypes,
            IndexBuildDriverFactoryProvider indexBuildDriverFactoryProvider,
            DataSize maxIndexMemorySize,
            IndexJoinLookupStats stats,
            boolean shareIndexLoading)
    {
        this.outputTypes = ImmutableList.copyOf(requireNonNull(outputTypes, "outputTypes is null"));

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
    public ListenableFuture<LookupSource> getLookupSource(OperatorContext operatorContext)
    {
        IndexLoader indexLoader = indexLoaderSupplier.get();
        indexLoader.setContext(operatorContext.getDriverContext().getPipelineContext().getTaskContext());
        return Futures.immediateFuture(new IndexLookupSource(indexLoader));
    }

    @Override
    public void release()
    {
        // no-op
    }

    @Override
    public void retain()
    {
        // no-op
    }
}
