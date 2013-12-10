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

import com.facebook.presto.operator.DriverFactory;
import com.facebook.presto.operator.LookupSource;
import com.facebook.presto.operator.LookupSourceSupplier;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.index.PagesIndexBuilderOperator.PagesIndexBuilderOperatorFactory;
import com.facebook.presto.spi.type.Type;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

public class IndexLookupSourceSupplier
        implements LookupSourceSupplier
{
    private final IndexLoader indexLoader;

    public IndexLookupSourceSupplier(
            List<Integer> indexChannels,
            List<Type> types,
            int snapshotOperatorId,
            DriverFactory indexBuildDriverFactory,
            PagesIndexBuilderOperatorFactory pagesIndexOutput)
    {
        this.indexLoader = new IndexLoader(indexChannels, types, snapshotOperatorId, indexBuildDriverFactory, pagesIndexOutput, 1_000);
    }

    @Override
    public List<Type> getTypes()
    {
        return indexLoader.getOutputTypes();
    }

    @Override
    public ListenableFuture<LookupSource> getLookupSource(OperatorContext operatorContext)
    {
        indexLoader.setContext(operatorContext.getDriverContext().getPipelineContext().getTaskContext());
        return Futures.<LookupSource>immediateFuture(new IndexLookupSource(indexLoader));
    }
}
