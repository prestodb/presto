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
package com.facebook.presto.lance.ingestion;

import com.facebook.presto.common.Page;
import com.facebook.presto.lance.LanceConfig;
import com.facebook.presto.spi.ConnectorPageSink;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.lancedb.lance.FragmentMetadata;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class LancePageSink
        implements ConnectorPageSink
{
    private final LanceConfig lanceConfig;
    private final LanceIngestionTableHandle tableHandle;
    private final LancePageWriter lancePageWriter;
    private final Schema arrowSchema;
    private final HashSet<FragmentMetadata> fragmentMetaSet;

    public LancePageSink(
            LanceConfig lanceConfig,
            LanceIngestionTableHandle tableHandle,
            LancePageWriter lancePageWriter, Schema arrowSchema)
    {
        this.lanceConfig = requireNonNull(lanceConfig, "LanceConfig is null");
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.lancePageWriter = requireNonNull(lancePageWriter, "pageWriter is null");
        this.arrowSchema = requireNonNull(arrowSchema, "arrowSchema is null");
        this.fragmentMetaSet = new HashSet<>();
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        fragmentMetaSet.add(lancePageWriter.append(page, tableHandle, arrowSchema));
        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        ImmutableCollection.Builder<Slice> fragmentSliceList = ImmutableList.builder();
        for (FragmentMetadata fragmentMetadata : fragmentMetaSet) {
            fragmentSliceList.add(Slices.wrappedBuffer(fragmentMetadata.getJsonMetadata().getBytes()));
        }
        return completedFuture(fragmentSliceList.build());
    }

    @Override
    public void abort()
    {
    }
}
