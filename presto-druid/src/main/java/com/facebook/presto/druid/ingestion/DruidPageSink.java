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
package com.facebook.presto.druid.ingestion;

import com.facebook.presto.common.Page;
import com.facebook.presto.druid.DruidClient;
import com.facebook.presto.spi.ConnectorPageSink;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.CompletableFuture.completedFuture;

public class DruidPageSink
        implements ConnectorPageSink
{
    private final DruidClient druidClient;
    private final DruidIngestionTableHandle tableHandle;

    public DruidPageSink(DruidClient druidClient, DruidIngestionTableHandle tableHandle)
    {
        this.druidClient = druidClient;
        this.tableHandle = tableHandle;
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return completedFuture(ImmutableList.of());
    }

    @Override
    public void abort()
    {
    }
}
