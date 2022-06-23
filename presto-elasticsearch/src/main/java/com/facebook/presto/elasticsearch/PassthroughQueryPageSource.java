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
package com.facebook.presto.elasticsearch;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.elasticsearch.client.ElasticsearchClient;
import com.facebook.presto.spi.ConnectorPageSource;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class PassthroughQueryPageSource
        implements ConnectorPageSource
{
    private final long readTimeNanos;
    private final String result;
    private final Type jsonType;
    private boolean done;

    public PassthroughQueryPageSource(ElasticsearchClient client, ElasticsearchTableHandle table, Type jsonType)
    {
        requireNonNull(client, "client is null");
        requireNonNull(table, "table is null");
        this.jsonType = requireNonNull(jsonType, "jsonType is null");

        long start = System.nanoTime();
        result = client.executeQuery(table.getIndex(), table.getQuery().get());
        readTimeNanos = System.nanoTime() - start;
    }

    @Override
    public long getCompletedBytes()
    {
        return result.length();
    }

    @Override
    public long getCompletedPositions()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return done;
    }

    @Override
    public Page getNextPage()
    {
        if (done) {
            return null;
        }

        done = true;

        PageBuilder page = new PageBuilder(1, ImmutableList.of(jsonType));
        page.declarePosition();
        BlockBuilder column = page.getBlockBuilder(0);
        jsonType.writeSlice(column, Slices.utf8Slice(result));
        return page.build();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
            throws IOException
    {
    }
}
