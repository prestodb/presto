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
package com.facebook.presto.hive;

import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import io.airlift.slice.Slice;
import org.joda.time.DateTimeZone;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class TimestampFixingHiveConnectorPageSink
        implements ConnectorPageSink
{
    private final ConnectorPageSink pageSink;
    private final TimestampRewriter timestampRewriter;

    public TimestampFixingHiveConnectorPageSink(ConnectorPageSink pageSink,
            HiveWritableTableHandle tableHandle,
            TypeManager typeManager,
            DateTimeZone storageTimeZone)
    {
        this.pageSink = pageSink;

        List<Type> columnTypes = tableHandle.getInputColumns().stream()
                .map(columnHandle -> typeManager.getType(columnHandle.getTypeSignature()))
                .collect(toImmutableList());

        timestampRewriter = new TimestampRewriter(columnTypes, storageTimeZone, typeManager);
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        return pageSink.appendPage(timestampRewriter.rewritePagePrestoToHive(page));
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return pageSink.finish();
    }

    @Override
    public void abort()
    {
        pageSink.abort();
    }
}
