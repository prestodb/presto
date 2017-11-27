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

import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.annotations.VisibleForTesting;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class TimestampFixingHiveConnectorPageSource
        implements ConnectorPageSource
{
    private final ConnectorPageSource connectorPageSource;
    private final TimestampRewriter timestampRewriter;

    public TimestampFixingHiveConnectorPageSource(ConnectorPageSource connectorPageSource,
            List<HiveColumnHandle> columns,
            TypeManager typeManager,
            DateTimeZone storageTimeZone)
    {
        this.connectorPageSource = connectorPageSource;

        List<Type> columnTypes = columns.stream()
                .map(columnHandle -> typeManager.getType(columnHandle.getTypeSignature()))
                .collect(toImmutableList());

        timestampRewriter = new TimestampRewriter(columnTypes, storageTimeZone, typeManager);
    }

    @Override
    public long getCompletedBytes()
    {
        return connectorPageSource.getCompletedBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return connectorPageSource.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return connectorPageSource.isFinished();
    }

    @Override
    public Page getNextPage()
    {
        Page nextPage = connectorPageSource.getNextPage();
        if (nextPage == null) {
            return null;
        }
        return timestampRewriter.rewritePageHiveToPresto(nextPage);
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return connectorPageSource.getSystemMemoryUsage();
    }

    @Override
    public void close()
            throws IOException
    {
        connectorPageSource.close();
    }

    @VisibleForTesting
        // This should never be used outside of tests
    ConnectorPageSource getInternalPageSource()
    {
        return connectorPageSource;
    }
}
