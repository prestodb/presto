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
package com.facebook.presto.atop;

import com.facebook.presto.atop.AtopTable.AtopColumn;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.util.List;
import java.util.concurrent.Semaphore;

import static com.facebook.presto.atop.Types.checkType;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Objects.requireNonNull;

public final class AtopPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final Semaphore readerPermits;
    private final DateTimeZone timeZone;
    private final AtopFactory atopFactory;
    private final TypeManager typeManager;

    @Inject
    public AtopPageSourceProvider(AtopConnectorConfig config, AtopFactory atopFactory, TypeManager typeManager)
    {
        readerPermits = new Semaphore(requireNonNull(config, "config is null").getConcurrentReadersPerNode());
        timeZone = config.getDateTimeZone();
        this.atopFactory = requireNonNull(atopFactory, "atopFactory is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorSplit split,
            List<ColumnHandle> columns)
    {
        AtopSplit atopSplit = checkType(split, AtopSplit.class, "split");

        ImmutableList.Builder<Type> types = ImmutableList.builder();
        ImmutableList.Builder<AtopColumn> atopColumns = ImmutableList.builder();

        for (ColumnHandle column : columns) {
            AtopColumnHandle atopColumnHandle = checkType(column, AtopColumnHandle.class, "column");
            AtopColumn atopColumn = atopSplit.getTable().getColumn(atopColumnHandle.getName());
            atopColumns.add(atopColumn);
            types.add(typeManager.getType(atopColumn.getType()));
        }

        // Timezone is not preserved during JSON serialization
        DateTime date = atopSplit.getDate().withZone(timeZone);
        checkState(date.equals(date.withTimeAtStartOfDay()), "Expected date to be at beginning of day");
        return new AtopPageSource(readerPermits, atopFactory, session, utf8Slice(atopSplit.getHost().getHostText()), atopSplit.getTable(), date, atopColumns.build(), types.build());
    }
}
