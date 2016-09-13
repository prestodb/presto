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
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import javax.annotation.Nullable;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.concurrent.Semaphore;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class AtopPageSource
        implements ConnectorPageSource
{
    private final Semaphore readerPermits;
    private final AtopFactory atopFactory;
    private final ConnectorSession session;
    private final Slice hostIp;
    private final AtopTable table;
    private final ZonedDateTime date;
    private final List<AtopColumn> columns;
    private final List<Type> types;
    private final PageBuilder pageBuilder;
    @Nullable
    private Atop atop;
    private boolean finished;

    public AtopPageSource(Semaphore readerPermits, AtopFactory atopFactory, ConnectorSession session, Slice hostIp, AtopTable table, ZonedDateTime date, List<AtopColumn> columns, List<Type> types)
    {
        this.readerPermits = requireNonNull(readerPermits, "readerPermits is null");
        this.atopFactory = requireNonNull(atopFactory, "atopFactory is null");
        this.session = requireNonNull(session, "session is null");
        this.hostIp = requireNonNull(hostIp, "hostIp is null");
        this.table = requireNonNull(table, "table is null");
        this.date = requireNonNull(date, "date is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        checkArgument(columns.size() == types.size(), "columns (%s) does not match types (%s)", columns.size(), types.size());
        pageBuilder = new PageBuilder(types);
    }

    @Override
    public void close()
    {
        finished = true;
        if (atop != null) {
            try {
                atop.close();
            }
            finally {
                atop = null;
                readerPermits.release();
            }
        }
    }

    @Override
    public long getTotalBytes()
    {
        return 0;
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public Page getNextPage()
    {
        if (atop == null) {
            try {
                readerPermits.acquire();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
            }
            try {
                atop = atopFactory.create(table, date);
            }
            catch (RuntimeException e) {
                atop = null;
                readerPermits.release();
                throw e;
            }
        }

        while (!pageBuilder.isFull()) {
            if (!atop.hasNext()) {
                close();
                break;
            }
            String row = atop.next();

            if (row.equals("SEP")) {
                continue;
            }

            if (table == AtopTable.REBOOTS) {
                // For the reboots table we're only interested in RESET events, and the event that follows them
                if (!row.equals("RESET")) {
                    continue;
                }
                if (!atop.hasNext()) {
                    close();
                    break;
                }
                row = atop.next();
            }
            else {
                if (row.equals("RESET")) {
                    if (atop.hasNext()) {
                        // Drop samples immediately after a RESET, since this will be a "since boot" event
                        // which tends to have a very long duration
                        atop.next();
                    }
                    continue;
                }
            }

            List<String> fields = Splitter.on(" ").splitToList(row);
            pageBuilder.declarePosition();
            for (int i = 0; i < columns.size(); i++) {
                if (columns.get(i) == AtopColumn.HOST_IP) {
                    types.get(i).writeSlice(pageBuilder.getBlockBuilder(i), hostIp);
                }
                else {
                    columns.get(i).getParser().parse(fields, types.get(i), pageBuilder.getBlockBuilder(i), session);
                }
            }
        }

        Page page = pageBuilder.build();
        pageBuilder.reset();

        return page;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }
}
