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
package com.facebook.presto.accumulo.io;

import com.facebook.presto.accumulo.metadata.AccumuloTable;
import com.facebook.presto.accumulo.model.AccumuloColumnHandle;
import com.facebook.presto.accumulo.model.Row;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeUtils;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.accumulo.AccumuloErrorCode.ACCUMULO_TABLE_DNE;
import static com.facebook.presto.accumulo.AccumuloErrorCode.UNEXPECTED_ACCUMULO_ERROR;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * Output class for serializing Presto pages (blocks of rows of data) to Accumulo.
 * This class converts the rows from within a page to a collection of Accumulo Mutations,
 * writing and indexed the rows. Writers are flushed and closed on commit, and if a rollback occurs...
 * we'll you're gonna have a bad time.
 *
 * @see AccumuloPageSinkProvider
 */
public class AccumuloPageSink
        implements ConnectorPageSink
{
    private final PrestoBatchWriter batchWriter;
    private final List<AccumuloColumnHandle> columns;
    private long numRows;

    public AccumuloPageSink(
            Connector connector,
            AccumuloTable table,
            Authorizations auths,
            String username)
    {
        requireNonNull(connector, "connector is null");
        requireNonNull(config, "config is null");
        requireNonNull(auths, "auths is null");
        requireNonNull(table, "table is null");

        this.columns = table.getColumns();

        try {
            this.batchWriter = new PrestoBatchWriter(connector, config, auths, table);
        }
        catch (AccumuloException | AccumuloSecurityException e) {
            throw new PrestoException(UNEXPECTED_ACCUMULO_ERROR, "Accumulo error when creating PrestoBatchWriter", e);
        }
        catch (TableNotFoundException e) {
            throw new PrestoException(ACCUMULO_TABLE_DNE, "Accumulo error when creating PrestoBatchWriter, table does not exist", e);
        }
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        // For each position within the page, i.e. row
        for (int position = 0; position < page.getPositionCount(); ++position) {
            Row row = new Row();
            // For each channel within the page, i.e. column
            for (int channel = 0; channel < page.getChannelCount(); ++channel) {
                // Get the type for this channel
                Type type = columns.get(channel).getType();

                // Read the value from the page and append the field to the row
                row.addField(TypeUtils.readNativeValue(type, page.getBlock(channel), position), type);
            }

            try {
                batchWriter.addRow(row);

                // TODO Fix arbitrary flush every 100k rows
                if (++numRows % 100_000 == 0) {
                    batchWriter.flush();
                }
            }
            catch (MutationsRejectedException e) {
                throw new PrestoException(UNEXPECTED_ACCUMULO_ERROR, "Mutation rejected by server on flush", e);
            }
        }

        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        try {
            batchWriter.close();
        }
        catch (MutationsRejectedException e) {
            throw new PrestoException(UNEXPECTED_ACCUMULO_ERROR, "Mutation rejected by server on flush", e);
        }

        // TODO Look into any use of the metadata for writing out the rows
        return completedFuture(ImmutableList.of());
    }

    @Override
    public void abort()
    {
        getFutureValue(finish());
    }
}
