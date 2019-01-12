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
package io.prestosql.plugin.kudu;

import io.airlift.slice.Slice;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.RecordPageSource;
import io.prestosql.spi.connector.UpdatablePageSource;
import org.apache.kudu.Schema;
import org.apache.kudu.client.Delete;
import org.apache.kudu.client.KeyEncoderAccessor;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.SessionConfiguration.FlushMode;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

public class KuduUpdatablePageSource
        implements UpdatablePageSource
{
    private final KuduClientSession clientSession;
    private final KuduTable table;
    private final RecordPageSource inner;

    public KuduUpdatablePageSource(KuduRecordSet recordSet)
    {
        this.clientSession = recordSet.getClientSession();
        this.table = recordSet.getTable();
        this.inner = new RecordPageSource(recordSet);
    }

    @Override
    public void deleteRows(Block rowIds)
    {
        Schema schema = table.getSchema();
        KuduSession session = clientSession.newSession();
        session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND);
        try {
            try {
                for (int i = 0; i < rowIds.getPositionCount(); i++) {
                    int len = rowIds.getSliceLength(i);
                    Slice slice = rowIds.getSlice(i, 0, len);
                    PartialRow row = KeyEncoderAccessor.decodePrimaryKey(schema, slice.getBytes());
                    Delete delete = table.newDelete();
                    RowHelper.copyPrimaryKey(schema, row, delete.getRow());
                    session.apply(delete);
                }
            }
            finally {
                session.close();
            }
        }
        catch (KuduException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        CompletableFuture<Collection<Slice>> cf = new CompletableFuture<>();
        cf.complete(Collections.emptyList());
        return cf;
    }

    @Override
    public long getCompletedBytes()
    {
        return inner.getCompletedBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return inner.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return inner.isFinished();
    }

    @Override
    public Page getNextPage()
    {
        return inner.getNextPage();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return inner.getSystemMemoryUsage();
    }

    @Override
    public void close()
    {
        inner.close();
    }
}
