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

import com.facebook.presto.lance.LanceConfig;
import com.facebook.presto.lance.ingestion.LanceIngestionTableHandle;
import com.facebook.presto.lance.ingestion.LancePageSink;
import com.facebook.presto.lance.ingestion.LancePageWriter;
import com.facebook.presto.lance.metadata.LanceColumnInfo;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PageSinkContext;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class LancePageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final LanceConfig lanceConfig;
    private final LancePageWriter lancePageWriter;

    @Inject
    public LancePageSinkProvider(
            LanceConfig lanceConfig,
            LancePageWriter lancePageWriter)
    {
        this.lanceConfig = requireNonNull(lanceConfig, "lance config is null");
        this.lancePageWriter = requireNonNull(lancePageWriter, "page writer is null");
    }
    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle, PageSinkContext pageSinkContext)
    {
        LanceIngestionTableHandle tableHandle = (LanceIngestionTableHandle) outputTableHandle;
        ImmutableList.Builder<Field> arrowFieldBuilder = ImmutableList.builder();
        for (LanceColumnInfo column : tableHandle.getColumns()) {
            arrowFieldBuilder.add(Field.nullable(column.getColumnName(), column.getDataType().getArrowType()));
        }
        Schema arrowSchema = new Schema(arrowFieldBuilder.build());
        return new LancePageSink(lanceConfig, tableHandle, lancePageWriter, arrowSchema);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle, PageSinkContext pageSinkContext)
    {
        LanceIngestionTableHandle tableHandle = (LanceIngestionTableHandle) insertTableHandle;
        ImmutableList.Builder<Field> arrowFieldBuilder = ImmutableList.builder();
        for (LanceColumnInfo column : tableHandle.getColumns()) {
            arrowFieldBuilder.add(Field.nullable(column.getColumnName(), column.getDataType().getArrowType()));
        }
        Schema arrowSchema = new Schema(arrowFieldBuilder.build());
        return new LancePageSink(lanceConfig, tableHandle, lancePageWriter, arrowSchema);
    }
}
