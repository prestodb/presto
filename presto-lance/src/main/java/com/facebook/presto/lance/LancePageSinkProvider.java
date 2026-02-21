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
package com.facebook.presto.lance;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PageSinkContext;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import org.apache.arrow.vector.types.pojo.Schema;

import javax.inject.Inject;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class LancePageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final LanceNamespaceHolder namespaceHolder;
    private final JsonCodec<LanceCommitTaskData> jsonCodec;

    @Inject
    public LancePageSinkProvider(
            LanceNamespaceHolder namespaceHolder,
            JsonCodec<LanceCommitTaskData> jsonCodec)
    {
        this.namespaceHolder = requireNonNull(namespaceHolder, "namespaceHolder is null");
        this.jsonCodec = requireNonNull(jsonCodec, "jsonCodec is null");
    }

    @Override
    public ConnectorPageSink createPageSink(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorOutputTableHandle outputTableHandle,
            PageSinkContext pageSinkContext)
    {
        LanceWritableTableHandle handle = (LanceWritableTableHandle) outputTableHandle;
        return createPageSink(handle);
    }

    @Override
    public ConnectorPageSink createPageSink(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorInsertTableHandle insertTableHandle,
            PageSinkContext pageSinkContext)
    {
        LanceWritableTableHandle handle = (LanceWritableTableHandle) insertTableHandle;
        return createPageSink(handle);
    }

    private ConnectorPageSink createPageSink(LanceWritableTableHandle handle)
    {
        Schema arrowSchema;
        try {
            arrowSchema = Schema.fromJSON(handle.getSchemaJson());
        }
        catch (IOException e) {
            throw new PrestoException(LanceErrorCode.LANCE_ERROR,
                    "Failed to parse Arrow schema", e);
        }

        String tablePath = namespaceHolder.getTablePath(
                handle.getSchemaName(), handle.getTableName());

        return new LancePageSink(
                tablePath,
                arrowSchema,
                handle.getInputColumns(),
                jsonCodec);
    }
}
