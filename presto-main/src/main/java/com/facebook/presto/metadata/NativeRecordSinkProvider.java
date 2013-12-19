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
package com.facebook.presto.metadata;

import com.facebook.presto.spi.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.OutputTableHandle;
import com.facebook.presto.spi.RecordSink;
import com.google.common.base.Throwables;
import io.airlift.node.NodeInfo;

import javax.inject.Inject;

import java.io.IOException;
import java.util.List;

import static com.facebook.presto.util.Types.checkType;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.UUID.randomUUID;

public class NativeRecordSinkProvider
        implements ConnectorRecordSinkProvider
{
    private final LocalStorageManager storageManager;
    private final String nodeId;

    @Inject
    public NativeRecordSinkProvider(LocalStorageManager storageManager, NodeInfo nodeInfo)
    {
        this.storageManager = checkNotNull(storageManager, "storageManager is null");
        nodeId = checkNotNull(nodeInfo, "nodeInfo is null").getNodeId();
    }

    @Override
    public boolean canHandle(OutputTableHandle tableHandle)
    {
        return tableHandle instanceof NativeOutputTableHandle;
    }

    @Override
    public RecordSink getRecordSink(OutputTableHandle tableHandle)
    {
        NativeOutputTableHandle handle = checkType(tableHandle, NativeOutputTableHandle.class, "tableHandle");

        ColumnFileHandle fileHandle = createStagingFileHandle(handle.getColumnHandles());

        return new NativeRecordSink(nodeId, fileHandle, storageManager, handle.getColumnTypes());
    }

    private ColumnFileHandle createStagingFileHandle(List<NativeColumnHandle> columnHandles)
    {
        try {
            return storageManager.createStagingFileHandles(randomUUID(), columnHandles);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
