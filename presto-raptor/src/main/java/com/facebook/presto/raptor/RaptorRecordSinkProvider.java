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
package com.facebook.presto.raptor;

import com.facebook.presto.raptor.storage.ColumnFileHandle;
import com.facebook.presto.raptor.storage.LocalStorageManager;
import com.facebook.presto.raptor.util.CurrentNodeId;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.RecordSink;
import com.google.common.base.Throwables;

import javax.inject.Inject;

import java.io.IOException;
import java.util.List;

import static com.facebook.presto.util.Types.checkType;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.UUID.randomUUID;

public class RaptorRecordSinkProvider
        implements ConnectorRecordSinkProvider
{
    private final LocalStorageManager storageManager;
    private final String nodeId;

    @Inject
    public RaptorRecordSinkProvider(LocalStorageManager storageManager, CurrentNodeId currentNodeId)
    {
        this(storageManager, currentNodeId.toString());
    }

    public RaptorRecordSinkProvider(LocalStorageManager storageManager, String nodeId)
    {
        this.storageManager = checkNotNull(storageManager, "storageManager is null");
        this.nodeId = checkNotNull(nodeId, "nodeId is null");
    }

    @Override
    public RecordSink getRecordSink(ConnectorOutputTableHandle tableHandle)
    {
        RaptorOutputTableHandle handle = checkType(tableHandle, RaptorOutputTableHandle.class, "tableHandle");

        ColumnFileHandle fileHandle = createStagingFileHandle(handle.getColumnHandles());

        return new RaptorRecordSink(nodeId, fileHandle, storageManager, handle.getColumnTypes(), handle.getSampleWeightColumnHandle());
    }

    private ColumnFileHandle createStagingFileHandle(List<RaptorColumnHandle> columnHandles)
    {
        try {
            return storageManager.createStagingFileHandles(randomUUID(), columnHandles);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
