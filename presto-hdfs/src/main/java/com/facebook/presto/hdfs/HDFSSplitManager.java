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
package com.facebook.presto.hdfs;

import com.facebook.presto.hdfs.exception.TableNotFoundException;
import com.facebook.presto.hdfs.fs.FSFactory;
import com.facebook.presto.hdfs.metaserver.MetaServer;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.inject.Inject;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.hdfs.Types.checkType;
import static java.util.Objects.requireNonNull;
/**
 * @author jelly.guodong.jin@gmail.com
 */
public class HDFSSplitManager
implements ConnectorSplitManager
{
    private final HDFSConnectorId connectorId;
    private final MetaServer metaServer;

    @Inject
    public HDFSSplitManager(
            HDFSConnectorId connectorId,
            MetaServer metaServer)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.metaServer = requireNonNull(metaServer, "metaServer is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle handle, ConnectorSession session, ConnectorTableLayoutHandle layoutHandle)
    {
        HDFSTableLayoutHandle layout = checkType(layoutHandle, HDFSTableLayoutHandle.class, "layoutHandle");
        Optional<HDFSTableHandle> tableHandle = metaServer.getTableHandle(layout.getTableName().getSchemaName(), layout.getTableName().getTableName());
        if (!tableHandle.isPresent()) {
            throw new TableNotFoundException(layout.getTableName().toString());
        }
        Path tablePath = tableHandle.get().getPath();

        List<ConnectorSplit> splits = new ArrayList<>();
        List<Path> files = FSFactory.listFiles(tablePath);
        files.forEach(file -> splits.add(new HDFSSplit(connectorId,
                        tableHandle.get().getSchemaTableName(),
                        tablePath, -1, -1,
                        FSFactory.getBlockLocations(file, 0, Long.MAX_VALUE))));
        return new FixedSplitSource(splits);
    }
}
