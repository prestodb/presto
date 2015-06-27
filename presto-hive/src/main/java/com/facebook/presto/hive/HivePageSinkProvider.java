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

import com.facebook.presto.hive.metastore.HiveMetastore;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorPageSinkProvider;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.TypeManager;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;

import static com.facebook.presto.hive.util.Types.checkType;
import static java.util.Objects.requireNonNull;

public class HivePageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;

    @Inject
    public HivePageSinkProvider(HdfsEnvironment hdfsEnvironment,
            HiveMetastore metastore,
            TypeManager typeManager)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorSession session, ConnectorOutputTableHandle tableHandle)
    {
        HiveOutputTableHandle handle = checkType(tableHandle, HiveOutputTableHandle.class, "tableHandle");
        return new HivePageSink(
                handle.getSchemaName(),
                handle.getTableName(),
                handle.getInputColumns(),
                handle.getHiveStorageFormat(),
                new Path(handle.getWritePath()),
                handle.getFilePrefix(),
                typeManager,
                hdfsEnvironment);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorSession session, ConnectorInsertTableHandle insertTableHandle)
    {
        throw new UnsupportedOperationException();
    }
}
