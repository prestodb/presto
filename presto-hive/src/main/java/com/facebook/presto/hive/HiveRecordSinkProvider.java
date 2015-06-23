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

import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.RecordSink;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import javax.inject.Inject;

import static com.facebook.presto.hive.util.Types.checkType;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.UUID.randomUUID;

public class HiveRecordSinkProvider
        implements ConnectorRecordSinkProvider
{
    private final HdfsEnvironment hdfsEnvironment;

    @Inject
    public HiveRecordSinkProvider(HdfsEnvironment hdfsEnvironment)
    {
        this.hdfsEnvironment = checkNotNull(hdfsEnvironment, "hdfsEnvironment is null");
    }

    @Override
    public RecordSink getRecordSink(ConnectorSession session, ConnectorOutputTableHandle tableHandle)
    {
        HiveOutputTableHandle handle = checkType(tableHandle, HiveOutputTableHandle.class, "tableHandle");

        Path target = new Path(handle.getTemporaryPath(), randomUUID().toString());
        JobConf conf = new JobConf(hdfsEnvironment.getConfiguration(target));

        return new HiveRecordSink(handle, target, conf);
    }

    @Override
    public RecordSink getRecordSink(ConnectorSession session, ConnectorInsertTableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }
}
