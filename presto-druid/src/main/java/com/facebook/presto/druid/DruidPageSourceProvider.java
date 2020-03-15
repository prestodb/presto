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
package com.facebook.presto.druid;

import com.facebook.presto.druid.metadata.DruidSegmentInfo;
import com.facebook.presto.druid.segment.DruidSegmentReader;
import com.facebook.presto.druid.segment.HdfsDataInputSource;
import com.facebook.presto.druid.segment.IndexFileSource;
import com.facebook.presto.druid.segment.SegmentColumnSource;
import com.facebook.presto.druid.segment.SegmentIndexSource;
import com.facebook.presto.druid.segment.SmooshedColumnSource;
import com.facebook.presto.druid.segment.V9SegmentIndexSource;
import com.facebook.presto.druid.segment.ZipIndexFileSource;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SplitContext;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;

import java.io.IOException;
import java.util.List;

import static com.facebook.presto.druid.DruidErrorCode.DRUID_DEEP_STORAGE_ERROR;
import static com.facebook.presto.druid.DruidSplit.SplitType.BROKER;
import static java.util.Objects.requireNonNull;

public class DruidPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final DruidClient druidClient;

    @Inject
    public DruidPageSourceProvider(DruidClient druidClient)
    {
        this.druidClient = requireNonNull(druidClient, "druid client is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            List<ColumnHandle> columns,
            SplitContext splitContext)
    {
        DruidSplit druidSplit = (DruidSplit) split;
        if (druidSplit.getSplitType() == BROKER) {
            return new DruidBrokerPageSource(
                    druidSplit.getBrokerDql().get(),
                    columns,
                    druidClient);
        }
        DruidSegmentInfo segmentInfo = druidSplit.getSegmentInfo().get();
        try {
            Path hdfsPath = new Path(segmentInfo.getDeepStoragePath());
            FileSystem fileSystem = hdfsPath.getFileSystem(new Configuration());
            long fileSize = fileSystem.getFileStatus(hdfsPath).getLen();
            FSDataInputStream inputStream = fileSystem.open(hdfsPath);
            DataInputSourceId dataInputSourceId = new DataInputSourceId(hdfsPath.toString());
            HdfsDataInputSource dataInputSource = new HdfsDataInputSource(dataInputSourceId, inputStream, fileSize);
            IndexFileSource indexFileSource = new ZipIndexFileSource(dataInputSource);
            SegmentColumnSource segmentColumnSource = new SmooshedColumnSource(indexFileSource);
            SegmentIndexSource segmentIndexSource = new V9SegmentIndexSource(segmentColumnSource);

            return new DruidSegmentPageSource(
                    dataInputSource,
                    columns,
                    new DruidSegmentReader(segmentIndexSource, columns));
        }
        catch (IOException e) {
            throw new PrestoException(DRUID_DEEP_STORAGE_ERROR, e);
        }
    }
}
