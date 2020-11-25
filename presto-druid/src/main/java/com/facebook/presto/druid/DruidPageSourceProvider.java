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

import com.facebook.presto.common.Page;
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
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.column.StringDictionaryEncodedColumn;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.druid.DruidErrorCode.DRUID_DEEP_STORAGE_ERROR;
import static com.facebook.presto.druid.DruidErrorCode.DRUID_SEGMENT_LOAD_ERROR;
import static com.facebook.presto.druid.DruidSplit.SplitType.BROKER;
import static java.util.Objects.requireNonNull;

public class DruidPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final DruidClient druidClient;
    private final Configuration hadoopConfiguration;

    @Inject
    public DruidPageSourceProvider(DruidClient druidClient, DruidConfig config)
    {
        this.druidClient = requireNonNull(druidClient, "druid client is null");
        this.hadoopConfiguration = config.readHadoopConfiguration();
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
        Map<String, List<Object>> dimFilters = druidSplit.getDimFilters();
        try {
            Path segmentPath = new Path(segmentInfo.getDeepStoragePath());
            FileSystem fileSystem = segmentPath.getFileSystem(hadoopConfiguration);
            long fileSize = fileSystem.getFileStatus(segmentPath).getLen();
            FSDataInputStream inputStream = fileSystem.open(segmentPath);
            DataInputSourceId dataInputSourceId = new DataInputSourceId(segmentPath.toString());
            HdfsDataInputSource dataInputSource = new HdfsDataInputSource(dataInputSourceId, inputStream, fileSize);
            IndexFileSource indexFileSource = new ZipIndexFileSource(dataInputSource);
            SegmentColumnSource segmentColumnSource = new SmooshedColumnSource(indexFileSource);
            SegmentIndexSource segmentIndexSource = new V9SegmentIndexSource(segmentColumnSource);

            try {
                QueryableIndex queryableIndex = segmentIndexSource.loadIndex(columns);
                if (dimFilters != null && !dimFilters.isEmpty()) {
                    for (Map.Entry<String, List<Object>> entry : dimFilters.entrySet()) {
                        String columnName = entry.getKey();
                        BaseColumn baseColumn = queryableIndex.getColumnHolder(columnName).getColumn();
                        if (baseColumn instanceof StringDictionaryEncodedColumn) {
                            StringDictionaryEncodedColumn sdec = (StringDictionaryEncodedColumn) baseColumn;
                            boolean hit = false;
                            for (Object v : entry.getValue()) {
                                if (sdec.lookupId(v.toString()) >= 0) {
                                    hit = true;
                                    break;
                                }
                            }
                            if (!hit) {
                                return emptyConnectorPageSource();
                            }
                        }
                    }
                }
                return new DruidSegmentPageSource(
                        dataInputSource,
                        columns,
                        new DruidSegmentReader(queryableIndex, columns));
            }
            catch (IOException e) {
                throw new PrestoException(DRUID_SEGMENT_LOAD_ERROR, "failed to load druid segment: " + e.getMessage());
            }
        }
        catch (IOException e) {
            throw new PrestoException(DRUID_DEEP_STORAGE_ERROR, "Failed to create page source on " + segmentInfo.getDeepStoragePath(), e);
        }
    }

    private ConnectorPageSource emptyConnectorPageSource()
    {
        return new ConnectorPageSource() {
            @Override
            public long getCompletedBytes()
            {
                return 0;
            }
            @Override
            public long getCompletedPositions()
            {
                return 0;
            }
            @Override
            public long getReadTimeNanos()
            {
                return 0;
            }
            @Override
            public boolean isFinished()
            {
                return true;
            }
            @Override
            public Page getNextPage()
            {
                return null;
            }
            @Override
            public long getSystemMemoryUsage()
            {
                return 0;
            }
            @Override
            public void close() {}
        };
    }
}
