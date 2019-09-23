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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PageSorter;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.List;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_WRITER_OPEN_ERROR;
import static java.util.Objects.requireNonNull;

public class HiveSortingFileWriterFactory
        implements SortingFileWriterFactory
{
    private final HdfsEnvironment hdfsEnvironment;
    private final ConnectorSession session;
    private final JobConf conf;
    private final List<Type> types;
    private final List<Integer> sortFields;
    private final List<SortOrder> sortOrders;
    private final DataSize sortBufferSize;
    private final int maxOpenSortFiles;
    private final PageSorter pageSorter;
    private final OrcFileWriterFactory orcFileWriterFactory;

    public HiveSortingFileWriterFactory(
            HdfsEnvironment hdfsEnvironment,
            ConnectorSession session,
            JobConf conf,
            List<Type> types,
            List<Integer> sortFields,
            List<SortOrder> sortOrders,
            DataSize sortBufferSize,
            int maxOpenSortFiles,
            PageSorter pageSorter,
            OrcFileWriterFactory orcFileWriterFactory)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.session = requireNonNull(session, "session is null");
        this.conf = requireNonNull(conf, "conf is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.sortFields = ImmutableList.copyOf(requireNonNull(sortFields, "sortFields is null"));
        this.sortOrders = ImmutableList.copyOf(requireNonNull(sortOrders, "sortOrders is null"));
        this.sortBufferSize = requireNonNull(sortBufferSize, "sortBufferSize is null");
        this.maxOpenSortFiles = maxOpenSortFiles;
        this.pageSorter = requireNonNull(pageSorter, "pageSorter is null");
        this.orcFileWriterFactory = requireNonNull(orcFileWriterFactory, "orcFileWriterFactory is null");
    }

    @Override
    public SortingFileWriter createSortingFileWriter(Path path, HiveFileWriter outputWriter, int bucketNumber)
    {
        FileSystem fileSystem;
        try {
            fileSystem = hdfsEnvironment.getFileSystem(session.getUser(), path, conf);
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_WRITER_OPEN_ERROR, e);
        }

        return new SortingFileWriter(
                fileSystem,
                new Path(path.getParent(), ".tmp-sort." + path.getName()),
                outputWriter,
                sortBufferSize,
                maxOpenSortFiles,
                types,
                sortFields,
                sortOrders,
                pageSorter,
                (fs, p) -> orcFileWriterFactory.createOrcDataSink(session, fs, p));
    }
}
