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
package com.facebook.presto.raptor.filesystem;

import com.facebook.presto.common.io.DataSink;
import com.facebook.presto.common.io.OutputStreamDataSink;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcDataSourceId;
import com.facebook.presto.raptor.storage.OrcDataEnvironment;
import com.facebook.presto.raptor.storage.ReaderAttributes;
import com.facebook.presto.spi.PrestoException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;

import java.io.IOException;

import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_FILE_SYSTEM_ERROR;
import static java.util.Objects.requireNonNull;

public class HdfsOrcDataEnvironment
        implements OrcDataEnvironment
{
    private final Path baseLocation;
    private final RaptorHdfsConfiguration configuration;

    @Inject
    public HdfsOrcDataEnvironment(Path baseLocation, RaptorHdfsConfiguration configuration)
    {
        this.baseLocation = requireNonNull(baseLocation, "baseLocation is null");
        this.configuration = requireNonNull(configuration, "configuration is null");
    }

    @Override
    public FileSystem getFileSystem(HdfsContext hdfsContext)
    {
        try {
            return baseLocation.getFileSystem(configuration.getConfiguration(hdfsContext, baseLocation.toUri()));
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_FILE_SYSTEM_ERROR, "Raptor cannot create HDFS file system", e);
        }
    }

    @Override
    public OrcDataSource createOrcDataSource(FileSystem fileSystem, Path path, ReaderAttributes readerAttributes)
            throws IOException
    {
        return new HdfsOrcDataSource(
                new OrcDataSourceId(path.toString()),
                fileSystem.getFileStatus(path).getLen(),
                readerAttributes.getMaxMergeDistance(),
                readerAttributes.getMaxReadSize(),
                readerAttributes.getStreamBufferSize(),
                readerAttributes.isLazyReadSmallRanges(),
                fileSystem.open(path));
    }

    @Override
    public DataSink createOrcDataSink(FileSystem fileSystem, Path path)
            throws IOException
    {
        return new OutputStreamDataSink(fileSystem.create(path));
    }
}
