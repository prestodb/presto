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
package com.facebook.presto.raptor.storage;

import com.facebook.presto.orc.OrcDataSink;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcDataSourceId;
import com.facebook.presto.orc.OutputStreamOrcDataSink;
import com.facebook.presto.raptor.filesystem.RaptorHdfsConfiguration;
import com.facebook.presto.spi.PrestoException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;

import java.io.IOException;

import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_LOCAL_FILE_SYSTEM_ERROR;

public class HdfsOrcDataEnvironment
        implements OrcDataEnvironment
{
    private final FileSystem fileSystem;

    @Inject
    public HdfsOrcDataEnvironment(Path baseLocation, RaptorHdfsConfiguration configuration)
    {
        try {
            this.fileSystem = baseLocation.getFileSystem(configuration.getConfiguration());
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_LOCAL_FILE_SYSTEM_ERROR, "Raptor cannot create local file system", e);
        }
    }

    @Override
    public FileSystem getFileSystem()
    {
        return fileSystem;
    }

    @Override
    public OrcDataSource createOrcDataSource(Path path, ReaderAttributes readerAttributes)
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
    public OrcDataSink createOrcDataSink(Path path)
            throws IOException
    {
        return new OutputStreamOrcDataSink(fileSystem.create(path));
    }
}
