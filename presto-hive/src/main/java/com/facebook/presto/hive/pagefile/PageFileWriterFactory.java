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
package com.facebook.presto.hive.pagefile;

import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveFileWriter;
import com.facebook.presto.hive.HiveFileWriterFactory;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.orc.DataSink;
import com.facebook.presto.orc.OutputStreamDataSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.page.PagesSerde;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import javax.inject.Inject;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_WRITER_OPEN_ERROR;
import static com.facebook.presto.hive.HiveSessionProperties.getPageFileStripeMaxSize;
import static com.facebook.presto.hive.HiveStorageFormat.PAGEFILE;
import static java.util.Objects.requireNonNull;

public class PageFileWriterFactory
        implements HiveFileWriterFactory
{
    private final HdfsEnvironment hdfsEnvironment;
    private final PagesSerde pagesSerde;

    @Inject
    public PageFileWriterFactory(
            HdfsEnvironment hdfsEnvironment,
            BlockEncodingSerde blockEncodingSerde)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");

        pagesSerde = new PagesSerde(
                requireNonNull(blockEncodingSerde, "blockEncodingSerde is null"),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    @Override
    public Optional<HiveFileWriter> createFileWriter(
            Path path,
            List<String> inputColumnNames,
            StorageFormat storageFormat,
            Properties schema,
            JobConf configuration,
            ConnectorSession session)
    {
        if (!storageFormat.getOutputFormat().equals(PAGEFILE.getOutputFormat())) {
            return Optional.empty();
        }

        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(session.getUser(), path, configuration);
            DataSink dataSink = createPageDataSink(fileSystem, path);

            Callable<Void> rollbackAction = () -> {
                fileSystem.delete(path, false);
                return null;
            };
            return Optional.of(new PageFileWriter(dataSink, pagesSerde, getPageFileStripeMaxSize(session), rollbackAction));
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_WRITER_OPEN_ERROR, "Error creating pagefile", e);
        }
    }

    protected DataSink createPageDataSink(FileSystem fileSystem, Path path)
            throws IOException
    {
        return new OutputStreamDataSink(fileSystem.create(path));
    }
}
