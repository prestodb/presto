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

import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.io.DataSink;
import com.facebook.presto.hive.EncryptionInformation;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveCompressionCodec;
import com.facebook.presto.hive.HiveFileWriter;
import com.facebook.presto.hive.HiveFileWriterFactory;
import com.facebook.presto.hive.datasink.DataSinkFactory;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.orc.zlib.DeflateCompressor;
import com.facebook.presto.orc.zlib.InflateDecompressor;
import com.facebook.presto.orc.zstd.ZstdJniCompressor;
import com.facebook.presto.orc.zstd.ZstdJniDecompressor;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.page.PageCompressor;
import com.facebook.presto.spi.page.PageDecompressor;
import com.facebook.presto.spi.page.PagesSerde;
import com.google.common.collect.ImmutableList;
import io.airlift.compress.lz4.Lz4Compressor;
import io.airlift.compress.lz4.Lz4Decompressor;
import io.airlift.compress.snappy.SnappyCompressor;
import io.airlift.compress.snappy.SnappyDecompressor;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import javax.inject.Inject;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.concurrent.Callable;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_WRITER_OPEN_ERROR;
import static com.facebook.presto.hive.HiveSessionProperties.getPageFileStripeMaxSize;
import static com.facebook.presto.hive.HiveStorageFormat.PAGEFILE;
import static com.facebook.presto.hive.pagefile.PageFileFooterOutput.createEmptyPageFileFooterOutput;
import static com.facebook.presto.hive.util.ConfigurationUtils.PAGE_FILE_COMPRESSION;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PageFileWriterFactory
        implements HiveFileWriterFactory
{
    private final HdfsEnvironment hdfsEnvironment;
    private final DataSinkFactory dataSinkFactory;
    private final BlockEncodingSerde blockEncodingSerde;

    @Inject
    public PageFileWriterFactory(
            HdfsEnvironment hdfsEnvironment,
            DataSinkFactory dataSinkFactory,
            BlockEncodingSerde blockEncodingSerde)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.dataSinkFactory = requireNonNull(dataSinkFactory, "dataSinkFactory is null");
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
    }

    @Override
    public Optional<HiveFileWriter> createFileWriter(
            Path path,
            List<String> inputColumnNames,
            StorageFormat storageFormat,
            Properties schema,
            JobConf configuration,
            ConnectorSession session,
            Optional<EncryptionInformation> encryptionInformation)
    {
        if (!storageFormat.getOutputFormat().equals(PAGEFILE.getOutputFormat())) {
            return Optional.empty();
        }

        HiveCompressionCodec compression = HiveCompressionCodec.valueOf(configuration.get(PAGE_FILE_COMPRESSION));
        if (!compression.isSupportedStorageFormat(PAGEFILE)) {
            throw new PrestoException(
                    GENERIC_USER_ERROR,
                    format("%s compression is not supported for %s", compression.name(), PAGEFILE.getOutputFormat()));
        }
        PagesSerde pagesSerde = createPagesSerdeForPageFile(blockEncodingSerde, Optional.of(compression));

        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(session.getUser(), path, configuration);
            DataSink dataSink = dataSinkFactory.createDataSink(session, fileSystem, path);

            Callable<Void> rollbackAction = () -> {
                fileSystem.delete(path, false);
                return null;
            };
            return Optional.of(new PageFileWriter(dataSink, pagesSerde, compression, getPageFileStripeMaxSize(session), rollbackAction));
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_WRITER_OPEN_ERROR, "Error creating pagefile", e);
        }
    }

    public static void createEmptyPageFile(
            DataSinkFactory dataSinkFactory,
            ConnectorSession session,
            FileSystem fileSystem,
            Path path)
    {
        try {
            DataSink dataSink = dataSinkFactory.createDataSink(session, fileSystem, path);
            dataSink.write(ImmutableList.of(createEmptyPageFileFooterOutput()));
            dataSink.close();
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_WRITER_OPEN_ERROR, "Error creating empty pagefile", e);
        }
    }

    public static PagesSerde createPagesSerdeForPageFile(BlockEncodingSerde blockEncodingSerde, Optional<HiveCompressionCodec> compressionCodec)
    {
        if (!compressionCodec.isPresent()) {
            return new PagesSerde(blockEncodingSerde, Optional.empty(), Optional.empty(), Optional.empty());
        }

        PageCompressor pageCompressor = null;
        PageDecompressor pageDecompressor = null;

        switch (compressionCodec.get()) {
            case NONE:
                break;
            case SNAPPY:
                pageCompressor = new AirliftCompressorAdapter(new SnappyCompressor());
                pageDecompressor = new AirliftDecompressorAdapter(new SnappyDecompressor());
                break;
            case LZ4:
                pageCompressor = new AirliftCompressorAdapter(new Lz4Compressor());
                pageDecompressor = new AirliftDecompressorAdapter(new Lz4Decompressor());
                break;
            case GZIP:
                pageCompressor = new AirliftCompressorAdapter(new DeflateCompressor(OptionalInt.empty()));
                pageDecompressor = new AirliftDecompressorAdapter(new InflateDecompressor());
                break;
            case ZSTD:
                pageCompressor = new AirliftCompressorAdapter(new ZstdJniCompressor(OptionalInt.empty()));
                pageDecompressor = new AirliftDecompressorAdapter(new ZstdJniDecompressor());
                break;
            default:
                throw new PrestoException(
                        GENERIC_INTERNAL_ERROR,
                        format("%s compression is not supported for %s", compressionCodec.get().name(), PAGEFILE.getOutputFormat()));
        }

        return new PagesSerde(blockEncodingSerde, Optional.ofNullable(pageCompressor), Optional.ofNullable(pageDecompressor), Optional.empty());
    }
}
