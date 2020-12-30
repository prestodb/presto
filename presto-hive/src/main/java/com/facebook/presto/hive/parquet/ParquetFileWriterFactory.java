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
package com.facebook.presto.hive.parquet;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.EncryptionInformation;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveFileWriter;
import com.facebook.presto.hive.HiveFileWriterFactory;
import com.facebook.presto.hive.NodeVersion;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.parquet.writer.ParquetWriterOptions;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.google.common.base.Splitter;
import com.google.inject.Inject;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_WRITER_OPEN_ERROR;
import static com.facebook.presto.hive.HiveSessionProperties.getParquetWriterBlockSize;
import static com.facebook.presto.hive.HiveSessionProperties.getParquetWriterPageSize;
import static com.facebook.presto.hive.HiveSessionProperties.isParquetOptimizedWriterEnabled;
import static com.facebook.presto.hive.HiveType.toHiveTypes;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMNS;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMN_TYPES;

public class ParquetFileWriterFactory
        implements HiveFileWriterFactory
{
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;

    @Inject
    public ParquetFileWriterFactory(
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            NodeVersion nodeVersion,
            HiveClientConfig hiveConfig)
    {
        this(
                hdfsEnvironment,
                typeManager,
                nodeVersion,
                requireNonNull(hiveConfig, "hiveConfig is null").getDateTimeZone());
    }

    public ParquetFileWriterFactory(
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            NodeVersion nodeVersion,
            DateTimeZone hiveStorageTimeZone)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public Optional<HiveFileWriter> createFileWriter(
            Path path,
            List<String> inputColumnNames,
            StorageFormat storageFormat,
            Properties schema,
            JobConf conf,
            ConnectorSession session,
            Optional<EncryptionInformation> encryptionInformation)
    {
        if (!isParquetOptimizedWriterEnabled(session)) {
            return Optional.empty();
        }

        if (!MapredParquetOutputFormat.class.getName().equals(storageFormat.getOutputFormat())) {
            return Optional.empty();
        }

        ParquetWriterOptions parquetWriterOptions = ParquetWriterOptions.builder()
                .setMaxPageSize(getParquetWriterPageSize(session))
                .setMaxBlockSize(getParquetWriterBlockSize(session))
                .build();

        CompressionCodecName compressionCodecName = getCompression(conf);

        List<String> fileColumnNames = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(schema.getProperty(META_TABLE_COLUMNS, ""));
        List<Type> fileColumnTypes = toHiveTypes(schema.getProperty(META_TABLE_COLUMN_TYPES, "")).stream()
                .map(hiveType -> hiveType.getType(typeManager))
                .collect(toList());

        int[] fileInputColumnIndexes = fileColumnNames.stream()
                .mapToInt(inputColumnNames::indexOf)
                .toArray();

        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(session.getUser(), path, conf);

            Callable<Void> rollbackAction = () -> {
                fileSystem.delete(path, false);
                return null;
            };

            return Optional.of(new ParquetFileWriter(
                    fileSystem.create(path),
                    rollbackAction,
                    fileColumnNames,
                    fileColumnTypes,
                    parquetWriterOptions,
                    fileInputColumnIndexes,
                    compressionCodecName));
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_WRITER_OPEN_ERROR, "Error creating Parquet file", e);
        }
    }

    private static CompressionCodecName getCompression(JobConf configuration)
    {
        String compressionName = configuration.get(ParquetOutputFormat.COMPRESSION);
        if (compressionName == null) {
            return CompressionCodecName.GZIP;
        }
        return CompressionCodecName.valueOf(compressionName);
    }
}
