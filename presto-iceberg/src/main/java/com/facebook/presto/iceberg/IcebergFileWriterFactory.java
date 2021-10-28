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
package com.facebook.presto.iceberg;

import com.facebook.presto.common.io.DataSink;
import com.facebook.presto.common.io.OutputStreamDataSink;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveDwrfEncryptionProvider;
import com.facebook.presto.hive.HiveSessionProperties;
import com.facebook.presto.hive.NodeVersion;
import com.facebook.presto.hive.OrcFileWriterConfig;
import com.facebook.presto.hive.orc.HdfsOrcDataSource;
import com.facebook.presto.orc.DwrfEncryptionProvider;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcDataSourceId;
import com.facebook.presto.orc.OrcWriterStats;
import com.facebook.presto.parquet.writer.ParquetWriterOptions;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import javax.inject.Inject;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.facebook.presto.hive.HiveMetadata.PRESTO_VERSION_NAME;
import static com.facebook.presto.hive.HiveSessionProperties.getParquetWriterBlockSize;
import static com.facebook.presto.hive.HiveSessionProperties.getParquetWriterPageSize;
import static com.facebook.presto.hive.metastore.MetastoreUtil.PRESTO_QUERY_ID_NAME;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_WRITER_OPEN_ERROR;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_WRITE_VALIDATION_FAILED;
import static com.facebook.presto.iceberg.IcebergSessionProperties.getCompressionCodec;
import static com.facebook.presto.iceberg.IcebergSessionProperties.getOrcMaxBufferSize;
import static com.facebook.presto.iceberg.IcebergSessionProperties.getOrcMaxMergeDistance;
import static com.facebook.presto.iceberg.IcebergSessionProperties.getOrcOptimizedWriterValidateMode;
import static com.facebook.presto.iceberg.IcebergSessionProperties.getOrcStreamBufferSize;
import static com.facebook.presto.iceberg.IcebergSessionProperties.isOrcOptimizedWriterValidate;
import static com.facebook.presto.iceberg.TypeConverter.toOrcType;
import static com.facebook.presto.iceberg.TypeConverter.toPrestoType;
import static com.facebook.presto.iceberg.util.PrimitiveTypeMapBuilder.makeTypeMap;
import static com.facebook.presto.orc.OrcEncoding.ORC;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.parquet.ParquetSchemaUtil.convert;
import static org.joda.time.DateTimeZone.UTC;

public class IcebergFileWriterFactory
{
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;
    private final FileFormatDataSourceStats readStats;
    private final NodeVersion nodeVersion;
    private final OrcWriterStats orcWriterStats = new OrcWriterStats();
    private final OrcFileWriterConfig orcFileWriterConfig;
    private final DwrfEncryptionProvider dwrfEncryptionProvider;

    @Inject
    public IcebergFileWriterFactory(
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            FileFormatDataSourceStats readStats,
            NodeVersion nodeVersion,
            OrcFileWriterConfig orcFileWriterConfig,
            HiveDwrfEncryptionProvider dwrfEncryptionProvider)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.readStats = requireNonNull(readStats, "readStats is null");
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
        this.orcFileWriterConfig = requireNonNull(orcFileWriterConfig, "orcFileWriterConfig is null");
        this.dwrfEncryptionProvider = requireNonNull(dwrfEncryptionProvider, "DwrfEncryptionProvider is null").toDwrfEncryptionProvider();
    }

    public IcebergFileWriter createFileWriter(
            Path outputPath,
            Schema icebergSchema,
            JobConf jobConf,
            ConnectorSession session,
            HdfsContext hdfsContext,
            FileFormat fileFormat)
    {
        switch (fileFormat) {
            case PARQUET:
                return createParquetWriter(outputPath, icebergSchema, jobConf, session, hdfsContext);
            case ORC:
                return createOrcWriter(outputPath, icebergSchema, jobConf, session);
        }
        throw new PrestoException(NOT_SUPPORTED, "File format not supported for Iceberg: " + fileFormat);
    }

    private IcebergFileWriter createParquetWriter(
            Path outputPath,
            Schema icebergSchema,
            JobConf jobConf,
            ConnectorSession session,
            HdfsContext hdfsContext)
    {
        List<String> fileColumnNames = icebergSchema.columns().stream()
                .map(Types.NestedField::name)
                .collect(toImmutableList());
        List<Type> fileColumnTypes = icebergSchema.columns().stream()
                .map(column -> toPrestoType(column.type(), typeManager))
                .collect(toImmutableList());

        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(session.getUser(), outputPath, jobConf);

            Callable<Void> rollbackAction = () -> {
                fileSystem.delete(outputPath, false);
                return null;
            };

            ParquetWriterOptions parquetWriterOptions = ParquetWriterOptions.builder()
                    .setMaxPageSize(getParquetWriterPageSize(session))
                    .setMaxPageSize(getParquetWriterBlockSize(session))
                    .build();

            return new IcebergParquetFileWriter(
                    hdfsEnvironment.doAs(session.getUser(), () -> fileSystem.create(outputPath)),
                    rollbackAction,
                    fileColumnNames,
                    fileColumnTypes,
                    convert(icebergSchema, "table"),
                    makeTypeMap(fileColumnTypes, fileColumnNames),
                    parquetWriterOptions,
                    IntStream.range(0, fileColumnNames.size()).toArray(),
                    getCompressionCodec(session).getParquetCompressionCodec().get(),
                    outputPath,
                    hdfsEnvironment,
                    hdfsContext);
        }
        catch (IOException e) {
            throw new PrestoException(ICEBERG_WRITER_OPEN_ERROR, "Error creating Parquet file", e);
        }
    }

    private IcebergFileWriter createOrcWriter(
            Path outputPath,
            Schema icebergSchema,
            JobConf jobConf,
            ConnectorSession session)
    {
        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(session.getUser(), outputPath, jobConf);
            DataSink orcDataSink = hdfsEnvironment.doAs(session.getUser(), () -> new OutputStreamDataSink(fileSystem.create(outputPath)));
            Callable<Void> rollbackAction = () -> {
                hdfsEnvironment.doAs(session.getUser(), () -> fileSystem.delete(outputPath, false));
                return null;
            };

            List<Types.NestedField> columnFields = icebergSchema.columns();
            List<String> fileColumnNames = columnFields.stream()
                    .map(Types.NestedField::name)
                    .collect(toImmutableList());
            List<Type> fileColumnTypes = columnFields.stream()
                    .map(Types.NestedField::type)
                    .map(type -> toPrestoType(type, typeManager))
                    .collect(toImmutableList());

            Optional<Supplier<OrcDataSource>> validationInputFactory = Optional.empty();
            if (isOrcOptimizedWriterValidate(session)) {
                validationInputFactory = Optional.of(() -> {
                    try {
                        return new HdfsOrcDataSource(
                                new OrcDataSourceId(outputPath.toString()),
                                hdfsEnvironment.doAs(session.getUser(), () -> fileSystem.getFileStatus(outputPath).getLen()),
                                getOrcMaxMergeDistance(session),
                                getOrcMaxBufferSize(session),
                                getOrcStreamBufferSize(session),
                                false,
                                hdfsEnvironment.doAs(session.getUser(), () -> fileSystem.open(outputPath)),
                                readStats);
                    }
                    catch (IOException e) {
                        throw new PrestoException(ICEBERG_WRITE_VALIDATION_FAILED, e);
                    }
                });
            }

            return new IcebergOrcFileWriter(
                    icebergSchema,
                    orcDataSink,
                    rollbackAction,
                    ORC,
                    fileColumnNames,
                    fileColumnTypes,
                    toOrcType(icebergSchema),
                    getCompressionCodec(session).getOrcCompressionKind(),
                    orcFileWriterConfig
                            .toOrcWriterOptionsBuilder()
                            .withStripeMinSize(HiveSessionProperties.getOrcOptimizedWriterMinStripeSize(session))
                            .withStripeMaxSize(HiveSessionProperties.getOrcOptimizedWriterMaxStripeSize(session))
                            .withStripeMaxRowCount(HiveSessionProperties.getOrcOptimizedWriterMaxStripeRows(session))
                            .withDictionaryMaxMemory(HiveSessionProperties.getOrcOptimizedWriterMaxDictionaryMemory(session))
                            .withMaxStringStatisticsLimit(HiveSessionProperties.getOrcStringStatisticsLimit(session))
                            .build(),
                    IntStream.range(0, fileColumnNames.size()).toArray(),
                    ImmutableMap.<String, String>builder()
                            .put(PRESTO_VERSION_NAME, nodeVersion.toString())
                            .put(PRESTO_QUERY_ID_NAME, session.getQueryId())
                            .build(),
                    UTC,
                    validationInputFactory,
                    getOrcOptimizedWriterValidateMode(session),
                    orcWriterStats,
                    dwrfEncryptionProvider,
                    Optional.empty());
        }
        catch (IOException e) {
            throw new PrestoException(ICEBERG_WRITER_OPEN_ERROR, "Error creating ORC file", e);
        }
    }
}
