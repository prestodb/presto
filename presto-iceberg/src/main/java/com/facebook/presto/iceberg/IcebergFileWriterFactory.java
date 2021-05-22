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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.parquet.writer.ParquetWriterOptions;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import javax.inject.Inject;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.IntStream;

import static com.facebook.presto.hive.HiveSessionProperties.getParquetWriterBlockSize;
import static com.facebook.presto.hive.HiveSessionProperties.getParquetWriterPageSize;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_WRITER_OPEN_ERROR;
import static com.facebook.presto.iceberg.IcebergSessionProperties.getCompressionCodec;
import static com.facebook.presto.iceberg.TypeConverter.toPrestoType;
import static com.facebook.presto.iceberg.util.PrimitiveTypeMapBuilder.makeTypeMap;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.parquet.ParquetSchemaUtil.convert;

public class IcebergFileWriterFactory
{
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;
    private final FileFormatDataSourceStats readStats;

    @Inject
    public IcebergFileWriterFactory(
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            FileFormatDataSourceStats readStats)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.readStats = requireNonNull(readStats, "readStats is null");
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
            // TODO: support ORC
            case PARQUET:
                return createParquetWriter(outputPath, icebergSchema, jobConf, session, hdfsContext);
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
}
