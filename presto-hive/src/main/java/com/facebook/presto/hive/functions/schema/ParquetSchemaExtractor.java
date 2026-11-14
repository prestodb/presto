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
package com.facebook.presto.hive.functions.schema;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.parquet.HdfsParquetDataSource;
import com.facebook.presto.parquet.ParquetDataSource;
import com.facebook.presto.parquet.ParquetDataSourceId;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_BAD_DATA;
import static com.facebook.presto.parquet.ParquetToPrestoTypeConverter.fromParquet;
import static com.facebook.presto.parquet.cache.MetadataReader.readFooter;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

public class ParquetSchemaExtractor
        implements FileSchemaExtractor
{
    private final TypeManager typeManager;
    private final FileFormatDataSourceStats stats;

    public ParquetSchemaExtractor(TypeManager typeManager, FileFormatDataSourceStats stats)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.stats = requireNonNull(stats, "stats is null");
    }

    @Override
    public List<ColumnMetadata> extract(Path file, HdfsEnvironment hdfs, ConnectorSession session)
            throws IOException
    {
        requireNonNull(file, "file is null");
        requireNonNull(hdfs, "hdfs is null");
        requireNonNull(session, "session is null");

        HdfsContext context = new HdfsContext(session);
        FileSystem fileSystem = hdfs.getFileSystem(context, file);
        long fileSize = fileSystem.getFileStatus(file).getLen();
        try (FSDataInputStream inputStream = fileSystem.open(file);
                ParquetDataSource dataSource = new HdfsParquetDataSource(new ParquetDataSourceId(file.toString()), inputStream, stats)) {
            return extractFromDataSource(dataSource, file, fileSize, typeManager);
        }
    }

    static List<ColumnMetadata> extractFromDataSource(ParquetDataSource dataSource, Path file, long fileSize, TypeManager typeManager)
    {
        MessageType schema;
        try {
            ParquetMetadata metadata = readFooter(dataSource, fileSize, Optional.empty(), false).getParquetMetadata();
            schema = metadata.getFileMetaData().getSchema();
        }
        catch (IOException | RuntimeException e) {
            throw new PrestoException(HIVE_BAD_DATA, format("Failed to read Parquet footer for %s", file), e);
        }

        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        for (org.apache.parquet.schema.Type field : schema.getFields()) {
            Type prestoType;
            try {
                prestoType = fromParquet(field, typeManager);
            }
            catch (IllegalArgumentException e) {
                throw new PrestoException(HIVE_BAD_DATA, format("Unsupported Parquet column %s in %s: %s", field.getName(), file, e.getMessage()), e);
            }
            columns.add(ColumnMetadata.builder()
                    .setName(field.getName())
                    .setType(prestoType)
                    .setNullable(field.getRepetition() != REQUIRED)
                    .build());
        }
        return columns.build();
    }
}
