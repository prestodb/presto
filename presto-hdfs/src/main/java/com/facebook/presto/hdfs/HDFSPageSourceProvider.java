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
package com.facebook.presto.hdfs;

import com.facebook.presto.hdfs.exception.HdfsSplitNotOpenException;
import com.facebook.presto.hdfs.fs.FSFactory;
import com.facebook.presto.hive.parquet.HdfsParquetDataSource;
import com.facebook.presto.hive.parquet.ParquetDataSource;
import com.facebook.presto.hive.parquet.reader.ParquetMetadataReader;
import com.facebook.presto.hive.parquet.reader.ParquetReader;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.type.TypeManager;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.FileMetaData;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.MessageType;
import parquet.schema.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.hdfs.Types.checkType;
import static java.util.Objects.requireNonNull;

/**
 * @author jelly.guodong.jin@gmail.com
 */
public class HDFSPageSourceProvider
implements ConnectorPageSourceProvider
{
    private final TypeManager typeManager;
    private final FSFactory fsFactory;

    @Inject
    public HDFSPageSourceProvider(TypeManager typeManager, FSFactory fsFactory)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.fsFactory = requireNonNull(fsFactory, "fsFactory is null");
    }

    private Logger log = Logger.get(HDFSPageSourceProvider.class.getName());

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session,
                                                ConnectorSplit split, List<ColumnHandle> columns)
    {
        List<HDFSColumnHandle> hdfsColumns = columns.stream()
                .map(col -> (HDFSColumnHandle) col)
                .collect(Collectors.toList());
        HDFSSplit hdfsSplit = checkType(split, HDFSSplit.class, "hdfs split");
        Path path = new Path(hdfsSplit.getPath());

        Optional<ConnectorPageSource> pageSource = createHDFSPageSource(
                path,
                hdfsSplit.getStart(),
                hdfsSplit.getLen(),
                hdfsColumns);
        if (pageSource.isPresent()) {
            return pageSource.get();
        }
        throw new RuntimeException("Could not find a file reader for split " + hdfsSplit);
    }

    private Optional<ConnectorPageSource> createHDFSPageSource(
            Path path,
            long start,
            long length,
            List<HDFSColumnHandle> columns)
    {
        Optional<FileSystem> fileSystemOptional = fsFactory.getFS(path);
        FileSystem fileSystem;
        ParquetDataSource dataSource;
        if (fileSystemOptional.isPresent()) {
            fileSystem = fileSystemOptional.get();
        }
        else {
            throw new RuntimeException("Could not find filesystem for path " + path);
        }
        try {
            dataSource = buildHdfsParquetDataSource(fileSystem, path, start, length);
            // default length is file size, which means whole file is a split
            length = dataSource.getSize();
            ParquetMetadata parquetMetadata = ParquetMetadataReader.readFooter(fileSystem, path);
            FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
            MessageType fileSchema = fileMetaData.getSchema();

            List<Type> fields = columns.stream()
                    .filter(column -> column.getColType() != HDFSColumnHandle.ColumnType.NOTVALID)
                    .map(column -> getParquetType(column, fileSchema))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
            MessageType requestedSchema = new MessageType(fileSchema.getName(), fields);

            List<BlockMetaData> blocks = new ArrayList<>();
            for (BlockMetaData block : parquetMetadata.getBlocks()) {
                long firstDataPage = block.getColumns().get(0).getFirstDataPageOffset();
                if (firstDataPage >= start && firstDataPage < start + length) {
                    blocks.add(block);
                }
            }

            ParquetReader parquetReader = new ParquetReader(
                    fileSchema,
                    requestedSchema,
                    blocks,
                    dataSource,
                    typeManager);
            return Optional.of(new HDFSPageSource(
                    parquetReader,
                    dataSource,
                    fileSchema,
                    requestedSchema,
                    length,
                    columns,
                    typeManager));
        }
        catch (IOException e) {
            log.error(e);
            return Optional.empty();
        }
    }

    private HdfsParquetDataSource buildHdfsParquetDataSource(FileSystem fileSystem, Path path, long start, long length)
    {
        try {
            long size = fileSystem.getFileStatus(path).getLen();
            FSDataInputStream inputStream = fileSystem.open(path);
            return new HdfsParquetDataSource(path, size, inputStream);
        }
        catch (IOException e) {
            throw new HdfsSplitNotOpenException(path);
        }
    }

    private Type getParquetType(HDFSColumnHandle column, MessageType messageType)
    {
        if (messageType.containsField(column.getName())) {
            return messageType.getType(column.getName());
        }
        // parquet is case-insensitive, all hdfs-columns get converted to lowercase
        for (Type type : messageType.getFields()) {
            if (type.getName().equalsIgnoreCase(column.getName())) {
                return type;
            }
        }
        return null;
    }
}
