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
package com.facebook.presto.hive.parquet.reader;

import com.facebook.presto.hive.parquet.ParquetCodecFactory;
import com.facebook.presto.hive.parquet.ParquetCorruptionException;
import com.google.common.primitives.Ints;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import parquet.column.ColumnDescriptor;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.hadoop.metadata.ColumnPath;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class ParquetFileReader
        implements Closeable
{
    private final List<BlockMetaData> blocks;
    private final FSDataInputStream inputStream;
    private final Path file;
    private final ParquetCodecFactory codecFactory;

    private int currentBlock;
    private BlockMetaData currentBlockMetadata;

    public ParquetFileReader(
            Configuration configuration,
            Path file,
            List<BlockMetaData> blocks,
            List<ColumnDescriptor> columns)
            throws IOException
    {
        this.file = file;
        this.inputStream = file.getFileSystem(configuration).open(file);
        this.blocks = blocks;
        this.codecFactory = new ParquetCodecFactory(configuration);
    }

    public long readNextRowGroup()
    {
        if (currentBlock == blocks.size()) {
            return -1;
        }
        currentBlockMetadata = blocks.get(currentBlock);
        currentBlock = currentBlock + 1;
        return currentBlockMetadata.getRowCount();
    }

    public ParquetColumnChunkPageReader readColumn(ColumnDescriptor columnDescriptor)
            throws IOException
    {
        checkArgument(currentBlockMetadata.getRowCount() > 0, "Row group having 0 rows");

        ColumnChunkMetaData metadata = getColumnChunkMetaData(columnDescriptor);
        long startingPosition = metadata.getStartingPos();
        inputStream.seek(startingPosition);
        int totalSize = Ints.checkedCast(metadata.getTotalSize());
        byte[] buffer = new byte[totalSize];
        inputStream.readFully(buffer);
        ParquetColumnChunkDescriptor descriptor = new ParquetColumnChunkDescriptor(columnDescriptor, metadata, startingPosition, totalSize);
        ParquetColumnChunk columnChunk = new ParquetColumnChunk(descriptor, buffer, 0, codecFactory);
        return columnChunk.readAllPages();
    }

    private ColumnChunkMetaData getColumnChunkMetaData(ColumnDescriptor columnDescriptor)
            throws IOException
    {
        for (ColumnChunkMetaData metadata : currentBlockMetadata.getColumns()) {
            if (metadata.getPath().equals(ColumnPath.get(columnDescriptor.getPath()))) {
                return metadata;
            }
        }
        throw new ParquetCorruptionException("Malformed Parquet file. Could not find column metadata %s", columnDescriptor.toString());
    }

    @Override
    public void close()
            throws IOException
    {
        inputStream.close();
        codecFactory.release();
    }
}
