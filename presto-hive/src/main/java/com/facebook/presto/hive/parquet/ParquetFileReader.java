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

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import parquet.column.ColumnDescriptor;
import parquet.hadoop.metadata.ColumnPath;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.ColumnChunkMetaData;

import static com.google.common.base.Preconditions.checkArgument;

public class ParquetFileReader
    implements Closeable
{
    private final List<BlockMetaData> blocks;
    private final FSDataInputStream inputStream;
    private final Path file;
    private final Map<ColumnDescriptor, ColumnChunkMetaData> columnMetadata = new HashMap<ColumnDescriptor, ColumnChunkMetaData>();
    private final ParquetCodecFactory codecFactory;

    private int currentBlock = 0;
    private BlockMetaData currentBlockMetadata = null;

    public ParquetFileReader(Configuration configuration,
                            Path file,
                            List<BlockMetaData> blocks,
                            List<ColumnDescriptor> columns)
        throws IOException
    {
        this.file = file;
        this.inputStream = file.getFileSystem(configuration).open(file);
        this.blocks = blocks;
        if (!blocks.isEmpty()) {
            for (ColumnDescriptor columnDescriptor : columns) {
                for (ColumnChunkMetaData metadata : blocks.get(0).getColumns()) {
                    if (metadata.getPath().equals(ColumnPath.get(columnDescriptor.getPath()))) {
                        columnMetadata.put(columnDescriptor, metadata);
                    }
                }
            }
        }
        this.codecFactory = new ParquetCodecFactory(configuration);
    }

    public long readNextRowGroup()
    {
        if (this.currentBlock == this.blocks.size()) {
            return -1;
        }
        this.currentBlockMetadata = this.blocks.get(this.currentBlock);
        this.currentBlock = this.currentBlock + 1;
        return this.currentBlockMetadata.getRowCount();
    }

    public ParquetColumnChunkPageReader readColumn(ColumnDescriptor columnDescriptor)
        throws IOException
    {
        checkArgument(this.currentBlockMetadata.getRowCount() > 0, "Row group having 0 rows");

        ColumnChunkMetaData metadata = columnMetadata.get(columnDescriptor);
        long startingPosition = metadata.getStartingPos();
        this.inputStream.seek(startingPosition);
        int totalSize = (int) metadata.getTotalSize();
        byte[] buffer = new byte[totalSize];
        inputStream.readFully(buffer);
        ParquetColumnChunkDescriptor descriptor = new ParquetColumnChunkDescriptor(columnDescriptor,
                                                                                    metadata,
                                                                                    startingPosition,
                                                                                    totalSize);
        ParquetColumnChunk columnChunk = new ParquetColumnChunk(descriptor,
                                                                buffer,
                                                                0,
                                                                this.codecFactory);
        return columnChunk.readAllPages();
    }

    @Override
    public void close()
        throws IOException
    {
        inputStream.close();
        this.codecFactory.release();
    }
}
