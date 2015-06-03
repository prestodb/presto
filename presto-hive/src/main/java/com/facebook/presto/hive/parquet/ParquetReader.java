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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import parquet.column.ColumnDescriptor;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.column.page.PageReadStore;
import parquet.io.ColumnIOFactory;
import parquet.io.MessageColumnIO;
import parquet.schema.MessageType;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class ParquetReader<T>
{
    private final ColumnIOFactory columnIOFactory = new ColumnIOFactory();

    private Path file;
    private MessageType fileSchema;
    private Map<String, String> extraMetadata;
    private List<BlockMetaData> blocks;
    private Configuration configuration;
    private int rowCount = 0;
    private int current = 0;

    // Parquet Reader iterates through each specified column schema
    private MessageType columnSchema;
    private ParquetFileReader fileReader;
    private ParquetColumnReader columnReader;

    public ParquetReader(MessageType fileSchema,
                        Map<String, String> extraMetadata,
                        Path file,
                        List<BlockMetaData> blocks,
                        Configuration configuration) throws IOException
    {
        this.fileSchema = fileSchema;
        this.file = file;
        this.extraMetadata = extraMetadata;
        this.blocks = blocks;
        this.configuration = configuration;
        for (BlockMetaData block : blocks) {
            rowCount += block.getRowCount();
        }
    }

    public void close()
        throws IOException
    {
        if (fileReader != null) {
            fileReader.close();
        }
    }

    public float getProgress()
        throws IOException, InterruptedException
    {
        if (rowCount == 0) {
            return 1.0f;
        }
        return (float) current / rowCount;
    }

    public int getBatchSize()
    {
        return this.rowCount;
    }

    public ParquetBatch nextBatch(ParquetBatch previous)
        throws IOException, InterruptedException
    {
        checkNotNull(previous, "ParquetBatch is null");
        if (current >= rowCount) {
            return null;
        }
        return previous;
    }

    public void readColumn(ColumnVector vector, MessageType columnSchema)
        throws IOException, InterruptedException
    {
        this.columnSchema = columnSchema;
        this.fileReader = new ParquetFileReader(configuration, file, blocks, columnSchema.getColumns());
        initializaReader();
        this.columnReader.readVector(vector);
        this.columnReader.loadPages();
        current += vector.size();
    }

    private void initializaReader()
        throws IOException
    {
        PageReadStore readerStore = fileReader.readNextRowGroup();
        if (readerStore == null) {
            throw new IOException("Error initializing reader store when reading: " + columnSchema.toString());
        }

        MessageColumnIO columnIO = columnIOFactory.getColumnIO(columnSchema, fileSchema);
        checkArgument(columnIO.getLeaves().size() == 1, "Read one Primitive Column at one time");
        ColumnDescriptor columnDescriptor = columnIO.getLeaves().get(0).getColumnDescriptor();
        this.columnReader = new ParquetColumnReader(columnDescriptor, readerStore.getPageReader(columnDescriptor));
    }
}
