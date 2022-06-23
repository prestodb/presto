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

package com.facebook.presto.parquet;

import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.util.Optional;

public class FileParquetDataSource
        extends AbstractParquetDataSource
{
    private final RandomAccessFile input;

    public FileParquetDataSource(File file)
            throws FileNotFoundException
    {
        super(new ParquetDataSourceId(file.getAbsolutePath()));
        input = new RandomAccessFile(file, "r");
    }

    @Override
    public void close()
            throws IOException
    {
        input.close();
    }

    @Override
    protected void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
    {
        try {
            input.seek(position);
            input.readFully(buffer, bufferOffset, bufferLength);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Optional<ColumnIndex> readColumnIndex(ColumnChunkMetaData column) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<OffsetIndex> readOffsetIndex(ColumnChunkMetaData column) throws IOException
    {
        throw new UnsupportedOperationException();
    }
}
