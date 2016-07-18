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
package com.facebook.presto.raptor.storage;

import com.facebook.presto.raptor.util.Closer;
import com.facebook.presto.raptor.util.SyncingFileSystem;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.google.common.primitives.Ints;
import io.airlift.log.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile.WriterOptions;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.raptor.util.Closer.closer;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.units.Duration.nanosSince;
import static org.apache.hadoop.hive.ql.io.orc.OrcFile.createReader;
import static org.apache.hadoop.hive.ql.io.orc.OrcFile.createWriter;
import static org.apache.hadoop.hive.ql.io.orc.OrcFile.writerOptions;
import static org.apache.hadoop.hive.ql.io.orc.OrcUtil.getFieldValue;

public final class OrcFileRewriter
{
    private static final Logger log = Logger.get(OrcFileRewriter.class);
    private static final Configuration CONFIGURATION = new Configuration();

    private OrcFileRewriter() {}

    public static OrcFileInfo rewrite(File input, File output, BitSet rowsToDelete)
            throws IOException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(FileSystem.class.getClassLoader());
                FileSystem fileSystem = new SyncingFileSystem(CONFIGURATION)) {
            Reader reader = createReader(fileSystem, path(input));

            if (reader.getNumberOfRows() < rowsToDelete.length()) {
                throw new IOException("File has fewer rows than deletion vector");
            }
            int deleteRowCount = rowsToDelete.cardinality();
            if (reader.getNumberOfRows() == deleteRowCount) {
                return new OrcFileInfo(0, 0);
            }
            if (reader.getNumberOfRows() >= Integer.MAX_VALUE) {
                throw new IOException("File has too many rows");
            }
            int inputRowCount = Ints.checkedCast(reader.getNumberOfRows());

            WriterOptions writerOptions = writerOptions(CONFIGURATION)
                    .fileSystem(fileSystem)
                    .compress(reader.getCompression())
                    .inspector(reader.getObjectInspector());

            long start = System.nanoTime();
            try (Closer<RecordReader, IOException> recordReader = closer(reader.rows(), RecordReader::close);
                    Closer<Writer, IOException> writer = closer(createWriter(path(output), writerOptions), Writer::close)) {
                if (reader.hasMetadataValue(OrcFileMetadata.KEY)) {
                    ByteBuffer orcFileMetadata = reader.getMetadataValue(OrcFileMetadata.KEY);
                    writer.get().addUserMetadata(OrcFileMetadata.KEY, orcFileMetadata);
                }
                OrcFileInfo fileInfo = rewrite(recordReader.get(), writer.get(), rowsToDelete, inputRowCount);
                log.debug("Rewrote file %s in %s (input rows: %s, output rows: %s)", input.getName(), nanosSince(start), inputRowCount, inputRowCount - deleteRowCount);
                return fileInfo;
            }
        }
    }

    private static OrcFileInfo rewrite(RecordReader reader, Writer writer, BitSet rowsToDelete, int inputRowCount)
            throws IOException
    {
        Object object = null;
        int row = 0;
        long rowCount = 0;
        long uncompressedSize = 0;

        row = rowsToDelete.nextClearBit(row);
        if (row < inputRowCount) {
            reader.seekToRow(row);
        }

        while (row < inputRowCount) {
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedIOException();
            }

            // seekToRow() is extremely expensive
            if (reader.getRowNumber() < row) {
                reader.next(object);
                continue;
            }

            object = reader.next(object);
            writer.addRow(object);
            rowCount++;
            uncompressedSize += uncompressedSize(object);

            row = rowsToDelete.nextClearBit(row + 1);
        }
        return new OrcFileInfo(rowCount, uncompressedSize);
    }

    private static Path path(File input)
    {
        return new Path(input.toURI());
    }

    private static int uncompressedSize(Object object)
            throws IOException
    {
        if (object instanceof OrcStruct) {
            OrcStruct struct = (OrcStruct) object;
            int size = 0;
            for (int i = 0; i < struct.getNumFields(); i++) {
                size += uncompressedSize(getFieldValue(struct, i));
            }
            return size;
        }
        if ((object == null) || (object instanceof BooleanWritable)) {
            return SIZE_OF_BYTE;
        }
        if (object instanceof LongWritable) {
            return SIZE_OF_LONG;
        }
        if (object instanceof DoubleWritable) {
            return SIZE_OF_DOUBLE;
        }
        if (object instanceof Text) {
            return ((Text) object).getLength();
        }
        if (object instanceof BytesWritable) {
            return ((BytesWritable) object).getLength();
        }
        if (object instanceof List<?>) {
            int size = 0;
            for (Object element : (Iterable<?>) object) {
                size += uncompressedSize(element);
            }
            return size;
        }
        if (object instanceof Map<?, ?>) {
            int size = 0;
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) object).entrySet()) {
                size += uncompressedSize(entry.getKey());
                size += uncompressedSize(entry.getValue());
            }
            return size;
        }
        throw new IOException("Unhandled ORC object: " + object.getClass().getName());
    }

    public static class OrcFileInfo
    {
        private final long rowCount;
        private final long uncompressedSize;

        public OrcFileInfo(long rowCount, long uncompressedSize)
        {
            this.rowCount = rowCount;
            this.uncompressedSize = uncompressedSize;
        }

        public long getRowCount()
        {
            return rowCount;
        }

        public long getUncompressedSize()
        {
            return uncompressedSize;
        }
    }
}
