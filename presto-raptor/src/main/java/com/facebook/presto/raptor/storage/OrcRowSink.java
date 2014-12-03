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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.mapred.JobConf;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.List;

import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_ERROR;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.hadoop.hive.ql.io.orc.CompressionKind.SNAPPY;

public class OrcRowSink
        implements RowSink
{
    private static final Constructor<? extends RecordWriter> WRITER_CONSTRUCTOR = getOrcWriterConstructor();

    private final RecordWriter recordWriter;
    private final OrcTupleBuffer tupleBuffer;

    public OrcRowSink(List<Long> columnIds, List<StorageType> columnTypes, Optional<Long> sampleWeightColumnId, File target)
    {
        tupleBuffer = new OrcTupleBuffer(columnIds, columnTypes, sampleWeightColumnId);
        recordWriter = createRecordWriter(new Path(target.toURI()), tupleBuffer.getJobConf());
    }

    @Override
    public void beginRecord(long sampleWeight)
    {
        tupleBuffer.beginRecord(sampleWeight);
    }

    @Override
    public void finishRecord()
    {
        try {
            recordWriter.write(tupleBuffer.finishRecordAndReset());
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_ERROR, "Failed to write record", e);
        }
    }

    @Override
    public int currentField()
    {
        return tupleBuffer.currentField();
    }

    @Override
    public void appendNull()
    {
        tupleBuffer.append(null);
    }

    @Override
    public void appendBoolean(boolean value)
    {
        tupleBuffer.append(value);
    }

    @Override
    public void appendLong(long value)
    {
        tupleBuffer.append(value);
    }

    @Override
    public void appendDouble(double value)
    {
        tupleBuffer.append(value);
    }

    @Override
    public void appendString(String value)
    {
        tupleBuffer.append(value);
    }

    @Override
    public void appendBytes(byte[] value)
    {
        tupleBuffer.append(value);
    }

    @Override
    public void close()
    {
        checkState(tupleBuffer.currentField() == -1, "record not finished");

        try {
            recordWriter.close(false);
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_ERROR, "Failed to close writer", e);
        }
    }

    private static RecordWriter createRecordWriter(Path target, JobConf conf)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(FileSystem.class.getClassLoader())) {
            FileSystem fileSystem = target.getFileSystem(conf);
            fileSystem.setWriteChecksum(false);
            OrcFile.WriterOptions options = OrcFile.writerOptions(conf)
                    .fileSystem(fileSystem)
                    .blockPadding(false)
                    .compress(SNAPPY);
            return WRITER_CONSTRUCTOR.newInstance(target, options);
        }
        catch (ReflectiveOperationException | IOException e) {
            throw new PrestoException(RAPTOR_ERROR, "Failed to create writer", e);
        }
    }

    private static Constructor<? extends RecordWriter> getOrcWriterConstructor()
    {
        try {
            String writerClassName = OrcOutputFormat.class.getName() + "$OrcRecordWriter";
            Constructor<? extends RecordWriter> constructor = OrcOutputFormat.class.getClassLoader()
                    .loadClass(writerClassName).asSubclass(RecordWriter.class)
                    .getDeclaredConstructor(Path.class, OrcFile.WriterOptions.class);
            constructor.setAccessible(true);
            return constructor;
        }
        catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
    }
}
