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
package com.facebook.presto.spiller;

import com.facebook.presto.common.io.DataSink;
import com.facebook.presto.common.io.OutputStreamDataSink;
import com.facebook.presto.spi.spiller.SpillFileHolder;
import io.airlift.slice.InputStreamSliceInput;
import io.airlift.slice.SliceInput;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.google.common.base.Preconditions.checkState;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.util.Objects.requireNonNull;

@ThreadSafe
final class SpillLocalFileHolder
        implements SpillFileHolder
{
    private final Path filePath;
    private final int bufferSize;

    @GuardedBy("this")
    private boolean deleted;

    public SpillLocalFileHolder(Path filePath, int bufferSize)
    {
        this.filePath = requireNonNull(filePath, "filePath is null");
        this.bufferSize = bufferSize;
    }

    @Override
    public synchronized SliceInput newSliceInput()
            throws IOException
    {
        checkState(!deleted, "File already deleted");
        return new InputStreamSliceInput(Files.newInputStream(filePath), bufferSize);
    }

    @Override
    public synchronized DataSink newDataSink()
            throws IOException
    {
        checkState(!deleted, "File already deleted");
        OutputStream outputStream = Files.newOutputStream(filePath, APPEND);
        return new OutputStreamDataSink(outputStream, bufferSize);
    }

    @Override
    public synchronized void close()
    {
        if (deleted) {
            return;
        }
        deleted = true;

        try {
            Files.delete(filePath);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
