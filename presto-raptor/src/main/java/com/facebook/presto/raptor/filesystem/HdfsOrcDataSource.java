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
package com.facebook.presto.raptor.filesystem;

import com.facebook.presto.orc.AbstractOrcDataSource;
import com.facebook.presto.orc.OrcDataSourceId;
import com.facebook.presto.spi.PrestoException;
import io.airlift.units.DataSize;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;

import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HdfsOrcDataSource
        extends AbstractOrcDataSource
{
    private final FSDataInputStream inputStream;

    public HdfsOrcDataSource(
            OrcDataSourceId id,
            long size,
            DataSize maxMergeDistance,
            DataSize maxReadSize,
            DataSize streamBufferSize,
            boolean lazyReadSmallRanges,
            FSDataInputStream inputStream)
    {
        super(id, size, maxMergeDistance, maxReadSize, streamBufferSize, lazyReadSmallRanges);
        this.inputStream = requireNonNull(inputStream, "inputStream is null");
    }

    @Override
    public void close()
            throws IOException
    {
        inputStream.close();
    }

    @Override
    protected void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
    {
        try {
            inputStream.readFully(position, buffer, bufferOffset, bufferLength);
        }
        catch (PrestoException e) {
            // just in case there is a Presto wrapper or hook
            throw e;
        }
        catch (Exception e) {
            String message = format("Error reading from %s at position %s", this, position);
            if (e.getClass().getSimpleName().equals("BlockMissingException")) {
                throw new PrestoException(RAPTOR_ERROR, message, e);
            }
            if (e instanceof IOException) {
                throw new PrestoException(RAPTOR_ERROR, message, e);
            }
            throw new PrestoException(RAPTOR_ERROR, message, e);
        }
    }
}
