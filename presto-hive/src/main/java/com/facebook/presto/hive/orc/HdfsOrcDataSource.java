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
package com.facebook.presto.hive.orc;

import com.facebook.presto.orc.AbstractOrcDataSource;
import com.facebook.presto.spi.PrestoException;
import io.airlift.units.DataSize;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_MISSING_DATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_UNKNOWN_ERROR;
import static java.lang.String.format;

public class HdfsOrcDataSource
        extends AbstractOrcDataSource
{
    private final FSDataInputStream inputStream;

    public HdfsOrcDataSource(String name, long size, DataSize maxMergeDistance, DataSize maxReadSize, DataSize streamBufferSize, FSDataInputStream inputStream)
    {
        super(name, size, maxMergeDistance, maxReadSize, streamBufferSize);
        this.inputStream = inputStream;
    }

    @Override
    public void close()
            throws IOException
    {
        inputStream.close();
    }

    @Override
    protected void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
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
                throw new PrestoException(HIVE_MISSING_DATA, message, e);
            }
            throw new PrestoException(HIVE_UNKNOWN_ERROR, message, e);
        }
    }
}
