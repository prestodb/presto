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
package io.prestosql.orc.stream;

import io.prestosql.orc.checkpoint.ByteArrayStreamCheckpoint;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class ByteArrayInputStream
        implements ValueInputStream<ByteArrayStreamCheckpoint>
{
    private final OrcInputStream inputStream;

    public ByteArrayInputStream(OrcInputStream inputStream)
    {
        this.inputStream = requireNonNull(inputStream, "inputStream is null");
    }

    public byte[] next(int length)
            throws IOException
    {
        byte[] data = new byte[length];
        next(data, 0, length);
        return data;
    }

    public void next(byte[] data, int offset, int length)
            throws IOException
    {
        inputStream.readFully(data, offset, length);
    }

    @Override
    public Class<ByteArrayStreamCheckpoint> getCheckpointType()
    {
        return ByteArrayStreamCheckpoint.class;
    }

    @Override
    public void seekToCheckpoint(ByteArrayStreamCheckpoint checkpoint)
            throws IOException
    {
        inputStream.seekToCheckpoint(checkpoint.getInputStreamCheckpoint());
    }

    @Override
    public void skip(long skipSize)
            throws IOException
    {
        inputStream.skipFully(skipSize);
    }
}
