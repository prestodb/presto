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
package com.facebook.presto.hive.orc.stream;

import java.io.IOException;
import java.io.InputStream;

import static com.facebook.presto.hive.orc.stream.OrcStreamUtils.readFully;
import static com.facebook.presto.hive.orc.stream.OrcStreamUtils.skipFully;
import static com.google.common.base.Preconditions.checkNotNull;

public class ByteArrayStream
{
    private final InputStream inputStream;

    public ByteArrayStream(InputStream inputStream)
    {
        this.inputStream = checkNotNull(inputStream, "inputStream is null");
    }

    public byte[] next(int length)
            throws IOException
    {
        byte[] data = new byte[length];
        readFully(inputStream, data, 0, length);
        return data;
    }

    public void next(int length, byte[] data)
            throws IOException
    {
        readFully(inputStream, data, 0, length);
    }

    public void skip(long skipSize)
            throws IOException
    {
        skipFully(inputStream, skipSize);
    }
}
