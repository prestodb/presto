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
package com.facebook.presto.orc.stream;

import com.google.common.base.Objects;
import com.google.common.io.ByteSource;

import java.io.IOException;

public class ByteArrayStreamSource
        implements StreamSource<ByteArrayStream>
{
    private final ByteSource byteSource;

    public ByteArrayStreamSource(ByteSource byteSource)
    {
        this.byteSource = byteSource;
    }

    @Override
    public Class<ByteArrayStream> getStreamType()
    {
        return ByteArrayStream.class;
    }

    @Override
    public ByteArrayStream openStream()
            throws IOException
    {
        return new ByteArrayStream(byteSource.openStream());
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("byteSource", byteSource)
                .toString();
    }
}
