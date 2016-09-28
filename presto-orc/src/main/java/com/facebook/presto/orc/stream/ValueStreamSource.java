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

import javax.annotation.Nullable;

import java.io.IOException;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ValueStreamSource<S extends ValueStream<?>> implements StreamSource<S>
{
    private final S stream;

    public ValueStreamSource(S stream)
    {
        this.stream = requireNonNull(stream, "stream is null");
    }

    @Override
    public Class<S> getStreamType()
    {
        return (Class<S>) stream.getClass();
    }

    @Nullable
    @Override
    public S openStream()
            throws IOException
    {
        return stream;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("stream", stream)
                .toString();
    }
}
