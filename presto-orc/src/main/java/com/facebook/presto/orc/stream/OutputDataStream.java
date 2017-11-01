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

import com.facebook.presto.orc.metadata.Stream;
import io.airlift.slice.SliceOutput;

import javax.annotation.Nonnull;

import java.util.Optional;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public final class OutputDataStream
        implements Comparable<OutputDataStream>
{
    private final Function<SliceOutput, Optional<Stream>> writer;
    private final long sizeInBytes;

    public OutputDataStream(Function<SliceOutput, Optional<Stream>> writer, long sizeInBytes)
    {
        this.writer = requireNonNull(writer, "writer is null");
        this.sizeInBytes = sizeInBytes;
    }

    @Override
    public int compareTo(@Nonnull OutputDataStream otherStream)
    {
        return Long.compare(sizeInBytes, otherStream.sizeInBytes);
    }

    public long getSizeInBytes()
    {
        return sizeInBytes;
    }

    public Optional<Stream> writeData(SliceOutput sliceOutput)
    {
        return writer.apply(sliceOutput);
    }
}
