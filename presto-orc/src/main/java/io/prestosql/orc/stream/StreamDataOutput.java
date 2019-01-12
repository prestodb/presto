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

import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.prestosql.orc.metadata.Stream;

import java.util.function.ToLongFunction;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public final class StreamDataOutput
        implements OrcDataOutput, Comparable<StreamDataOutput>
{
    private final ToLongFunction<SliceOutput> writer;
    private final Stream stream;

    public StreamDataOutput(Slice slice, Stream stream)
    {
        this(
                sliceOutput -> {
                    sliceOutput.writeBytes(slice);
                    return slice.length();
                },
                stream);
    }

    public StreamDataOutput(ToLongFunction<SliceOutput> writer, Stream stream)
    {
        this.writer = requireNonNull(writer, "writer is null");
        this.stream = requireNonNull(stream, "stream is null");
    }

    @Override
    public int compareTo(StreamDataOutput otherStream)
    {
        return Long.compare(size(), otherStream.size());
    }

    @Override
    public long size()
    {
        return stream.getLength();
    }

    public Stream getStream()
    {
        return stream;
    }

    @Override
    public void writeData(SliceOutput sliceOutput)
    {
        long size = writer.applyAsLong(sliceOutput);
        verify(stream.getLength() == size, "Data stream did not write expected size");
    }
}
