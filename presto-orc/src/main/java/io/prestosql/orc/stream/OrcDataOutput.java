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

import static java.util.Objects.requireNonNull;

public interface OrcDataOutput
{
    static OrcDataOutput createDataOutput(Slice slice)
    {
        requireNonNull(slice, "slice is null");
        return new OrcDataOutput()
        {
            @Override
            public long size()
            {
                return slice.length();
            }

            @Override
            public void writeData(SliceOutput sliceOutput)
            {
                sliceOutput.writeBytes(slice);
            }
        };
    }

    /**
     * Number of bytes that will be written.
     */
    long size();

    /**
     * Writes data to the output. The output must be exactly
     * {@link #size()} bytes.
     */
    void writeData(SliceOutput sliceOutput);
}
