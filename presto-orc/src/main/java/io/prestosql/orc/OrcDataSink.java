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
package io.prestosql.orc;

import io.prestosql.orc.stream.OrcDataOutput;

import java.io.IOException;
import java.util.List;

public interface OrcDataSink
{
    /**
     * Number of bytes written to this sink so far.
     */
    long size();

    /**
     * Gets the size of the memory buffers.
     */
    long getRetainedSizeInBytes();

    /**
     * Write a stripe and optionally header and footer data
     */
    void write(List<OrcDataOutput> outputData)
            throws IOException;

    /**
     * ORC file is complete
     */
    void close()
            throws IOException;
}
