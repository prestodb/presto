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

import com.facebook.presto.orc.OrcDataSourceId;
import io.airlift.slice.Slice;

import java.io.IOException;

public interface OrcDataReader
{
    OrcDataSourceId getOrcDataSourceId();

    /**
     * Gets the memory size of this instance.
     */
    long getRetainedSize();

    /**
     * Total size of the data that can be accessed with {@link #seekBuffer(int)}.
     */
    int getSize();

    /**
     * Maximum size of the buffer returned from {@link #seekBuffer(int)}.
     */
    int getMaxBufferSize();

    /**
     * Returns a buffer of the data starting at the specified position.  This will
     * return a buffer of {@link #getMaxBufferSize()}, unless near the end of the data.
     */
    Slice seekBuffer(int position)
            throws IOException;
}
