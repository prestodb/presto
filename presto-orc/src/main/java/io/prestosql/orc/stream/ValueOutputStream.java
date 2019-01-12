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

import io.prestosql.orc.checkpoint.StreamCheckpoint;

import java.util.List;

public interface ValueOutputStream<C extends StreamCheckpoint>
{
    void recordCheckpoint();

    void close();

    List<C> getCheckpoints();

    StreamDataOutput getStreamDataOutput(int column);

    /**
     * This method returns the size of the flushed data plus any unflushed data.
     * If the output is compressed, flush data size is the size after compression.
     */
    long getBufferedBytes();

    long getRetainedBytes();

    void reset();
}
