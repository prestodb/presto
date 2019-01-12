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

import java.io.IOException;

public interface ValueInputStream<C extends StreamCheckpoint>
{
    Class<? extends C> getCheckpointType();

    void seekToCheckpoint(C checkpoint)
            throws IOException;

    void skip(long items)
            throws IOException;
}
