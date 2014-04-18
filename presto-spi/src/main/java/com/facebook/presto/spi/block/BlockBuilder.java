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
package com.facebook.presto.spi.block;

import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;

public interface BlockBuilder
        extends RandomAccessBlock
{
    BlockBuilder append(boolean value);

    BlockBuilder append(long value);

    BlockBuilder append(double value);

    BlockBuilder append(Slice value);

    BlockBuilder append(Slice value, int offset, int length);

    BlockBuilder appendNull();

    RandomAccessBlock build();

    int getPositionCount();

    Type getType();

    boolean isEmpty();

    boolean isFull();

    int size();
}
