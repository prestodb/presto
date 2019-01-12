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
package io.prestosql.rcfile.binary;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.prestosql.rcfile.ColumnEncoding;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;

public interface BinaryColumnEncoding
        extends ColumnEncoding
{
    void encodeValueInto(Block block, int position, SliceOutput output);

    int getValueOffset(Slice slice, int offset);

    int getValueLength(Slice slice, int offset);

    void decodeValueInto(BlockBuilder builder, Slice slice, int offset, int length);
}
