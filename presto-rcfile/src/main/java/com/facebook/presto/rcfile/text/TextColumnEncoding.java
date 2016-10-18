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
package com.facebook.presto.rcfile.text;

import com.facebook.presto.rcfile.ColumnEncoding;
import com.facebook.presto.rcfile.RcFileCorruptionException;
import com.facebook.presto.spi.block.BlockBuilder;
import io.airlift.slice.Slice;

public interface TextColumnEncoding
        extends ColumnEncoding
{
    void decodeValueInto(int depth, BlockBuilder builder, Slice slice, int offset, int length)
            throws RcFileCorruptionException;
}
