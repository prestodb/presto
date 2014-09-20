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
package com.facebook.presto.testing;

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.block.dictionary.DictionaryBlockEncoding;
import com.facebook.presto.block.rle.RunLengthBlockEncoding;
import com.facebook.presto.block.snappy.SnappyBlockEncoding;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.block.FixedWidthBlockEncoding;
import com.facebook.presto.spi.block.SliceArrayBlockEncoding;
import com.facebook.presto.spi.block.VariableWidthBlockEncoding;
import com.facebook.presto.type.TypeRegistry;

public final class TestingBlockEncodingManager
{
    private TestingBlockEncodingManager()
    {
    }

    public static BlockEncodingSerde createTestingBlockEncodingManager()
    {
        return new BlockEncodingManager(
                new TypeRegistry(),
                VariableWidthBlockEncoding.FACTORY,
                FixedWidthBlockEncoding.FACTORY,
                RunLengthBlockEncoding.FACTORY,
                DictionaryBlockEncoding.FACTORY,
                SnappyBlockEncoding.FACTORY,
                SliceArrayBlockEncoding.FACTORY);
    }
}
