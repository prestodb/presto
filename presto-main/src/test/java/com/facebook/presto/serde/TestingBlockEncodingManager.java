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
package com.facebook.presto.serde;

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.block.dictionary.DictionaryBlockEncoding;
import com.facebook.presto.block.rle.RunLengthBlockEncoding;
import com.facebook.presto.block.snappy.SnappyBlockEncoding;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.HyperLogLogType;
import com.facebook.presto.spi.type.IntervalDayTimeType;
import com.facebook.presto.spi.type.IntervalYearMonthType;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimeWithTimeZoneType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.TimestampWithTimeZoneType;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.type.ColorType;
import com.facebook.presto.type.TypeRegistry;
import com.facebook.presto.type.UnknownType;

public final class TestingBlockEncodingManager
{
    private TestingBlockEncodingManager()
    {
    }

    public static BlockEncodingSerde createTestingBlockEncodingManager()
    {
        return new BlockEncodingManager(
                new TypeRegistry(),
                UnknownType.BLOCK_ENCODING_FACTORY,
                BooleanType.BLOCK_ENCODING_FACTORY,
                BigintType.BLOCK_ENCODING_FACTORY,
                DoubleType.BLOCK_ENCODING_FACTORY,
                VarcharType.BLOCK_ENCODING_FACTORY,
                DateType.BLOCK_ENCODING_FACTORY,
                TimeType.BLOCK_ENCODING_FACTORY,
                TimeWithTimeZoneType.BLOCK_ENCODING_FACTORY,
                TimestampType.BLOCK_ENCODING_FACTORY,
                TimestampWithTimeZoneType.BLOCK_ENCODING_FACTORY,
                IntervalYearMonthType.BLOCK_ENCODING_FACTORY,
                IntervalDayTimeType.BLOCK_ENCODING_FACTORY,
                RunLengthBlockEncoding.FACTORY,
                DictionaryBlockEncoding.FACTORY,
                SnappyBlockEncoding.FACTORY,
                HyperLogLogType.BLOCK_ENCODING_FACTORY,
                ColorType.BLOCK_ENCODING_FACTORY);
    }
}
