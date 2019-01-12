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

package com.facebook.presto.operator.scalar;

import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.util.Failures.checkCondition;
import static java.lang.String.format;

@Description("creates a map using entryDelimiter and keyValueDelimiter")
@ScalarFunction("split_to_map")
public class SplitToMapFunction
{
    private final PageBuilder pageBuilder;

    public SplitToMapFunction(@TypeParameter("map<varchar,varchar>") Type mapType)
    {
        pageBuilder = new PageBuilder(ImmutableList.of(mapType));
    }

    @SqlType("map<varchar,varchar>")
    public Block splitToMap(@TypeParameter("map<varchar,varchar>") Type mapType, @SqlType(StandardTypes.VARCHAR) Slice string, @SqlType(StandardTypes.VARCHAR) Slice entryDelimiter, @SqlType(StandardTypes.VARCHAR) Slice keyValueDelimiter)
    {
        checkCondition(entryDelimiter.length() > 0, INVALID_FUNCTION_ARGUMENT, "entryDelimiter is empty");
        checkCondition(keyValueDelimiter.length() > 0, INVALID_FUNCTION_ARGUMENT, "keyValueDelimiter is empty");
        checkCondition(!entryDelimiter.equals(keyValueDelimiter), INVALID_FUNCTION_ARGUMENT, "entryDelimiter and keyValueDelimiter must not be the same");

        Map<Slice, Slice> map = new HashMap<>();
        int entryStart = 0;
        while (entryStart < string.length()) {
            // Extract key-value pair based on current index
            // then add the pair if it can be split by keyValueDelimiter
            Slice keyValuePair;
            int entryEnd = string.indexOf(entryDelimiter, entryStart);
            if (entryEnd >= 0) {
                keyValuePair = string.slice(entryStart, entryEnd - entryStart);
            }
            else {
                // The rest of the string is the last possible pair.
                keyValuePair = string.slice(entryStart, string.length() - entryStart);
            }

            int keyEnd = keyValuePair.indexOf(keyValueDelimiter);
            if (keyEnd >= 0) {
                int valueStart = keyEnd + keyValueDelimiter.length();
                Slice key = keyValuePair.slice(0, keyEnd);
                Slice value = keyValuePair.slice(valueStart, keyValuePair.length() - valueStart);

                if (value.indexOf(keyValueDelimiter) >= 0) {
                    throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Key-value delimiter must appear exactly once in each entry. Bad input: '" + keyValuePair.toStringUtf8() + "'");
                }
                if (map.containsKey(key)) {
                    throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Duplicate keys (%s) are not allowed", key.toStringUtf8()));
                }

                map.put(key, value);
            }
            else {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Key-value delimiter must appear exactly once in each entry. Bad input: '" + keyValuePair.toStringUtf8() + "'");
            }

            if (entryEnd < 0) {
                // No more pairs to add
                break;
            }
            // Next possible pair is placed next to the current entryDelimiter
            entryStart = entryEnd + entryDelimiter.length();
        }

        if (pageBuilder.isFull()) {
            pageBuilder.reset();
        }
        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(0);
        BlockBuilder singleMapBlockBuilder = blockBuilder.beginBlockEntry();
        for (Map.Entry<Slice, Slice> entry : map.entrySet()) {
            VARCHAR.writeSlice(singleMapBlockBuilder, entry.getKey());
            VARCHAR.writeSlice(singleMapBlockBuilder, entry.getValue());
        }
        blockBuilder.closeEntry();
        pageBuilder.declarePosition();

        return (Block) mapType.getObject(blockBuilder, blockBuilder.getPositionCount() - 1);
    }
}
