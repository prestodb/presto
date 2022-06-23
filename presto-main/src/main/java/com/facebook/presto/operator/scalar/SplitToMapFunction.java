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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.sql.gen.lambda.LambdaFunctionInterface;
import io.airlift.slice.Slice;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.util.Failures.checkCondition;
import static java.lang.String.format;

public class SplitToMapFunction
{
    private SplitToMapFunction() {}

    private static Block splitToMap(
            Type mapType,
            Slice string,
            Slice entryDelimiter,
            Slice keyValueDelimiter,
            Optional<DuplicateKeyResolutionLambda> function)
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
                    throw new PrestoException(
                            INVALID_FUNCTION_ARGUMENT,
                            "Key-value delimiter must appear exactly once in each entry. Bad input: '" + keyValuePair.toStringUtf8() + "'");
                }

                if (map.containsKey(key)) {
                    if (!function.isPresent()) {
                        throw new PrestoException(
                                INVALID_FUNCTION_ARGUMENT,
                                format("Duplicate keys (%s) are not allowed. Specifying a lambda to resolve conflicts can avoid this error", key.toStringUtf8()));
                    }

                    value = function.get().apply(key, map.get(key), value);
                }

                map.put(key, value);
            }
            else {
                throw new PrestoException(
                        INVALID_FUNCTION_ARGUMENT,
                        "Key-value delimiter must appear exactly once in each entry. Bad input: '" + keyValuePair.toStringUtf8() + "'");
            }

            if (entryEnd < 0) {
                // No more pairs to add
                break;
            }
            // Next possible pair is placed next to the current entryDelimiter
            entryStart = entryEnd + entryDelimiter.length();
        }

        BlockBuilder blockBuilder = mapType.createBlockBuilder(null, 10);
        BlockBuilder singleMapBlockBuilder = blockBuilder.beginBlockEntry();
        for (Map.Entry<Slice, Slice> entry : map.entrySet()) {
            VARCHAR.writeSlice(singleMapBlockBuilder, entry.getKey());
            VARCHAR.writeSlice(singleMapBlockBuilder, entry.getValue());
        }
        blockBuilder.closeEntry();
        return (Block) mapType.getObject(blockBuilder, blockBuilder.getPositionCount() - 1);
    }

    @Description("creates a map using entryDelimiter and keyValueDelimiter")
    @ScalarFunction("split_to_map")
    public static class FailOnDuplicateKeys
    {
        public FailOnDuplicateKeys(@TypeParameter("map<varchar,varchar>") Type mapType) {}

        @SqlType("map<varchar,varchar>")
        public Block split(
                @TypeParameter("map<varchar,varchar>") Type mapType,
                @SqlType(StandardTypes.VARCHAR) Slice string,
                @SqlType(StandardTypes.VARCHAR) Slice entryDelimiter,
                @SqlType(StandardTypes.VARCHAR) Slice keyValueDelimiter)
        {
            return splitToMap(mapType, string, entryDelimiter, keyValueDelimiter, Optional.empty());
        }
    }

    @Description("creates a map using entryDelimiter and keyValueDelimiter along with a lambda to handle duplicate keys")
    @ScalarFunction("split_to_map")
    public static class ResolveDuplicateKeys
    {
        public ResolveDuplicateKeys(@TypeParameter("map<varchar,varchar>") Type mapType) {}

        @SqlType("map<varchar,varchar>")
        public Block split(
                @TypeParameter("map<varchar,varchar>") Type mapType,
                @SqlType(StandardTypes.VARCHAR) Slice string,
                @SqlType(StandardTypes.VARCHAR) Slice entryDelimiter,
                @SqlType(StandardTypes.VARCHAR) Slice keyValueDelimiter,
                @SqlType("function(varchar, varchar, varchar, varchar)") DuplicateKeyResolutionLambda function)
        {
            return splitToMap(mapType, string, entryDelimiter, keyValueDelimiter, Optional.of(function));
        }
    }

    @FunctionalInterface
    public interface DuplicateKeyResolutionLambda
            extends LambdaFunctionInterface
    {
        Slice apply(Slice key, Slice value1, Slice value2);
    }
}
