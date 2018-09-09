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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.util.Failures.checkCondition;


@Description("Return N-grams for the string")
public final class NgramsFunction
{
    private NgramsFunction() {}

    @ScalarFunction
    @LiteralParameters("x")
    @SqlType("array(varchar(x))")
    public static Block ngrams(@SqlType("varchar(x)") Slice string, @SqlType(StandardTypes.BIGINT) long n)
    {
        return ngrams(string, n, Slices.utf8Slice(" "), Slices.utf8Slice(" "));
    }

    @ScalarFunction
    @LiteralParameters({"x", "y"})
    @SqlType("array(varchar(x))")
    public static Block ngrams(@SqlType("varchar(x)") Slice string, @SqlType(StandardTypes.BIGINT) long n, @SqlType("varchar(y)") Slice delimiter)
    {
        return ngrams(string, n, delimiter, delimiter);
    }

    @ScalarFunction
    @LiteralParameters({"x", "y", "z"})
    @SqlType("array(varchar(x))")
    public static Block ngrams(@SqlType("varchar(x)") Slice string, @SqlType(StandardTypes.BIGINT) long n, @SqlType("varchar(y)") Slice delimiter, @SqlType("varchar(z)") Slice glue)
    {
        checkCondition(n > 0, INVALID_FUNCTION_ARGUMENT, "N must be positive");
        checkCondition(n <= Integer.MAX_VALUE, INVALID_FUNCTION_ARGUMENT, "N is too large");
        BlockBuilder parts = VARCHAR.createBlockBuilder(null, (int) n, string.length());
        Queue<Slice> queue = new LinkedList<>();
        int index = 0;
        // Track the sum of slices' length in the queue
        int curSlicesLen = 0;
        while (index < string.length()) {
            int splitIndex = string.indexOf(delimiter, index);
            // If the delimiter is an empty str, increase the index by one
            if (delimiter.length() == 0) {
                if (splitIndex >= string.length() - 1) {
                    break;
                }
                splitIndex = index + 1;
            }
            // Found split?
            if (splitIndex < 0) {
                break;
            }
            // Add the slice to the queue and increase the curSlicesLen
            queue.add(string.slice(index,splitIndex - index));
            curSlicesLen += splitIndex - index;
            if (queue.size() == n) {
                Slice slice = mergeSlices(queue, glue, curSlicesLen);
                VARCHAR.writeSlice(parts, slice);
                // Pop the head slice and deduct its len from the curSlicesLen
                curSlicesLen -= queue.remove().length();
            }
            // Continue searching after delimiter
            index = splitIndex + delimiter.length();
        }
        queue.add(string.slice(index, string.length() - index));
        curSlicesLen += string.length() - index;

        Slice slice = mergeSlices(queue, glue, curSlicesLen);
        VARCHAR.writeSlice(parts, slice);
        return parts.build();
    }

    private static Slice mergeSlices(Queue<Slice> queue, Slice glue, int curSlicesLen) {
        Slice slice = Slices.allocate(curSlicesLen + glue.length() * (queue.size() - 1)) ;
        Iterator<Slice> iterator = queue.iterator();
        int index = 0;
        while(iterator.hasNext()){
            Slice s = iterator.next();
            slice.setBytes(index, s);
            index += s.length();
            if (index < slice.length()) {
                slice.setBytes(index, glue);
                index += glue.length();
            }
        }
        return slice;
    }

}
