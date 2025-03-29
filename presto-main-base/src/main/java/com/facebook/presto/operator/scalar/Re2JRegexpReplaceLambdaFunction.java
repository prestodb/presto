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
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.sql.gen.lambda.UnaryFunctionInterface;
import com.facebook.presto.type.Re2JRegexp;
import com.facebook.presto.type.Re2JRegexpType;
import com.google.re2j.Matcher;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;

@ScalarFunction("regexp_replace")
@Description("replaces substrings matching a regular expression using a lambda function")
public final class Re2JRegexpReplaceLambdaFunction
{
    @LiteralParameters("x")
    @SqlType("varchar")
    @SqlNullable
    public Slice regexpReplace(
            @SqlType("varchar") Slice source,
            @SqlType(Re2JRegexpType.NAME) Re2JRegexp pattern,
            @SqlType("function(array(varchar), varchar(x))") UnaryFunctionInterface replaceFunction)
    {
        // If there is no match we can simply return the original source without doing copy.
        Matcher matcher = pattern.re2jPattern.matcher(source);
        if (!matcher.find()) {
            return source;
        }

        SliceOutput output = new DynamicSliceOutput(source.length());
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, 10);

        int groupCount = matcher.groupCount();
        int appendPosition = 0;

        do {
            int start = matcher.start();
            int end = matcher.end();

            // Append the un-matched part
            if (appendPosition < start) {
                output.writeBytes(source, appendPosition, start - appendPosition);
            }
            appendPosition = end;

            // Append the capturing groups to the target block that will be passed to lambda
            for (int i = 1; i <= groupCount; i++) {
                Slice matchedGroupSlice = matcher.group(i);
                if (matchedGroupSlice != null) {
                    VARCHAR.writeSlice(blockBuilder, matchedGroupSlice);
                }
                else {
                    blockBuilder.appendNull();
                }
            }

            Block target = blockBuilder.getRegion(blockBuilder.getPositionCount() - groupCount, groupCount);

            // Call the lambda function to replace the block, and append the result to output
            Slice replaced = (Slice) replaceFunction.apply(target);
            if (replaced == null) {
                // replacing a substring with null (unknown) makes the entire string null
                return null;
            }
            output.appendBytes(replaced);
        }
        while (matcher.find());

        // Append the rest of un-matched
        output.writeBytes(source, appendPosition, source.length() - appendPosition);
        return output.slice();
    }
}
