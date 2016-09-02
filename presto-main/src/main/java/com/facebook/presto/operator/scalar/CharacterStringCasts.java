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

import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.type.LiteralParameter;
import io.airlift.slice.Slice;

import static com.facebook.presto.spi.type.Chars.padSpaces;
import static com.facebook.presto.spi.type.Chars.trimSpacesAndTruncateToLength;
import static com.facebook.presto.spi.type.Varchars.truncateToLength;

public final class CharacterStringCasts
{
    private CharacterStringCasts() {}

    @ScalarOperator(OperatorType.CAST)
    @SqlType("varchar(y)")
    @LiteralParameters({"x", "y"})
    public static Slice varcharToVarcharCast(@LiteralParameter("x") Long x, @LiteralParameter("y") Long y, @SqlType("varchar(x)") Slice slice)
    {
        if (x > y) {
            return truncateToLength(slice, y.intValue());
        }
        else {
            return slice;
        }
    }

    @ScalarOperator(OperatorType.CAST)
    @SqlType("char(y)")
    @LiteralParameters({"x", "y"})
    public static Slice charToCharCast(@LiteralParameter("x") Long x, @LiteralParameter("y") Long y, @SqlType("char(x)") Slice slice)
    {
        if (x > y) {
            return truncateToLength(slice, y.intValue());
        }
        else {
            return slice;
        }
    }

    @ScalarOperator(OperatorType.CAST)
    @SqlType("char(y)")
    @LiteralParameters({"x", "y"})
    public static Slice varcharToCharCast(@LiteralParameter("y") Long y, @SqlType("varchar(x)") Slice slice)
    {
        return trimSpacesAndTruncateToLength(slice, y.intValue());
    }

    @ScalarOperator(OperatorType.CAST)
    @SqlType("varchar(y)")
    @LiteralParameters({"x", "y"})
    public static Slice charToVarcharCast(@LiteralParameter("x") Long x, @LiteralParameter("y") Long y, @SqlType("char(x)") Slice slice)
    {
        if (x.intValue() <= y.intValue()) {
            return padSpaces(slice, x.intValue());
        }

        return padSpaces(truncateToLength(slice, y.intValue()), y.intValue());
    }
}
