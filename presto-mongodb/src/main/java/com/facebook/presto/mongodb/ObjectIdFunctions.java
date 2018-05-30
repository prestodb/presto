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
package com.facebook.presto.mongodb;

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import com.google.common.base.CharMatcher;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.bson.types.ObjectId;

import static com.facebook.presto.spi.function.OperatorType.BETWEEN;
import static com.facebook.presto.spi.function.OperatorType.EQUAL;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.HASH_CODE;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.NOT_EQUAL;

public class ObjectIdFunctions
{
    private ObjectIdFunctions() {}

    @Description("mongodb ObjectId")
    @ScalarFunction("objectid")
    @SqlType("ObjectId")
    public static Slice ObjectId()
    {
        return Slices.wrappedBuffer(new ObjectId().toByteArray());
    }

    @Description("mongodb ObjectId from the given string")
    @ScalarFunction("objectid")
    @SqlType("ObjectId")
    public static Slice ObjectId(@SqlType(StandardTypes.VARCHAR) Slice value)
    {
        return Slices.wrappedBuffer(new ObjectId(CharMatcher.is(' ').removeFrom(value.toStringUtf8())).toByteArray());
    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean equal(@SqlType("ObjectId") Slice left, @SqlType("ObjectId") Slice right)
    {
        return left.equals(right);
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean notEqual(@SqlType("ObjectId") Slice left, @SqlType("ObjectId") Slice right)
    {
        return !left.equals(right);
    }

    @ScalarOperator(GREATER_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThan(@SqlType("ObjectId") Slice left, @SqlType("ObjectId") Slice right)
    {
        return compareTo(left, right) > 0;
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThanOrEqual(@SqlType("ObjectId") Slice left, @SqlType("ObjectId") Slice right)
    {
        return compareTo(left, right) >= 0;
    }

    @ScalarOperator(LESS_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThan(@SqlType("ObjectId") Slice left, @SqlType("ObjectId") Slice right)
    {
        return compareTo(left, right) < 0;
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThanOrEqual(@SqlType("ObjectId") Slice left, @SqlType("ObjectId") Slice right)
    {
        return compareTo(left, right) <= 0;
    }

    @ScalarOperator(BETWEEN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean between(@SqlType("ObjectId") Slice value, @SqlType("ObjectId") Slice min, @SqlType("ObjectId") Slice max)
    {
        return compareTo(value, min) >= 0 && compareTo(value, max) <= 0;
    }

    @ScalarOperator(HASH_CODE)
    @SqlType(StandardTypes.BIGINT)
    public static long hashCode(@SqlType("ObjectId") Slice value)
    {
        return new ObjectId(value.getBytes()).hashCode();
    }

    private static int compareTo(Slice left, Slice right)
    {
        return new ObjectId(left.getBytes()).compareTo(new ObjectId(right.getBytes()));
    }
}
