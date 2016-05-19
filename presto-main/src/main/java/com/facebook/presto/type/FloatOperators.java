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
package com.facebook.presto.type;

import com.facebook.presto.operator.scalar.ScalarOperator;
import com.facebook.presto.spi.type.StandardTypes;

import static com.facebook.presto.metadata.OperatorType.ADD;
import static com.facebook.presto.metadata.OperatorType.BETWEEN;
import static com.facebook.presto.metadata.OperatorType.DIVIDE;
import static com.facebook.presto.metadata.OperatorType.EQUAL;
import static com.facebook.presto.metadata.OperatorType.GREATER_THAN;
import static com.facebook.presto.metadata.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.metadata.OperatorType.HASH_CODE;
import static com.facebook.presto.metadata.OperatorType.LESS_THAN;
import static com.facebook.presto.metadata.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.metadata.OperatorType.MODULUS;
import static com.facebook.presto.metadata.OperatorType.MULTIPLY;
import static com.facebook.presto.metadata.OperatorType.NEGATION;
import static com.facebook.presto.metadata.OperatorType.NOT_EQUAL;
import static com.facebook.presto.metadata.OperatorType.SUBTRACT;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;

public final class FloatOperators
{
    private FloatOperators()
    {
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.FLOAT)
    public static long add(@SqlType(StandardTypes.FLOAT) long left, @SqlType(StandardTypes.FLOAT) long right)
    {
        return floatToRawIntBits(intBitsToFloat((int) left) + intBitsToFloat((int) right));
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.FLOAT)
    public static long subtract(@SqlType(StandardTypes.FLOAT) long left, @SqlType(StandardTypes.FLOAT) long right)
    {
        return floatToRawIntBits(intBitsToFloat((int) left) - intBitsToFloat((int) right));
    }

    @ScalarOperator(MULTIPLY)
    @SqlType(StandardTypes.FLOAT)
    public static long multiply(@SqlType(StandardTypes.FLOAT) long left, @SqlType(StandardTypes.FLOAT) long right)
    {
        return floatToRawIntBits(intBitsToFloat((int) left) * intBitsToFloat((int) right));
    }

    @ScalarOperator(DIVIDE)
    @SqlType(StandardTypes.FLOAT)
    public static long divide(@SqlType(StandardTypes.FLOAT) long left, @SqlType(StandardTypes.FLOAT) long right)
    {
        return floatToRawIntBits(intBitsToFloat((int) left) / intBitsToFloat((int) right));
    }

    @ScalarOperator(MODULUS)
    @SqlType(StandardTypes.FLOAT)
    public static long modulus(@SqlType(StandardTypes.FLOAT) long left, @SqlType(StandardTypes.FLOAT) long right)
    {
        return floatToRawIntBits(intBitsToFloat((int) left) % intBitsToFloat((int) right));
    }

    @ScalarOperator(NEGATION)
    @SqlType(StandardTypes.FLOAT)
    public static long negate(@SqlType(StandardTypes.FLOAT) long value)
    {
        return floatToRawIntBits(-intBitsToFloat((int) value));
    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean equal(@SqlType(StandardTypes.FLOAT) long left, @SqlType(StandardTypes.FLOAT) long right)
    {
        return intBitsToFloat((int) left) == intBitsToFloat((int) right);
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean notEqual(@SqlType(StandardTypes.FLOAT) long left, @SqlType(StandardTypes.FLOAT) long right)
    {
        return intBitsToFloat((int) left) != intBitsToFloat((int) right);
    }

    @ScalarOperator(LESS_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThan(@SqlType(StandardTypes.FLOAT) long left, @SqlType(StandardTypes.FLOAT) long right)
    {
        return intBitsToFloat((int) left) < intBitsToFloat((int) right);
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThanOrEqual(@SqlType(StandardTypes.FLOAT) long left, @SqlType(StandardTypes.FLOAT) long right)
    {
        return intBitsToFloat((int) left) <= intBitsToFloat((int) right);
    }

    @ScalarOperator(GREATER_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThan(@SqlType(StandardTypes.FLOAT) long left, @SqlType(StandardTypes.FLOAT) long right)
    {
        return intBitsToFloat((int) left) > intBitsToFloat((int) right);
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThanOrEqual(@SqlType(StandardTypes.FLOAT) long left, @SqlType(StandardTypes.FLOAT) long right)
    {
        return intBitsToFloat((int) left) >= intBitsToFloat((int) right);
    }

    @ScalarOperator(BETWEEN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean between(@SqlType(StandardTypes.FLOAT) long value, @SqlType(StandardTypes.FLOAT) long min, @SqlType(StandardTypes.FLOAT) long max)
    {
        return intBitsToFloat((int) min) <= intBitsToFloat((int) value) &&
                intBitsToFloat((int) value) <= intBitsToFloat((int) max);
    }

    @ScalarOperator(HASH_CODE)
    @SqlType(StandardTypes.BIGINT)
    public static long hashCode(@SqlType(StandardTypes.FLOAT) long value)
    {
        return value;
    }
}
