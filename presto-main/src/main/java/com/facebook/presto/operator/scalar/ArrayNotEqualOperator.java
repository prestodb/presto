package com.facebook.presto.operator.scalar;
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

import com.facebook.presto.operator.scalar.annotations.OperatorDependency;
import com.facebook.presto.operator.scalar.annotations.ScalarOperator;
import com.facebook.presto.operator.scalar.annotations.TypeParameter;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.SqlType;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.metadata.OperatorType.EQUAL;
import static com.facebook.presto.metadata.OperatorType.NOT_EQUAL;

@ScalarOperator(NOT_EQUAL)
public final class ArrayNotEqualOperator
{
    private ArrayNotEqualOperator() {}

    @TypeParameter("E")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean notEqual(
            @OperatorDependency(operator = EQUAL, returnType = StandardTypes.BOOLEAN, argumentTypes = {"E", "E"}) MethodHandle equalsFunction,
            @TypeParameter("E") Type type,
            @SqlType("array(E)") Block left,
            @SqlType("array(E)") Block right)
    {
        return !ArrayEqualOperator.equals(equalsFunction, type, left, right);
    }
}
