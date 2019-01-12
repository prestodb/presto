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
import com.facebook.presto.spi.function.IsNull;
import com.facebook.presto.spi.function.OperatorDependency;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.spi.function.OperatorType.INDETERMINATE;
import static com.facebook.presto.spi.type.TypeUtils.readNativeValue;
import static com.facebook.presto.util.Failures.internalError;

@ScalarOperator(INDETERMINATE)
public final class MapIndeterminateOperator
{
    private MapIndeterminateOperator() {}

    @TypeParameter("K")
    @TypeParameter("V")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean indeterminate(
            @OperatorDependency(operator = INDETERMINATE, returnType = StandardTypes.BOOLEAN, argumentTypes = {"V"}) MethodHandle valueIndeterminateFunction,
            @TypeParameter("K") Type keyType,
            @TypeParameter("V") Type valueType,
            @SqlType("map(K,V)") Block block,
            @IsNull boolean isNull)
    {
        if (isNull) {
            return true;
        }
        for (int i = 0; i < block.getPositionCount(); i += 2) {
            // since maps are not allowed to have indeterminate keys we only check values here
            if (block.isNull(i + 1)) {
                return true;
            }
            try {
                if ((boolean) valueIndeterminateFunction.invoke(readNativeValue(valueType, block, i + 1), false)) {
                    return true;
                }
            }
            catch (Throwable t) {
                throw internalError(t);
            }
        }
        return false;
    }
}
