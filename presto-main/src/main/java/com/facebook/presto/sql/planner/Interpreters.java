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
package com.facebook.presto.sql.planner;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.type.LikeFunctions;
import io.airlift.joni.Regex;
import io.airlift.slice.Slice;

import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class Interpreters
{
    private Interpreters() {}

    static Object interpretDereference(Object value, Type returnType, int index)
    {
        Block row = (Block) value;

        checkState(index >= 0, "could not find field index: %s", index);
        if (row.isNull(index)) {
            return null;
        }
        Class<?> javaType = returnType.getJavaType();
        if (javaType == long.class) {
            return returnType.getLong(row, index);
        }
        else if (javaType == double.class) {
            return returnType.getDouble(row, index);
        }
        else if (javaType == boolean.class) {
            return returnType.getBoolean(row, index);
        }
        else if (javaType == Slice.class) {
            return returnType.getSlice(row, index);
        }
        else if (!javaType.isPrimitive()) {
            return returnType.getObject(row, index);
        }
        throw new UnsupportedOperationException("Dereference a unsupported primitive type: " + javaType.getName());
    }

    static boolean interpretLikePredicate(Type valueType, Slice value, Regex regex)
    {
        if (valueType instanceof VarcharType) {
            return LikeFunctions.likeVarchar(value, regex);
        }

        checkState(valueType instanceof CharType, "LIKE value is neither VARCHAR or CHAR");
        return LikeFunctions.likeChar((long) ((CharType) valueType).getLength(), value, regex);
    }

    public static class LambdaVariableResolver
            implements VariableResolver
    {
        private final Map<String, Object> values;

        public LambdaVariableResolver(Map<String, Object> values)
        {
            this.values = requireNonNull(values, "values is null");
        }

        @Override
        public Object getValue(VariableReferenceExpression variable)
        {
            checkState(values.containsKey(variable.getName()), "values does not contain %s", variable);
            return values.get(variable.getName());
        }
    }
}
