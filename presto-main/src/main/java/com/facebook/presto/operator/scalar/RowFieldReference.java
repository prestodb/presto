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

import com.facebook.presto.annotation.UsedByGeneratedCode;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.type.RowType;
import com.facebook.presto.util.Reflection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.sql.QueryUtil.mangleFieldReference;
import static java.util.Objects.requireNonNull;

public class RowFieldReference
        extends SqlScalarFunction
{
    private static final Map<String, MethodHandle> METHOD_HANDLE_MAP;

    private final MethodHandle methodHandle;

    static {
        ImmutableMap.Builder<String, MethodHandle> builder = ImmutableMap.builder();
        builder.put("long", Reflection.methodHandle(RowFieldReference.class, "longAccessor", Type.class, Integer.class, Block.class));
        builder.put("double", Reflection.methodHandle(RowFieldReference.class, "doubleAccessor", Type.class, Integer.class, Block.class));
        builder.put("boolean", Reflection.methodHandle(RowFieldReference.class, "booleanAccessor", Type.class, Integer.class, Block.class));
        builder.put("slice", Reflection.methodHandle(RowFieldReference.class, "sliceAccessor", Type.class, Integer.class, Block.class));
        METHOD_HANDLE_MAP = builder.build();
    }

    private static final MethodHandle METHOD_HANDLE_OBJECT = Reflection.methodHandle(RowFieldReference.class, "objectAccessor", Type.class, Integer.class, Block.class);

    public RowFieldReference(RowType type, Type returnType, int index, String fieldName)
    {
        super(mangleFieldReference(fieldName), ImmutableList.of(), ImmutableList.of(), returnType.getTypeSignature().toString(), ImmutableList.of(type.getTypeSignature().toString()));

        String stackType = returnType.getJavaType().getSimpleName().toLowerCase();
        MethodHandle methodHandle;
        if (METHOD_HANDLE_MAP.containsKey(stackType)) {
            methodHandle = METHOD_HANDLE_MAP.get(stackType);
        }
        else {
            methodHandle = METHOD_HANDLE_OBJECT;
        }
        this.methodHandle = methodHandle.bindTo(returnType).bindTo(index);
    }

    @Override
    public boolean isHidden()
    {
        return true;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public String getDescription()
    {
        return null;
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        requireNonNull(methodHandle, "methodHandle is null");
        return new ScalarFunctionImplementation(true, ImmutableList.of(false), methodHandle, isDeterministic());
    }

    @UsedByGeneratedCode
    public static Long longAccessor(Type type, Integer field, Block row)
    {
        return row.isNull(field) ? null : type.getLong(row, field);
    }

    @UsedByGeneratedCode
    public static Boolean booleanAccessor(Type type, Integer field, Block row)
    {
        return row.isNull(field) ? null : type.getBoolean(row, field);
    }

    @UsedByGeneratedCode
    public static Double doubleAccessor(Type type, Integer field, Block row)
    {
        return row.isNull(field) ? null : type.getDouble(row, field);
    }

    @UsedByGeneratedCode
    public static Slice sliceAccessor(Type type, Integer field, Block row)
    {
        return row.isNull(field) ? null : type.getSlice(row, field);
    }

    @UsedByGeneratedCode
    public static Object objectAccessor(Type type, Integer field, Block row)
    {
        return row.isNull(field) ? null : type.getObject(row, field);
    }
}
