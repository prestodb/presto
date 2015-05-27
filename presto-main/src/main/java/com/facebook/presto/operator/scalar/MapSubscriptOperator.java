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

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.OperatorType;
import com.facebook.presto.metadata.ParametricOperator;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.metadata.OperatorType.SUBSCRIPT;
import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;
import static com.facebook.presto.type.TypeUtils.castValue;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static com.facebook.presto.type.TypeUtils.readStructuralBlock;
import static java.lang.invoke.MethodHandles.lookup;

public class MapSubscriptOperator
        extends ParametricOperator
{
    public static final MapSubscriptOperator MAP_SUBSCRIPT = new MapSubscriptOperator();

    protected MapSubscriptOperator()
    {
        super(SUBSCRIPT, ImmutableList.of(typeParameter("K"), typeParameter("V")), "V", ImmutableList.of("map<K,V>", "K"));
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type keyType = types.get("K");
        Type valueType = types.get("V");

        MethodHandle keyEqualsMethod = functionRegistry.resolveOperator(OperatorType.EQUAL, ImmutableList.of(keyType, keyType)).getMethodHandle();

        MethodHandle methodHandle = lookupMethod(keyType, valueType);
        methodHandle = methodHandle.bindTo(keyEqualsMethod).bindTo(keyType).bindTo(valueType);

        Signature signature = new Signature(SUBSCRIPT.name(), valueType.getTypeSignature(), parameterizedTypeName("map", keyType.getTypeSignature(), valueType.getTypeSignature()), keyType.getTypeSignature());
        return new FunctionInfo(signature, "Map subscript", true, methodHandle, true, true, ImmutableList.of(false, false));
    }

    private static MethodHandle lookupMethod(Type keyType, Type valueType)
    {
        String methodName = keyType.getJavaType().getSimpleName();
        methodName += valueType.getJavaType().getSimpleName();
        methodName += "Subscript";
        try {
            return lookup().unreflect(MapSubscriptOperator.class.getMethod(methodName, MethodHandle.class, Type.class, Type.class, Slice.class, keyType.getJavaType()));
        }
        catch (IllegalAccessException | NoSuchMethodException e) {
            throw Throwables.propagate(e);
        }
    }

    public static void longvoidSubscript(MethodHandle keyEqualsMethod, Type keyType, Type valueType, Slice map, long key)
    {
    }

    public static void SlicevoidSubscript(MethodHandle keyEqualsMethod, Type keyType, Type valueType, Slice map, Slice key)
    {
    }

    public static void booleanvoidSubscript(MethodHandle keyEqualsMethod, Type keyType, Type valueType, Slice map, boolean key)
    {
    }

    public static void doublevoidSubscript(MethodHandle keyEqualsMethod, Type keyType, Type valueType, Slice map, double key)
    {
    }

    public static Long SlicelongSubscript(MethodHandle keyEqualsMethod, Type keyType, Type valueType, Slice map, Slice key)
    {
        return subscript(keyEqualsMethod, keyType, valueType, map, key);
    }

    public static Boolean SlicebooleanSubscript(MethodHandle keyEqualsMethod, Type keyType, Type valueType, Slice map, Slice key)
    {
        return subscript(keyEqualsMethod, keyType, valueType, map, key);
    }

    public static Double SlicedoubleSubscript(MethodHandle keyEqualsMethod, Type keyType, Type valueType, Slice map, Slice key)
    {
        return subscript(keyEqualsMethod, keyType, valueType, map, key);
    }

    public static Slice SliceSliceSubscript(MethodHandle keyEqualsMethod, Type keyType, Type valueType, Slice map, Slice key)
    {
        return subscript(keyEqualsMethod, keyType, valueType, map, key);
    }

    public static Long doublelongSubscript(MethodHandle keyEqualsMethod, Type keyType, Type valueType, Slice map, double key)
    {
        return subscript(keyEqualsMethod, keyType, valueType, map, key);
    }

    public static Boolean doublebooleanSubscript(MethodHandle keyEqualsMethod, Type keyType, Type valueType, Slice map, double key)
    {
        return subscript(keyEqualsMethod, keyType, valueType, map, key);
    }

    public static Double doubledoubleSubscript(MethodHandle keyEqualsMethod, Type keyType, Type valueType, Slice map, double key)
    {
        return subscript(keyEqualsMethod, keyType, valueType, map, key);
    }

    public static Slice doubleSliceSubscript(MethodHandle keyEqualsMethod, Type keyType, Type valueType, Slice map, double key)
    {
        return subscript(keyEqualsMethod, keyType, valueType, map, key);
    }

    public static Long booleanlongSubscript(MethodHandle keyEqualsMethod, Type keyType, Type valueType, Slice map, boolean key)
    {
        return subscript(keyEqualsMethod, keyType, valueType, map, key);
    }

    public static Boolean booleanbooleanSubscript(MethodHandle keyEqualsMethod, Type keyType, Type valueType, Slice map, boolean key)
    {
        return subscript(keyEqualsMethod, keyType, valueType, map, key);
    }

    public static Double booleandoubleSubscript(MethodHandle keyEqualsMethod, Type keyType, Type valueType, Slice map, boolean key)
    {
        return subscript(keyEqualsMethod, keyType, valueType, map, key);
    }

    public static Slice booleanSliceSubscript(MethodHandle keyEqualsMethod, Type keyType, Type valueType, Slice map, boolean key)
    {
        return subscript(keyEqualsMethod, keyType, valueType, map, key);
    }

    public static Long longlongSubscript(MethodHandle keyEqualsMethod, Type keyType, Type valueType, Slice map, long key)
    {
        return subscript(keyEqualsMethod, keyType, valueType, map, key);
    }

    public static Boolean longbooleanSubscript(MethodHandle keyEqualsMethod, Type keyType, Type valueType, Slice map, long key)
    {
        return subscript(keyEqualsMethod, keyType, valueType, map, key);
    }

    public static Double longdoubleSubscript(MethodHandle keyEqualsMethod, Type keyType, Type valueType, Slice map, long key)
    {
        return subscript(keyEqualsMethod, keyType, valueType, map, key);
    }

    public static Slice longSliceSubscript(MethodHandle keyEqualsMethod, Type keyType, Type valueType, Slice map, long key)
    {
        return subscript(keyEqualsMethod, keyType, valueType, map, key);
    }

    @SuppressWarnings({"unchecked", "TypeParameterUnusedInFormals"})
    private static <T> T subscript(MethodHandle keyEqualsMethod, Type keyType, Type valueType, Slice map, Object key)
    {
        Block block = readStructuralBlock(map);

        int position = 0;

        Class<?> keyTypeJavaType = keyType.getJavaType();

        for (; position < block.getPositionCount(); position += 2) {
            try {
                boolean equals;
                if (keyTypeJavaType == long.class) {
                    equals = (boolean) keyEqualsMethod.invokeExact(keyType.getLong(block, position), (long) key);
                }
                else if (keyTypeJavaType == double.class) {
                    equals = (boolean) keyEqualsMethod.invokeExact(keyType.getDouble(block, position), (double) key);
                }
                else if (keyTypeJavaType == boolean.class) {
                    equals = (boolean) keyEqualsMethod.invokeExact(keyType.getBoolean(block, position), (boolean) key);
                }
                else if (keyTypeJavaType == Slice.class) {
                    equals = (boolean) keyEqualsMethod.invokeExact(keyType.getSlice(block, position), (Slice) key);
                }
                else {
                    throw new IllegalArgumentException("Unsupported type: " + keyTypeJavaType.getSimpleName());
                }

                if (equals) {
                    break;
                }
            }
            catch (Throwable t) {
                    Throwables.propagateIfInstanceOf(t, Error.class);
                    Throwables.propagateIfInstanceOf(t, PrestoException.class);
                    throw new PrestoException(INTERNAL_ERROR, t);
                }
            }

        if (position == block.getPositionCount()) {
            // key not found
            return null;
        }

        position += 1; // value position

        return (T) castValue(valueType, block, position);
    }
}
