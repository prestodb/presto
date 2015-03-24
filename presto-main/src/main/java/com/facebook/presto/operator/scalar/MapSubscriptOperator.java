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
import com.facebook.presto.metadata.ParametricOperator;
import com.facebook.presto.metadata.Signature;
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
import static com.facebook.presto.type.TypeUtils.castValue;
import static com.facebook.presto.type.TypeUtils.createBlock;
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

        MethodHandle methodHandle = lookupMethod(keyType, valueType);
        methodHandle = methodHandle.bindTo(keyType);
        methodHandle = methodHandle.bindTo(valueType);

        Signature signature = new Signature(SUBSCRIPT.name(), valueType.getTypeSignature(), parameterizedTypeName("map", keyType.getTypeSignature(), valueType.getTypeSignature()), keyType.getTypeSignature());
        return new FunctionInfo(signature, "Map subscript", true, methodHandle, true, true, ImmutableList.of(false, false));
    }

    private static MethodHandle lookupMethod(Type keyType, Type valueType)
    {
        String methodName = keyType.getJavaType().getSimpleName();
        methodName += valueType.getJavaType().getSimpleName();
        methodName += "Subscript";
        try {
            return lookup().unreflect(MapSubscriptOperator.class.getMethod(methodName, Type.class, Type.class, Slice.class, keyType.getJavaType()));
        }
        catch (IllegalAccessException | NoSuchMethodException e) {
            throw Throwables.propagate(e);
        }
    }

    public static void longvoidSubscript(Type keyType, Type valueType, Slice map, long key)
    {
    }

    public static void SlicevoidSubscript(Type keyType, Type valueType, Slice map, Slice key)
    {
    }

    public static void booleanvoidSubscript(Type keyType, Type valueType, Slice map, boolean key)
    {
    }

    public static void doublevoidSubscript(Type keyType, Type valueType, Slice map, double key)
    {
    }

    public static Long SlicelongSubscript(Type keyType, Type valueType, Slice map, Slice key)
    {
        return subscript(keyType, valueType, map, key);
    }

    public static Boolean SlicebooleanSubscript(Type keyType, Type valueType, Slice map, Slice key)
    {
        return subscript(keyType, valueType, map, key);
    }

    public static Double SlicedoubleSubscript(Type keyType, Type valueType, Slice map, Slice key)
    {
        return subscript(keyType, valueType, map, key);
    }

    public static Slice SliceSliceSubscript(Type keyType, Type valueType, Slice map, Slice key)
    {
        return subscript(keyType, valueType, map, key);
    }

    public static Long doublelongSubscript(Type keyType, Type valueType, Slice map, double key)
    {
        return subscript(keyType, valueType, map, key);
    }

    public static Boolean doublebooleanSubscript(Type keyType, Type valueType, Slice map, double key)
    {
        return subscript(keyType, valueType, map, key);
    }

    public static Double doubledoubleSubscript(Type keyType, Type valueType, Slice map, double key)
    {
        return subscript(keyType, valueType, map, key);
    }

    public static Slice doubleSliceSubscript(Type keyType, Type valueType, Slice map, double key)
    {
        return subscript(keyType, valueType, map, key);
    }

    public static Long booleanlongSubscript(Type keyType, Type valueType, Slice map, boolean key)
    {
        return subscript(keyType, valueType, map, key);
    }

    public static Boolean booleanbooleanSubscript(Type keyType, Type valueType, Slice map, boolean key)
    {
        return subscript(keyType, valueType, map, key);
    }

    public static Double booleandoubleSubscript(Type keyType, Type valueType, Slice map, boolean key)
    {
        return subscript(keyType, valueType, map, key);
    }

    public static Slice booleanSliceSubscript(Type keyType, Type valueType, Slice map, boolean key)
    {
        return subscript(keyType, valueType, map, key);
    }

    public static Long longlongSubscript(Type keyType, Type valueType, Slice map, long key)
    {
        return subscript(keyType, valueType, map, key);
    }

    public static Boolean longbooleanSubscript(Type keyType, Type valueType, Slice map, long key)
    {
        return subscript(keyType, valueType, map, key);
    }

    public static Double longdoubleSubscript(Type keyType, Type valueType, Slice map, long key)
    {
        return subscript(keyType, valueType, map, key);
    }

    public static Slice longSliceSubscript(Type keyType, Type valueType, Slice map, long key)
    {
        return subscript(keyType, valueType, map, key);
    }

    @SuppressWarnings("unchecked")
    private static <T> T subscript(Type keyType, Type valueType, Slice map, Object key)
    {
        Block block = readStructuralBlock(map);

        Block keyBlock = createBlock(keyType, key);

        int position = 0;
        //TODO: This could be quite slow, it should use parametric equals
        for (; position < block.getPositionCount(); position += 2) {
            if (keyType.equalTo(block, position, keyBlock, 0)) {
                break;
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
