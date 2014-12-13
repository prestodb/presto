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
import com.facebook.presto.metadata.ParametricScalar;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.TypeParameter;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.VariableWidthBlockBuilder;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.metadata.Signature.comparableTypeParameter;
import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.StandardTypes.MAP;
import static com.facebook.presto.type.TypeUtils.buildStructuralSlice;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static com.facebook.presto.type.TypeUtils.readStructuralBlock;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.facebook.presto.util.Reflection.methodHandle;

public final class MapConstructor
        extends ParametricScalar
{
    public static final MapConstructor MAP_CONSTRUCTOR = new MapConstructor();

    private static final Signature SIGNATURE = new Signature("map", ImmutableList.of(comparableTypeParameter("K"), typeParameter("V")), "map<K,V>", ImmutableList.of("array<K>", "array<V>"), false, false);
    private static final MethodHandle METHOD_HANDLE = methodHandle(MapConstructor.class, "createMap", Type.class, Type.class, Slice.class, Slice.class);
    private static final String DESCRIPTION = "Constructs a map from the given key/value arrays";

    @Override
    public Signature getSignature()
    {
        return SIGNATURE;
    }

    @Override
    public boolean isHidden()
    {
        return false;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public String getDescription()
    {
        return DESCRIPTION;
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type keyType = types.get("K");
        Type valueType = types.get("V");

        MethodHandle methodHandle = METHOD_HANDLE.bindTo(keyType);
        methodHandle = methodHandle.bindTo(valueType);

        Type mapType = typeManager.getParameterizedType(MAP, ImmutableList.of(keyType.getTypeSignature(), valueType.getTypeSignature()), ImmutableList.of());
        ImmutableList<TypeSignature> argumentTypes = ImmutableList.of(parameterizedTypeName(StandardTypes.ARRAY, keyType.getTypeSignature()), parameterizedTypeName(StandardTypes.ARRAY, valueType.getTypeSignature()));
        Signature signature = new Signature("map", ImmutableList.<TypeParameter>of(), mapType.getTypeSignature(), argumentTypes, false, false);

        return new FunctionInfo(signature, DESCRIPTION, isHidden(), methodHandle, isDeterministic(), false, ImmutableList.of(false, false));
    }

    public static Slice createMap(Type keyType, Type valueType, Slice keys, Slice values)
    {
        Block keyBlock = readStructuralBlock(keys);
        Block valueBlock = readStructuralBlock(values);
        BlockBuilder blockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus(), keyBlock.getSizeInBytes() + valueBlock.getSizeInBytes());

        checkCondition(keyBlock.getPositionCount() == valueBlock.getPositionCount(), INVALID_FUNCTION_ARGUMENT, "Key and value arrays must be the same length");
        for (int i = 0; i < keyBlock.getPositionCount(); i++) {
            if (keyBlock.isNull(i)) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "map key cannot be null");
            }
            keyType.appendTo(keyBlock, i, blockBuilder);
            valueType.appendTo(valueBlock, i, blockBuilder);
        }

        return buildStructuralSlice(blockBuilder);
    }
}
