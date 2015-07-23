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

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.ParametricOperator;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.metadata.FunctionRegistry.operatorInfo;
import static com.facebook.presto.metadata.OperatorType.EQUAL;
import static com.facebook.presto.metadata.Signature.comparableWithVariadicBound;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.util.Reflection.methodHandle;

public class RowEqualOperator
        extends ParametricOperator
{
    public static final RowEqualOperator ROW_EQUAL = new RowEqualOperator();
    private static final TypeSignature RETURN_TYPE = parseTypeSignature(StandardTypes.BOOLEAN);
    private static final MethodHandle METHOD_HANDLE = methodHandle(RowEqualOperator.class, "equals", Type.class, Block.class, Block.class);

    private RowEqualOperator()
    {
        super(EQUAL, ImmutableList.of(comparableWithVariadicBound("T", "row")), StandardTypes.BOOLEAN, ImmutableList.of("T", "T"));
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type type = types.get("T");
        TypeSignature typeSignature = type.getTypeSignature();
        return operatorInfo(EQUAL, RETURN_TYPE, ImmutableList.of(typeSignature, typeSignature), METHOD_HANDLE.bindTo(type), false, ImmutableList.of(false, false));
    }

    public static boolean equals(Type rowType, Block leftRow, Block rightRow)
    {
        // TODO: Fix this. It feels very inefficient and unnecessary to wrap and unwrap with Block
        BlockBuilder leftBlockBuilder = rowType.createBlockBuilder(new BlockBuilderStatus(), 1);
        BlockBuilder rightBlockBuilder = rowType.createBlockBuilder(new BlockBuilderStatus(), 1);
        rowType.writeObject(leftBlockBuilder, leftRow);
        rowType.writeObject(rightBlockBuilder, rightRow);
        return rowType.equalTo(leftBlockBuilder.build(), 0, rightBlockBuilder.build(), 0);
    }
}
