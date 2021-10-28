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

import com.facebook.presto.common.NotSupportedException;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.SingleMapBlock;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunctionVisibility;
import com.facebook.presto.sql.gen.lambda.LambdaFunctionInterface;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.block.MethodHandleUtil.compose;
import static com.facebook.presto.common.block.MethodHandleUtil.nativeValueGetter;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.TypeUtils.readNativeValue;
import static com.facebook.presto.common.type.TypeUtils.writeNativeValue;
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.DEFAULT_NAMESPACE;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.ArgumentProperty.functionTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.function.Signature.typeVariable;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.PUBLIC;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.lang.Math.max;

public final class MapZipWithFunction
        extends SqlScalarFunction
{
    public static final MapZipWithFunction MAP_ZIP_WITH_FUNCTION = new MapZipWithFunction();

    private static final MethodHandle METHOD_HANDLE = methodHandle(MapZipWithFunction.class, "mapZipWith", Type.class, Type.class, Type.class, MapType.class, MethodHandle.class, MethodHandle.class, MethodHandle.class, Block.class, Block.class, MapZipWithLambda.class);
    private MapZipWithFunction()
    {
        super(new Signature(
                QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, "map_zip_with"),
                FunctionKind.SCALAR,
                ImmutableList.of(typeVariable("K"), typeVariable("V1"), typeVariable("V2"), typeVariable("V3")),
                ImmutableList.of(),
                parseTypeSignature("map(K,V3)"),
                ImmutableList.of(parseTypeSignature("map(K,V1)"), parseTypeSignature("map(K,V2)"), parseTypeSignature("function(K,V1,V2,V3)")),
                false));
    }

    @Override
    public SqlFunctionVisibility getVisibility()
    {
        return PUBLIC;
    }

    @Override
    public boolean isDeterministic()
    {
        return false;
    }

    @Override
    public String getDescription()
    {
        return "merge two maps into a single map by applying the lambda function to the pair of values with the same key";
    }

    @Override
    public BuiltInScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        Type keyType = boundVariables.getTypeVariable("K");
        Type inputValueType1 = boundVariables.getTypeVariable("V1");
        Type inputValueType2 = boundVariables.getTypeVariable("V2");
        Type outputValueType = boundVariables.getTypeVariable("V3");
        Type outputMapType = functionAndTypeManager.getParameterizedType(
                StandardTypes.MAP,
                ImmutableList.of(
                        TypeSignatureParameter.of(keyType.getTypeSignature()),
                        TypeSignatureParameter.of(outputValueType.getTypeSignature())));
        MethodHandle keyNativeHashCode = functionAndTypeManager.getBuiltInScalarFunctionImplementation(functionAndTypeManager.resolveOperator(OperatorType.HASH_CODE, fromTypes(keyType))).getMethodHandle();
        MethodHandle keyBlockHashCode = compose(keyNativeHashCode, nativeValueGetter(keyType));
        MethodHandle keyNativeEquals = functionAndTypeManager.getBuiltInScalarFunctionImplementation(functionAndTypeManager.resolveOperator(OperatorType.EQUAL, fromTypes(keyType, keyType))).getMethodHandle();
        MethodHandle keyBlockNativeEquals = compose(keyNativeEquals, nativeValueGetter(keyType));
        MethodHandle keyBlockEquals = compose(keyNativeEquals, nativeValueGetter(keyType), nativeValueGetter(keyType));

        return new BuiltInScalarFunctionImplementation(
                false,
                ImmutableList.of(
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL),
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL),
                        functionTypeArgumentProperty(MapZipWithLambda.class)),
                METHOD_HANDLE.bindTo(keyType).bindTo(inputValueType1).bindTo(inputValueType2).bindTo(outputMapType).bindTo(keyNativeHashCode).bindTo(keyBlockNativeEquals).bindTo(keyBlockHashCode));
    }

    public static Object createState(MapType mapType)
    {
        return new PageBuilder(ImmutableList.of(mapType));
    }

    public static Block mapZipWith(
            Type keyType,
            Type leftValueType,
            Type rightValueType,
            MapType outputMapType,
            MethodHandle keyNativeHashCode,
            MethodHandle keyBlockNativeEquals,
            MethodHandle keyBlockHashCode,
            Block leftBlock,
            Block rightBlock,
            MapZipWithLambda function)
    {
        SingleMapBlock leftMapBlock = (SingleMapBlock) leftBlock;
        SingleMapBlock rightMapBlock = (SingleMapBlock) rightBlock;
        Type outputValueType = outputMapType.getValueType();
        BlockBuilder mapBlockBuilder = outputMapType.createBlockBuilder(null, max(leftMapBlock.getPositionCount(), rightMapBlock.getPositionCount()) / 2);
        BlockBuilder blockBuilder = mapBlockBuilder.beginBlockEntry();

        // seekKey() can take non-trivial time when key is complicated value, such as a long VARCHAR or ROW.
        boolean[] keyFound = new boolean[rightMapBlock.getPositionCount()];
        for (int leftKeyPosition = 0; leftKeyPosition < leftMapBlock.getPositionCount(); leftKeyPosition += 2) {
            Object key = readNativeValue(keyType, leftMapBlock, leftKeyPosition);
            Object leftValue = readNativeValue(leftValueType, leftMapBlock, leftKeyPosition + 1);

            int rightValuePosition;
            try {
                rightValuePosition = rightMapBlock.seekKey(key, keyNativeHashCode, keyBlockNativeEquals, keyBlockHashCode);
            }
            catch (NotSupportedException e) {
                throw new PrestoException(NOT_SUPPORTED, e.getMessage(), e);
            }

            Object rightValue = null;
            if (rightValuePosition != -1) {
                rightValue = readNativeValue(rightValueType, rightMapBlock, rightValuePosition);
                keyFound[rightValuePosition / 2] = true;
            }

            Object outputValue;
            try {
                outputValue = function.apply(key, leftValue, rightValue);
            }
            catch (Throwable throwable) {
                // Restore pageBuilder into a consistent state.
                mapBlockBuilder.closeEntry();

                throwIfUnchecked(throwable);
                throw new RuntimeException(throwable);
            }

            keyType.appendTo(leftMapBlock, leftKeyPosition, blockBuilder);
            writeNativeValue(outputValueType, blockBuilder, outputValue);
        }

        // iterate over keys that only exists in rightMapBlock
        for (int rightKeyPosition = 0; rightKeyPosition < rightMapBlock.getPositionCount(); rightKeyPosition += 2) {
            if (!keyFound[rightKeyPosition / 2]) {
                Object key = readNativeValue(keyType, rightMapBlock, rightKeyPosition);
                Object rightValue = readNativeValue(rightValueType, rightMapBlock, rightKeyPosition + 1);

                Object outputValue;
                try {
                    outputValue = function.apply(key, null, rightValue);
                }
                catch (Throwable throwable) {
                    // Restore pageBuilder into a consistent state.
                    mapBlockBuilder.closeEntry();

                    throwIfUnchecked(throwable);
                    throw new RuntimeException(throwable);
                }

                keyType.appendTo(rightMapBlock, rightKeyPosition, blockBuilder);
                writeNativeValue(outputValueType, blockBuilder, outputValue);
            }
        }

        mapBlockBuilder.closeEntry();
        return outputMapType.getObject(mapBlockBuilder, mapBlockBuilder.getPositionCount() - 1);
    }

    @FunctionalInterface
    public interface MapZipWithLambda
            extends LambdaFunctionInterface
    {
        Object apply(Object key, Object value1, Object value2);
    }
}
