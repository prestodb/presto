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
package io.prestosql.operator.scalar;

import com.google.common.collect.ImmutableList;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionKind;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.metadata.Signature;
import io.prestosql.metadata.SqlScalarFunction;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.SingleMapBlock;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignatureParameter;
import io.prestosql.sql.gen.lambda.LambdaFunctionInterface;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.prestosql.metadata.Signature.typeVariable;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.functionTypeArgumentProperty;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.spi.type.TypeUtils.readNativeValue;
import static io.prestosql.spi.type.TypeUtils.writeNativeValue;
import static io.prestosql.util.Reflection.methodHandle;

public final class MapZipWithFunction
        extends SqlScalarFunction
{
    public static final MapZipWithFunction MAP_ZIP_WITH_FUNCTION = new MapZipWithFunction();

    private static final MethodHandle METHOD_HANDLE = methodHandle(MapZipWithFunction.class, "mapZipWith", Type.class, Type.class, Type.class, MapType.class, Object.class, Block.class, Block.class, MapZipWithLambda.class);
    private static final MethodHandle STATE_FACTORY = methodHandle(MapZipWithFunction.class, "createState", MapType.class);

    private MapZipWithFunction()
    {
        super(new Signature(
                "map_zip_with",
                FunctionKind.SCALAR,
                ImmutableList.of(typeVariable("K"), typeVariable("V1"), typeVariable("V2"), typeVariable("V3")),
                ImmutableList.of(),
                parseTypeSignature("map(K,V3)"),
                ImmutableList.of(parseTypeSignature("map(K,V1)"), parseTypeSignature("map(K,V2)"), parseTypeSignature("function(K,V1,V2,V3)")),
                false));
    }

    @Override
    public boolean isHidden()
    {
        return false;
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
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type keyType = boundVariables.getTypeVariable("K");
        Type inputValueType1 = boundVariables.getTypeVariable("V1");
        Type inputValueType2 = boundVariables.getTypeVariable("V2");
        Type outputValueType = boundVariables.getTypeVariable("V3");
        Type outputMapType = typeManager.getParameterizedType(
                StandardTypes.MAP,
                ImmutableList.of(
                        TypeSignatureParameter.of(keyType.getTypeSignature()),
                        TypeSignatureParameter.of(outputValueType.getTypeSignature())));
        return new ScalarFunctionImplementation(
                false,
                ImmutableList.of(
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL),
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL),
                        functionTypeArgumentProperty(MapZipWithLambda.class)),
                METHOD_HANDLE.bindTo(keyType).bindTo(inputValueType1).bindTo(inputValueType2).bindTo(outputMapType),
                Optional.of(STATE_FACTORY.bindTo(outputMapType)),
                isDeterministic());
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
            Object state,
            Block leftBlock,
            Block rightBlock,
            MapZipWithLambda function)
    {
        SingleMapBlock leftMapBlock = (SingleMapBlock) leftBlock;
        SingleMapBlock rightMapBlock = (SingleMapBlock) rightBlock;
        Type outputValueType = outputMapType.getValueType();

        PageBuilder pageBuilder = (PageBuilder) state;
        if (pageBuilder.isFull()) {
            pageBuilder.reset();
        }
        BlockBuilder mapBlockBuilder = pageBuilder.getBlockBuilder(0);
        BlockBuilder blockBuilder = mapBlockBuilder.beginBlockEntry();

        // seekKey() can take non-trivial time when key is complicated value, such as a long VARCHAR or ROW.
        boolean[] keyFound = new boolean[rightMapBlock.getPositionCount()];
        for (int leftKeyPosition = 0; leftKeyPosition < leftMapBlock.getPositionCount(); leftKeyPosition += 2) {
            Object key = readNativeValue(keyType, leftMapBlock, leftKeyPosition);
            Object leftValue = readNativeValue(leftValueType, leftMapBlock, leftKeyPosition + 1);

            int rightValuePosition = rightMapBlock.seekKey(key);
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
                pageBuilder.declarePosition();

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
                    pageBuilder.declarePosition();

                    throwIfUnchecked(throwable);
                    throw new RuntimeException(throwable);
                }

                keyType.appendTo(rightMapBlock, rightKeyPosition, blockBuilder);
                writeNativeValue(outputValueType, blockBuilder, outputValue);
            }
        }

        mapBlockBuilder.closeEntry();
        pageBuilder.declarePosition();
        return outputMapType.getObject(mapBlockBuilder, mapBlockBuilder.getPositionCount() - 1);
    }

    @FunctionalInterface
    public interface MapZipWithLambda
            extends LambdaFunctionInterface
    {
        Object apply(Object key, Object value1, Object value2);
    }
}
