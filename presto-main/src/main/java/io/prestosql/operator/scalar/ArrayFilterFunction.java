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

import io.airlift.slice.Slice;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.function.TypeParameter;
import io.prestosql.spi.function.TypeParameterSpecialization;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.gen.lambda.LambdaFunctionInterface;

import static java.lang.Boolean.TRUE;

@Description("return array containing elements that match the given predicate")
@ScalarFunction(value = "filter", deterministic = false)
public final class ArrayFilterFunction
{
    private ArrayFilterFunction() {}

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = long.class)
    @SqlType("array(T)")
    public static Block filterLong(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("function(T, boolean)") FilterLongLambda function)
    {
        int positionCount = arrayBlock.getPositionCount();
        BlockBuilder resultBuilder = elementType.createBlockBuilder(null, positionCount);
        for (int position = 0; position < positionCount; position++) {
            Long input = null;
            if (!arrayBlock.isNull(position)) {
                input = elementType.getLong(arrayBlock, position);
            }

            Boolean keep = function.apply(input);
            if (TRUE.equals(keep)) {
                elementType.appendTo(arrayBlock, position, resultBuilder);
            }
        }
        return resultBuilder.build();
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = double.class)
    @SqlType("array(T)")
    public static Block filterDouble(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("function(T, boolean)") FilterDoubleLambda function)
    {
        int positionCount = arrayBlock.getPositionCount();
        BlockBuilder resultBuilder = elementType.createBlockBuilder(null, positionCount);
        for (int position = 0; position < positionCount; position++) {
            Double input = null;
            if (!arrayBlock.isNull(position)) {
                input = elementType.getDouble(arrayBlock, position);
            }

            Boolean keep = function.apply(input);
            if (TRUE.equals(keep)) {
                elementType.appendTo(arrayBlock, position, resultBuilder);
            }
        }
        return resultBuilder.build();
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = boolean.class)
    @SqlType("array(T)")
    public static Block filterBoolean(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("function(T, boolean)") FilterBooleanLambda function)
    {
        int positionCount = arrayBlock.getPositionCount();
        BlockBuilder resultBuilder = elementType.createBlockBuilder(null, positionCount);
        for (int position = 0; position < positionCount; position++) {
            Boolean input = null;
            if (!arrayBlock.isNull(position)) {
                input = elementType.getBoolean(arrayBlock, position);
            }

            Boolean keep = function.apply(input);
            if (TRUE.equals(keep)) {
                elementType.appendTo(arrayBlock, position, resultBuilder);
            }
        }
        return resultBuilder.build();
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = Slice.class)
    @SqlType("array(T)")
    public static Block filterSlice(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("function(T, boolean)") FilterSliceLambda function)
    {
        int positionCount = arrayBlock.getPositionCount();
        BlockBuilder resultBuilder = elementType.createBlockBuilder(null, positionCount);
        for (int position = 0; position < positionCount; position++) {
            Slice input = null;
            if (!arrayBlock.isNull(position)) {
                input = elementType.getSlice(arrayBlock, position);
            }

            Boolean keep = function.apply(input);
            if (TRUE.equals(keep)) {
                elementType.appendTo(arrayBlock, position, resultBuilder);
            }
        }
        return resultBuilder.build();
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = Block.class)
    @SqlType("array(T)")
    public static Block filterBlock(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("function(T, boolean)") FilterBlockLambda function)
    {
        int positionCount = arrayBlock.getPositionCount();
        BlockBuilder resultBuilder = elementType.createBlockBuilder(null, positionCount);
        for (int position = 0; position < positionCount; position++) {
            Block input = null;
            if (!arrayBlock.isNull(position)) {
                input = (Block) elementType.getObject(arrayBlock, position);
            }

            Boolean keep = function.apply(input);
            if (TRUE.equals(keep)) {
                elementType.appendTo(arrayBlock, position, resultBuilder);
            }
        }
        return resultBuilder.build();
    }

    @FunctionalInterface
    public interface FilterLongLambda
            extends LambdaFunctionInterface
    {
        Boolean apply(Long x);
    }

    @FunctionalInterface
    public interface FilterDoubleLambda
            extends LambdaFunctionInterface
    {
        Boolean apply(Double x);
    }

    @FunctionalInterface
    public interface FilterBooleanLambda
            extends LambdaFunctionInterface
    {
        Boolean apply(Boolean x);
    }

    @FunctionalInterface
    public interface FilterSliceLambda
            extends LambdaFunctionInterface
    {
        Boolean apply(Slice x);
    }

    @FunctionalInterface
    public interface FilterBlockLambda
            extends LambdaFunctionInterface
    {
        Boolean apply(Block x);
    }

    @FunctionalInterface
    public interface FilterVoidLambda
            extends LambdaFunctionInterface
    {
        Boolean apply(Void x);
    }
}
