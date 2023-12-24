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
package com.facebook.presto.operator.aggregation.sketch.theta;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.aggregation.AccumulatorCompiler;
import com.facebook.presto.operator.aggregation.BuiltInAggregationFunctionImplementation;
import com.facebook.presto.operator.aggregation.state.StateCompiler;
import com.facebook.presto.spi.function.aggregation.Accumulator;
import com.facebook.presto.spi.function.aggregation.AggregationMetadata;
import com.facebook.presto.spi.function.aggregation.GroupedAccumulator;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.function.Signature.typeVariable;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.NULLABLE_BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class ThetaSketchAggregationFunction
        extends SqlAggregationFunction
{
    private static final Logger log = Logger.get(ThetaSketchAggregationFunction.class);
    public static final String NAME = "sketch_theta";

    public static final ThetaSketchAggregationFunction THETA_SKETCH = new ThetaSketchAggregationFunction();

    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(ThetaSketchAggregationFunction.class, "output", ThetaSketchAggregationState.class, BlockBuilder.class);
    private static final MethodHandle INPUT_FUNCTION = methodHandle(ThetaSketchAggregationFunction.class, "input", Type.class, ThetaSketchAggregationState.class, Block.class, int.class);
    private static final MethodHandle MERGE_FUNCTION = methodHandle(ThetaSketchAggregationFunction.class, "merge", ThetaSketchAggregationState.class, ThetaSketchAggregationState.class);

    public ThetaSketchAggregationFunction()
    {
        super(NAME,
                ImmutableList.of(typeVariable("T")),
                ImmutableList.of(),
                parseTypeSignature(StandardTypes.VARBINARY),
                ImmutableList.of(parseTypeSignature("T")));
    }

    @Override
    public String getDescription()
    {
        return "calculates a theta sketch of the selected input column";
    }

    @Override
    public BuiltInAggregationFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(ThetaSketchAggregationFunction.class.getClassLoader());
        Type type = boundVariables.getTypeVariable("T");
        List<Type> inputTypes = ImmutableList.of(type);

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, type.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                createInputParameterMetadata(type),
                INPUT_FUNCTION.bindTo(type),
                MERGE_FUNCTION,
                OUTPUT_FUNCTION,
                ImmutableList.of(new AggregationMetadata.AccumulatorStateDescriptor(
                        ThetaSketchAggregationState.class,
                        StateCompiler.generateStateSerializer(ThetaSketchAggregationState.class, classLoader),
                        StateCompiler.generateStateFactory(ThetaSketchAggregationState.class, classLoader))),
                VARBINARY);

        Class<? extends Accumulator> accumulatorClass = AccumulatorCompiler.generateAccumulatorClass(
                Accumulator.class,
                metadata,
                classLoader);
        Class<? extends GroupedAccumulator> groupedAccumulatorClass = AccumulatorCompiler.generateAccumulatorClass(
                GroupedAccumulator.class,
                metadata,
                classLoader);
        return new BuiltInAggregationFunctionImplementation(NAME, inputTypes, ImmutableList.of(BIGINT), VARBINARY,
                true, false, metadata, accumulatorClass, groupedAccumulatorClass);
    }

    private static List<AggregationMetadata.ParameterMetadata> createInputParameterMetadata(Type type)
    {
        return ImmutableList.of(new AggregationMetadata.ParameterMetadata(STATE), new AggregationMetadata.ParameterMetadata(NULLABLE_BLOCK_INPUT_CHANNEL, type), new AggregationMetadata.ParameterMetadata(BLOCK_INDEX));
    }

    public static void input(Type type, ThetaSketchAggregationState state, Block block, int position)
    {
        if (block.isNull(position)) {
            return;
        }
        if (type.getJavaType().equals(Long.class) || type.getJavaType() == long.class) {
            state.getSketch().update(type.getLong(block, position));
        }
        else if (type.getJavaType().equals(Double.class) || type.getJavaType() == double.class) {
            state.getSketch().update(type.getDouble(block, position));
        }
        else if (type.getJavaType().equals(String.class) || type.getJavaType().equals(Slice.class)) {
            state.getSketch().update(type.getSlice(block, position).getBytes());
        }
        else {
            throw new RuntimeException("unsupported sketch column type: " + type + " (java type: " + type.getJavaType() + ")");
        }
    }

    public static void merge(ThetaSketchAggregationState state, ThetaSketchAggregationState otherState)
    {
        state.getSketch().union(otherState.getSketch().getResult());
    }

    public static void output(ThetaSketchAggregationState state, BlockBuilder out)
    {
        Slice output = Slices.wrappedBuffer(state.getSketch().getResult().toByteArray());
        VARBINARY.writeSlice(out, output);
    }
}
