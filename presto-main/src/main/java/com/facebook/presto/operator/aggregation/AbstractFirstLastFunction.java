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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.aggregation.state.BlockState;
import com.facebook.presto.operator.aggregation.state.StateCompiler;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.function.Signature.typeVariable;
import static com.facebook.presto.util.Reflection.methodHandle;

public abstract class AbstractFirstLastFunction
        extends SqlAggregationFunction
{
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(AbstractFirstLastFunction.class, "output", Type.class, BlockState.class, BlockBuilder.class);

    protected AbstractFirstLastFunction(List<TypeSignature> argumentTypes, String name)
    {
        super(name,
                ImmutableList.of(typeVariable("T")),
                ImmutableList.of(),
                parseTypeSignature("T"),
                argumentTypes);
    }

    @Override
    public boolean isDeterministic()
    {
        return false;
    }

    @Override
    public InternalAggregationFunction specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        Type type = boundVariables.getTypeVariable("T");
        return generateAggregation(type);
    }

    private InternalAggregationFunction generateAggregation(Type type)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(this.getClass().getClassLoader());

        AccumulatorStateSerializer<BlockState> stateSerializer = new InteranlBlockStateSerializer(type);
        AccumulatorStateFactory<BlockState> stateFactory = StateCompiler.generateStateFactory(BlockState.class, ImmutableMap.of("T", type), classLoader);
        Type intermediateType = stateSerializer.getSerializedType();

        List<AggregationMetadata.ParameterMetadata> inputParameterMetadata = getInputParameterMetadata(type);

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(getSignature().getNameSuffix(), type.getTypeSignature(), ImmutableList.of(type.getTypeSignature(), parseTypeSignature(StandardTypes.BOOLEAN))),
                inputParameterMetadata,
                getInputFunction(),
                getCombineFunction(),
                OUTPUT_FUNCTION.bindTo(type),
                ImmutableList.of(new AggregationMetadata.AccumulatorStateDescriptor(
                        BlockState.class,
                        stateSerializer,
                        stateFactory)),
                type);

        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(getSignature().getNameSuffix(), getInputTypes(type), ImmutableList.of(intermediateType), type, true, false, factory);
    }

    protected abstract List<Type> getInputTypes(Type type);

    protected abstract List<AggregationMetadata.ParameterMetadata> getInputParameterMetadata(Type type);

    protected abstract MethodHandle getInputFunction();

    protected abstract MethodHandle getCombineFunction();

    public static void combine(BlockState state, BlockState otherState)
    {
        if (state.getBlock() == null && otherState.getBlock() != null) {
            state.setBlock(otherState.getBlock());
        }
    }

    public static void output(Type type, BlockState state, BlockBuilder out)
    {
        if (state.getBlock() == null) {
            out.appendNull();
        }
        else {
            type.appendTo(state.getBlock(), 0, out);
        }
    }

    static class InteranlBlockStateSerializer
            implements AccumulatorStateSerializer<BlockState>
    {
        private final Type type;

        public InteranlBlockStateSerializer(Type type)
        {
            this.type = type;
        }

        @Override
        public Type getSerializedType()
        {
            return type;
        }

        @Override
        public void serialize(BlockState state, BlockBuilder out)
        {
            if (state.getBlock() == null) {
                out.appendNull();
            }
            else {
                type.appendTo(state.getBlock(), 0, out);
            }
        }

        @Override
        public void deserialize(Block block, int index, BlockState state)
        {
            state.setBlock(block.getSingleValueBlock(index));
        }
    }
}
