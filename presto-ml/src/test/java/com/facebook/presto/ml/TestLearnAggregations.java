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
package com.facebook.presto.ml;

import com.facebook.presto.RowPageBuilder;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.ml.type.ClassifierParametricType;
import com.facebook.presto.ml.type.ClassifierType;
import com.facebook.presto.ml.type.ModelType;
import com.facebook.presto.ml.type.RegressorType;
import com.facebook.presto.operator.aggregation.Accumulator;
import com.facebook.presto.operator.aggregation.AggregationCompiler;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.InterleavedBlockBuilder;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.type.TypeRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.Random;

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.testing.AggregationTestUtils.generateInternalAggregationFunction;
import static com.facebook.presto.type.TypeJsonUtils.appendToBlockBuilder;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestLearnAggregations
{
    private static final TypeManager typeManager;

    static {
        TypeRegistry typeRegistry = new TypeRegistry();
        typeRegistry.addParametricType(new ClassifierParametricType());
        typeRegistry.addType(ModelType.MODEL);
        typeRegistry.addType(RegressorType.REGRESSOR);
        typeManager = typeRegistry;
    }

    @Test
    public void testLearn()
            throws Exception
    {
        Type mapType = typeManager.getParameterizedType("map", ImmutableList.of(TypeSignatureParameter.of(parseTypeSignature(StandardTypes.BIGINT)), TypeSignatureParameter.of(parseTypeSignature(StandardTypes.DOUBLE))));
        InternalAggregationFunction aggregation = generateInternalAggregationFunction(LearnClassifierAggregation.class, ClassifierType.BIGINT_CLASSIFIER.getTypeSignature(), ImmutableList.of(BigintType.BIGINT.getTypeSignature(), mapType.getTypeSignature()), typeManager);
        assertLearnClassifer(aggregation.bind(ImmutableList.of(0, 1), Optional.empty(), Optional.empty(), 1.0).createAccumulator());
    }

    @Test
    public void testLearnLibSvm()
            throws Exception
    {
        Type mapType = typeManager.getParameterizedType("map", ImmutableList.of(TypeSignatureParameter.of(parseTypeSignature(StandardTypes.BIGINT)), TypeSignatureParameter.of(parseTypeSignature(StandardTypes.DOUBLE))));
        InternalAggregationFunction aggregation =
                AggregationCompiler.generateAggregationBindableFunction(LearnLibSvmClassifierAggregation.class, ClassifierType.BIGINT_CLASSIFIER.getTypeSignature(), ImmutableList.of(BigintType.BIGINT.getTypeSignature(), mapType.getTypeSignature(), VarcharType.getParametrizedVarcharSignature("x")))
                        .specialize(BoundVariables.builder().setLongVariable("x", (long) Integer.MAX_VALUE).build(), 1, typeManager);
        assertLearnClassifer(aggregation.bind(ImmutableList.of(0, 1, 2), Optional.empty(), Optional.empty(), 1.0).createAccumulator());
    }

    private static void assertLearnClassifer(Accumulator accumulator)
            throws Exception
    {
        accumulator.addInput(getPage());
        BlockBuilder finalOut = accumulator.getFinalType().createBlockBuilder(new BlockBuilderStatus(), 1);
        accumulator.evaluateFinal(finalOut);
        Block block = finalOut.build();
        Slice slice = accumulator.getFinalType().getSlice(block, 0);
        Model deserialized = ModelUtils.deserialize(slice);
        assertNotNull(deserialized, "deserialization failed");
        assertTrue(deserialized instanceof Classifier, "deserialized model is not a classifier");
    }

    private static Page getPage()
            throws JsonProcessingException
    {
        Type mapType = typeManager.getParameterizedType("map", ImmutableList.of(TypeSignatureParameter.of(parseTypeSignature(StandardTypes.BIGINT)), TypeSignatureParameter.of(parseTypeSignature(StandardTypes.DOUBLE))));
        int datapoints = 100;
        RowPageBuilder builder = RowPageBuilder.rowPageBuilder(BigintType.BIGINT, mapType, VarcharType.VARCHAR);
        Random rand = new Random(0);
        for (int i = 0; i < datapoints; i++) {
            long label = rand.nextDouble() < 0.5 ? 0 : 1;
            builder.row(label, mapSliceOf(BigintType.BIGINT, DoubleType.DOUBLE, 0, label + rand.nextGaussian()), "C=1");
        }

        return builder.build();
    }

    private static Block mapSliceOf(Type keyType, Type valueType, Object key, Object value)
    {
        BlockBuilder blockBuilder = new InterleavedBlockBuilder(ImmutableList.of(keyType, valueType), new BlockBuilderStatus(), 100);
        appendToBlockBuilder(keyType, key, blockBuilder);
        appendToBlockBuilder(valueType, value, blockBuilder);
        return blockBuilder.build();
    }
}
