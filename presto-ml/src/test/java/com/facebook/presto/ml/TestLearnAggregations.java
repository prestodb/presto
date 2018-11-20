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
import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.ml.type.ClassifierParametricType;
import com.facebook.presto.ml.type.ClassifierType;
import com.facebook.presto.ml.type.ModelType;
import com.facebook.presto.ml.type.RegressorType;
import com.facebook.presto.operator.aggregation.Accumulator;
import com.facebook.presto.operator.aggregation.AggregationFromAnnotationsParser;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.Random;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.testing.AggregationTestUtils.generateInternalAggregationFunction;
import static com.facebook.presto.tests.StructuralTestUtil.mapBlockOf;
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

        // associate typeRegistry with a function registry
        new FunctionRegistry(typeRegistry, new BlockEncodingManager(typeRegistry), new FeaturesConfig());

        typeManager = typeRegistry;
    }

    @Test
    public void testLearn()
    {
        Type mapType = typeManager.getParameterizedType("map", ImmutableList.of(TypeSignatureParameter.of(parseTypeSignature(StandardTypes.BIGINT)), TypeSignatureParameter.of(parseTypeSignature(StandardTypes.DOUBLE))));
        InternalAggregationFunction aggregation = generateInternalAggregationFunction(LearnClassifierAggregation.class, ClassifierType.BIGINT_CLASSIFIER.getTypeSignature(), ImmutableList.of(BIGINT.getTypeSignature(), mapType.getTypeSignature()), typeManager);
        assertLearnClassifer(aggregation.bind(ImmutableList.of(0, 1), Optional.empty()).createAccumulator());
    }

    @Test
    public void testLearnLibSvm()
    {
        Type mapType = typeManager.getParameterizedType("map", ImmutableList.of(TypeSignatureParameter.of(parseTypeSignature(StandardTypes.BIGINT)), TypeSignatureParameter.of(parseTypeSignature(StandardTypes.DOUBLE))));
        InternalAggregationFunction aggregation = AggregationFromAnnotationsParser.parseFunctionDefinitionWithTypesConstraint(
                LearnLibSvmClassifierAggregation.class,
                ClassifierType.BIGINT_CLASSIFIER.getTypeSignature(),
                ImmutableList.of(BIGINT.getTypeSignature(), mapType.getTypeSignature(), VarcharType.getParametrizedVarcharSignature("x"))
        ).specialize(BoundVariables.builder().setLongVariable("x", (long) Integer.MAX_VALUE).build(), 3, typeManager);
        assertLearnClassifer(aggregation.bind(ImmutableList.of(0, 1, 2), Optional.empty()).createAccumulator());
    }

    private static void assertLearnClassifer(Accumulator accumulator)
    {
        accumulator.addInput(getPage());
        BlockBuilder finalOut = accumulator.getFinalType().createBlockBuilder(null, 1);
        accumulator.evaluateFinal(finalOut);
        Block block = finalOut.build();
        Slice slice = accumulator.getFinalType().getSlice(block, 0);
        Model deserialized = ModelUtils.deserialize(slice);
        assertNotNull(deserialized, "deserialization failed");
        assertTrue(deserialized instanceof Classifier, "deserialized model is not a classifier");
    }

    private static Page getPage()
    {
        Type mapType = typeManager.getParameterizedType("map", ImmutableList.of(TypeSignatureParameter.of(parseTypeSignature(StandardTypes.BIGINT)), TypeSignatureParameter.of(parseTypeSignature(StandardTypes.DOUBLE))));
        int datapoints = 100;
        RowPageBuilder builder = RowPageBuilder.rowPageBuilder(BIGINT, mapType, VarcharType.VARCHAR);
        Random rand = new Random(0);
        for (int i = 0; i < datapoints; i++) {
            long label = rand.nextDouble() < 0.5 ? 0 : 1;
            builder.row(label, mapBlockOf(BIGINT, DOUBLE, 0L, label + rand.nextGaussian()), "C=1");
        }

        return builder.build();
    }
}
