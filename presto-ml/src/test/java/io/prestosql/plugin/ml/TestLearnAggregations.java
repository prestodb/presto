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
package io.prestosql.plugin.ml;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.prestosql.RowPageBuilder;
import io.prestosql.block.BlockEncodingManager;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.operator.aggregation.Accumulator;
import io.prestosql.operator.aggregation.AggregationFromAnnotationsParser;
import io.prestosql.operator.aggregation.InternalAggregationFunction;
import io.prestosql.plugin.ml.type.ClassifierParametricType;
import io.prestosql.plugin.ml.type.ClassifierType;
import io.prestosql.plugin.ml.type.ModelType;
import io.prestosql.plugin.ml.type.RegressorType;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignatureParameter;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.type.TypeRegistry;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.Random;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.testing.AggregationTestUtils.generateInternalAggregationFunction;
import static io.prestosql.tests.StructuralTestUtil.mapBlockOf;
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
