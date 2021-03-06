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
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.UpdateMemory;
import com.facebook.presto.operator.aggregation.Accumulator;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.FunctionExtractor.extractFunctions;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static org.testng.Assert.assertEquals;

public class TestEvaluateClassifierPredictions
{
    private final MetadataManager metadata = MetadataManager.createTestMetadataManager();
    private final FunctionAndTypeManager functionAndTypeManager = metadata.getFunctionAndTypeManager();

    @Test
    public void testEvaluateClassifierPredictions()
    {
        metadata.registerBuiltInFunctions(extractFunctions(new MLPlugin().getFunctions()));
        InternalAggregationFunction aggregation = functionAndTypeManager.getAggregateFunctionImplementation(
                functionAndTypeManager.lookupFunction("evaluate_classifier_predictions", fromTypes(BIGINT, BIGINT)));
        Accumulator accumulator = aggregation.bind(ImmutableList.of(0, 1), Optional.empty()).createAccumulator(UpdateMemory.NOOP);
        accumulator.addInput(getPage());
        BlockBuilder finalOut = accumulator.getFinalType().createBlockBuilder(null, 1);
        accumulator.evaluateFinal(finalOut);
        Block block = finalOut.build();

        String output = VARCHAR.getSlice(block, 0).toStringUtf8();
        List<String> parts = ImmutableList.copyOf(Splitter.on('\n').omitEmptyStrings().split(output));
        assertEquals(parts.size(), 7, output);
        assertEquals(parts.get(0), "Accuracy: 1/2 (50.00%)");
    }

    private static Page getPage()
    {
        return RowPageBuilder.rowPageBuilder(BIGINT, BIGINT)
                .row(1L, 1L)
                .row(1L, 0L)
                .build();
    }
}
