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

import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.RowPageBuilder;
import com.facebook.presto.operator.aggregation.Accumulator;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.spi.block.Block;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;

public class TestEvaluateClassifierPredictions
{
    @Test
    public void testEvaluateClassifierPredictions()
            throws Exception
    {
        MetadataManager metadata = new MetadataManager();
        metadata.addFunctions(new MLFunctionFactory().listFunctions());
        InternalAggregationFunction aggregation = metadata.getExactFunction(new Signature("evaluate_classifier_predictions", VARCHAR, BIGINT, BIGINT)).getAggregationFunction();
        Accumulator accumulator = aggregation.createAggregation(Optional.<Integer>absent(), Optional.<Integer>absent(), 1.0, 0, 1);
        accumulator.addInput(getPage());
        Block block = accumulator.evaluateFinal();

        String output = VARCHAR.getSlice(block, 0).toStringUtf8();
        List<String> parts = ImmutableList.copyOf(Splitter.on('\n').split(output));
        assertEquals(parts.size(), 3);
        assertEquals(parts.get(0), "Accuracy: 1/2 (50.00%)");
        assertEquals(parts.get(1), "Precision: 1/1 (100.00%)");
        assertEquals(parts.get(2), "Recall: 1/2 (50.00%)");
    }

    private static Page getPage()
            throws JsonProcessingException
    {
        return RowPageBuilder.rowPageBuilder(BIGINT, BIGINT)
                .row(1, 1)
                .row(1, 0)
                .build();
    }
}
