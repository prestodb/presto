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

import com.facebook.presto.ml.type.ClassifierType;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageBuilder;
import com.facebook.presto.operator.RowPageBuilder;
import com.facebook.presto.operator.aggregation.Accumulator;
import com.facebook.presto.operator.aggregation.AggregationFunction;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.VarcharType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import javax.annotation.concurrent.Immutable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestLearnAggregations
{
    @Test
    public void testLearn()
            throws Exception
    {
        LearnAggregation aggregation = new LearnAggregation(ClassifierType.CLASSIFIER, BigintType.BIGINT);
        Accumulator accumulator = aggregation.createAggregation(Optional.<Integer>absent(), Optional.<Integer>absent(), 1.0, 0, 1);
        accumulator.addInput(getPage());
        BlockCursor cursor = accumulator.evaluateFinal().cursor();
        cursor.advanceNextPosition();
        Slice slice = cursor.getSlice();
        Model deserialized = ModelUtils.deserialize(slice);
        assertNotNull(deserialized, "deserialization failed");
        assertTrue(deserialized instanceof Classifier, "deserialized model is not a classifier");
    }

    private static Page getPage()
            throws JsonProcessingException
    {
        int datapoints = 100;
        ObjectMapper mapper = new ObjectMapper();
        RowPageBuilder builder = RowPageBuilder.rowPageBuilder(BigintType.BIGINT, VarcharType.VARCHAR);
        Random rand = new Random(0);
        for (int i = 0; i < datapoints; i++) {
            long label = rand.nextDouble() < 0.5 ? 0 : 1;
            builder.row(label, mapper.writeValueAsString(ImmutableMap.of(0, label + rand.nextGaussian())));
        }

        return builder.build();
    }
}
