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

import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;

public class EvaluateClassifierPredictionsStateSerializer
        implements AccumulatorStateSerializer<EvaluateClassifierPredictionsState>
{
    private static final ObjectMapper OBJECT_MAPPER = new JsonObjectMapperProvider().get();
    private static final String TRUE_POSITIVES = "truePositives";
    private static final String FALSE_POSITIVES = "falsePositives";
    private static final String FALSE_NEGATIVES = "falseNegatives";

    @Override
    public Type getSerializedType()
    {
        return VARCHAR;
    }

    @Override
    public void serialize(EvaluateClassifierPredictionsState state, BlockBuilder out)
    {
        Map<String, Map<String, Integer>> jsonState = new HashMap<>();
        jsonState.put(TRUE_POSITIVES, state.getTruePositives());
        jsonState.put(FALSE_POSITIVES, state.getFalsePositives());
        jsonState.put(FALSE_NEGATIVES, state.getFalseNegatives());
        try {
            VARCHAR.writeSlice(out, Slices.utf8Slice(OBJECT_MAPPER.writeValueAsString(jsonState)));
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deserialize(Block block, int index, EvaluateClassifierPredictionsState state)
    {
        Slice slice = VARCHAR.getSlice(block, index);
        Map<String, Map<String, Integer>> jsonState;
        try {
            jsonState = OBJECT_MAPPER.readValue(slice.getBytes(), new TypeReference<Map<String, Map<String, Integer>>>() {});
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        state.addMemoryUsage(slice.length());
        state.getTruePositives().putAll(jsonState.getOrDefault(TRUE_POSITIVES, ImmutableMap.of()));
        state.getFalsePositives().putAll(jsonState.getOrDefault(FALSE_POSITIVES, ImmutableMap.of()));
        state.getFalseNegatives().putAll(jsonState.getOrDefault(FALSE_NEGATIVES, ImmutableMap.of()));
    }
}
