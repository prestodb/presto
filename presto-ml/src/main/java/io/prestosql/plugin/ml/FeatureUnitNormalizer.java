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

import com.google.common.collect.ImmutableSet;
import io.airlift.slice.SizeOf;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.prestosql.plugin.ml.type.ModelType;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;

import java.util.HashMap;
import java.util.Map;

/**
 * Normalizes features by making every feature value lie in [0, 1].
 */
public class FeatureUnitNormalizer
        extends AbstractFeatureTransformation
{
    private final Int2DoubleMap mins;
    private final Int2DoubleMap maxs;

    public FeatureUnitNormalizer()
    {
        mins = new Int2DoubleOpenHashMap();
        maxs = new Int2DoubleOpenHashMap();

        mins.defaultReturnValue(Double.POSITIVE_INFINITY);
        maxs.defaultReturnValue(Double.NEGATIVE_INFINITY);
    }

    @Override
    public ModelType getType()
    {
        return ModelType.MODEL;
    }

    @Override
    public byte[] getSerializedData()
    {
        // Serialization format is (<key:int><min:double><max:double>)*
        SliceOutput output = Slices.allocate((SizeOf.SIZE_OF_INT + 2 * SizeOf.SIZE_OF_DOUBLE) * mins.size()).getOutput();
        for (int key : mins.keySet()) {
            output.appendInt(key);
            output.appendDouble(mins.get(key));
            output.appendDouble(maxs.get(key));
        }
        return output.slice().getBytes();
    }

    public static FeatureUnitNormalizer deserialize(byte[] modelData)
    {
        SliceInput input = Slices.wrappedBuffer(modelData).getInput();
        FeatureUnitNormalizer model = new FeatureUnitNormalizer();
        while (input.isReadable()) {
            int key = input.readInt();
            model.mins.put(key, input.readDouble());
            model.maxs.put(key, input.readDouble());
        }
        return model;
    }

    @Override
    public void train(Dataset dataset)
    {
        for (FeatureVector vector : dataset.getDatapoints()) {
            for (Map.Entry<Integer, Double> feature : vector.getFeatures().entrySet()) {
                int key = feature.getKey();
                double value = feature.getValue();
                if (value < mins.get(key)) {
                    mins.put(key, value);
                }
                if (value > maxs.get(key)) {
                    maxs.put(key, value);
                }
            }
        }

        for (int key : ImmutableSet.copyOf(mins.keySet())) {
            // Remove any features that had a constant value
            if (mins.get(key) == maxs.get(key)) {
                mins.remove(key);
                maxs.remove(key);
            }
        }
    }

    @Override
    public FeatureVector transform(FeatureVector features)
    {
        Map<Integer, Double> transformed = new HashMap<>();
        for (Map.Entry<Integer, Double> entry : features.getFeatures().entrySet()) {
            int key = entry.getKey();
            double value = entry.getValue();
            if (mins.containsKey(entry.getKey())) {
                double min = mins.get(key);
                value = (value - min) / (maxs.get(key) - min);
            }
            else {
                // Set anything that had a constant value, or was missing, in the training set to zero
                value = 0;
            }
            // In case value is outside of the values seen in the training data, make sure it's [0, 1]
            value = Math.min(1, Math.max(0, value));
            transformed.put(entry.getKey(), value);
        }
        return new FeatureVector(transformed);
    }
}
