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

import io.prestosql.plugin.ml.type.ModelType;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Normalizes features by making every feature vector unit length.
 * <p>
 * NOTE: This is generally not a good way to normalize features, and is mainly provided as an example.
 */
public class FeatureVectorUnitNormalizer
        extends AbstractFeatureTransformation
{
    @Override
    public ModelType getType()
    {
        return ModelType.MODEL;
    }

    @Override
    public byte[] getSerializedData()
    {
        // This transformation has no state
        return new byte[0];
    }

    public static FeatureVectorUnitNormalizer deserialize(byte[] modelData)
    {
        checkArgument(modelData.length == 0, "modelData should be empty");
        return new FeatureVectorUnitNormalizer();
    }

    @Override
    public void train(Dataset dataset)
    {
        // Do nothing, since this transformation is stateless
    }

    @Override
    public FeatureVector transform(FeatureVector features)
    {
        double sumSquares = 0;
        for (Double value : features.getFeatures().values()) {
            sumSquares += value * value;
        }
        double magnitude = Math.sqrt(sumSquares);
        Map<Integer, Double> transformed = new HashMap<>();
        for (Map.Entry<Integer, Double> entry : features.getFeatures().entrySet()) {
            transformed.put(entry.getKey(), entry.getValue() / magnitude);
        }
        return new FeatureVector(transformed);
    }
}
