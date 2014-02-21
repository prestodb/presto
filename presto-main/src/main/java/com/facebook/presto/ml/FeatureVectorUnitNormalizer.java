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

import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.ml.ModelType.CLASSIFIER;
import static com.facebook.presto.ml.ModelType.REGRESSOR;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * Normalizes features by making every feature vector unit length.
 *
 * NOTE: This is generally not a good way to normalize features, and is mainly provided as an example.
 */
public class FeatureVectorUnitNormalizer
        extends FeatureTransformer
{
    public FeatureVectorUnitNormalizer(Classifier classifier)
    {
        super(classifier);
    }

    public FeatureVectorUnitNormalizer(Regressor regressor)
    {
        super(regressor);
    }

    public static FeatureVectorUnitNormalizer deserialize(byte[] modelData)
    {
        Model model = ModelUtils.deserialize(modelData);
        if (model.getType() == CLASSIFIER) {
            checkArgument(model instanceof Classifier, "model is not a classifier");
            return new FeatureVectorUnitNormalizer((Classifier) model);
        }
        if (model.getType() == REGRESSOR) {
            checkArgument(model instanceof Regressor, "model is not a regressor");
            return new FeatureVectorUnitNormalizer((Regressor) model);
        }
        throw new IllegalArgumentException(String.format("Unsupported model type %s", model.getType()));
    }

    @Override
    public byte[] getSerializedData()
    {
        return ModelUtils.serialize(getDelegate()).getBytes();
    }

    @Override
    protected FeatureVector transform(FeatureVector features)
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
