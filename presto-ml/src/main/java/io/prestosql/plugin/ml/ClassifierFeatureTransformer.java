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

import io.prestosql.plugin.ml.type.ClassifierType;
import io.prestosql.plugin.ml.type.ModelType;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class ClassifierFeatureTransformer
        implements Classifier<Integer>
{
    private final Classifier<Integer> classifier;
    private final FeatureTransformation transformation;

    public ClassifierFeatureTransformer(Classifier<Integer> classifier, FeatureTransformation transformation)
    {
        this.classifier = requireNonNull(classifier, "classifier is is null");
        this.transformation = requireNonNull(transformation, "transformation is null");
    }

    @Override
    public ModelType getType()
    {
        return ClassifierType.BIGINT_CLASSIFIER;
    }

    @Override
    public byte[] getSerializedData()
    {
        return ModelUtils.serializeModels(classifier, transformation);
    }

    public static ClassifierFeatureTransformer deserialize(byte[] data)
    {
        List<Model> models = ModelUtils.deserializeModels(data);

        return new ClassifierFeatureTransformer((Classifier) models.get(0), (FeatureTransformation) models.get(1));
    }

    @Override
    public Integer classify(FeatureVector features)
    {
        return classifier.classify(transformation.transform(features));
    }

    @Override
    public void train(Dataset dataset)
    {
        transformation.train(dataset);
        classifier.train(transformation.transform(dataset));
    }
}
