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

import com.facebook.presto.ml.type.ModelType;
import com.facebook.presto.ml.type.RegressorType;

import java.util.List;

import static com.facebook.presto.ml.Types.checkType;
import static java.util.Objects.requireNonNull;

public class RegressorFeatureTransformer
        implements Regressor
{
    private final Regressor regressor;
    private final FeatureTransformation transformation;

    public RegressorFeatureTransformer(Regressor regressor, FeatureTransformation transformation)
    {
        this.regressor = requireNonNull(regressor, "regressor is null");
        this.transformation = requireNonNull(transformation, "transformation is null");
    }

    @Override
    public ModelType getType()
    {
        return RegressorType.REGRESSOR;
    }

    @Override
    public byte[] getSerializedData()
    {
        return ModelUtils.serializeModels(regressor, transformation);
    }

    public static RegressorFeatureTransformer deserialize(byte[] data)
    {
        List<Model> models = ModelUtils.deserializeModels(data);

        return new RegressorFeatureTransformer(checkType(models.get(0), Regressor.class, "model 0"), checkType(models.get(1), FeatureTransformation.class, "model 1"));
    }

    @Override
    public double regress(FeatureVector features)
    {
        return regressor.regress(transformation.transform(features));
    }

    @Override
    public void train(Dataset dataset)
    {
        transformation.train(dataset);
        regressor.train(transformation.transform(dataset));
    }
}
