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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.ml.Classifier;
import com.facebook.presto.ml.FeatureVector;
import com.facebook.presto.ml.Model;
import com.facebook.presto.ml.ModelType;
import com.facebook.presto.ml.ModelUtils;
import com.facebook.presto.ml.Regressor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.hash.HashCode;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

public final class MLFunctions
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Cache<HashCode, Model> MODEL_CACHE = CacheBuilder.newBuilder().maximumSize(5).build();

    private MLFunctions()
    {
    }

    @ScalarFunction
    public static long classify(Slice featuresMap, Slice slice)
    {
        FeatureVector features = ModelUtils.jsonToFeatures(featuresMap);
        Model model = getOrLoadModel(slice);
        checkArgument(model instanceof Classifier && model.getType() == ModelType.CLASSIFIER, "model is not a regressor");
        return ((Classifier) model).classify(features);
    }

    @ScalarFunction
    public static double regress(Slice featuresMap, Slice slice)
    {
        FeatureVector features = ModelUtils.jsonToFeatures(featuresMap);
        Model model = getOrLoadModel(slice);
        checkArgument(model instanceof Regressor && model.getType() == ModelType.REGRESSOR, "model is not a regressor");
        return ((Regressor) model).regress(features);
    }

    private static Model getOrLoadModel(Slice slice)
    {
        HashCode modelHash = ModelUtils.modelHash(slice);

        Model model = MODEL_CACHE.getIfPresent(modelHash);
        if (model == null) {
            model = ModelUtils.deserialize(slice);
            MODEL_CACHE.put(modelHash, model);
        }

        return model;
    }

    @ScalarFunction
    public static Slice features(double f1)
    {
        return featuresHelper(f1);
    }

    @ScalarFunction
    public static Slice features(double f1, double f2)
    {
        return featuresHelper(f1, f2);
    }

    @ScalarFunction
    public static Slice features(double f1, double f2, double f3)
    {
        return featuresHelper(f1, f2, f3);
    }

    @ScalarFunction
    public static Slice features(double f1, double f2, double f3, double f4)
    {
        return featuresHelper(f1, f2, f3, f4);
    }

    @ScalarFunction
    public static Slice features(double f1, double f2, double f3, double f4, double f5)
    {
        return featuresHelper(f1, f2, f3, f4, f5);
    }

    @ScalarFunction
    public static Slice features(double f1, double f2, double f3, double f4, double f5, double f6)
    {
        return featuresHelper(f1, f2, f3, f4, f5, f6);
    }

    @ScalarFunction
    public static Slice features(double f1, double f2, double f3, double f4, double f5, double f6, double f7)
    {
        return featuresHelper(f1, f2, f3, f4, f5, f6, f7);
    }

    @ScalarFunction
    public static Slice features(double f1, double f2, double f3, double f4, double f5, double f6, double f7, double f8)
    {
        return featuresHelper(f1, f2, f3, f4, f5, f6, f7, f8);
    }

    @ScalarFunction
    public static Slice features(double f1, double f2, double f3, double f4, double f5, double f6, double f7, double f8, double f9)
    {
        return featuresHelper(f1, f2, f3, f4, f5, f6, f7, f8, f9);
    }

    @ScalarFunction
    public static Slice features(double f1, double f2, double f3, double f4, double f5, double f6, double f7, double f8, double f9, double f10)
    {
        return featuresHelper(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10);
    }

    private static Slice featuresHelper(double... features)
    {
        Map<Integer, Double> featureMap = new HashMap<>();

        for (int i = 0; i < features.length; i++) {
            featureMap.put(i, features[i]);
        }

        try {
            return Slices.utf8Slice(OBJECT_MAPPER.writeValueAsString(featureMap));
        }
        catch (JsonProcessingException e) {
            throw Throwables.propagate(e);
        }
    }
}
