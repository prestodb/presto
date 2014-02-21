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
import com.facebook.presto.ml.type.RegressorType;
import com.facebook.presto.operator.scalar.ScalarFunction;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.type.SqlType;
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

import static com.facebook.presto.ml.type.ClassifierType.CLASSIFIER;
import static com.facebook.presto.ml.type.RegressorType.REGRESSOR;
import static com.google.common.base.Preconditions.checkArgument;

public final class MLFunctions
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Cache<HashCode, Model> MODEL_CACHE = CacheBuilder.newBuilder().maximumSize(5).build();

    private MLFunctions()
    {
    }

    @ScalarFunction
    @SqlType(BigintType.class)
    public static long classify(@SqlType(VarcharType.class) Slice featuresMap, @SqlType(ClassifierType.class) Slice modelSlice)
    {
        FeatureVector features = ModelUtils.jsonToFeatures(featuresMap);
        Model model = getOrLoadModel(modelSlice);
        checkArgument(model instanceof Classifier && model.getType().equals(CLASSIFIER), "model is not a classifier");
        return ((Classifier) model).classify(features);
    }

    @ScalarFunction
    @SqlType(DoubleType.class)
    public static double regress(@SqlType(VarcharType.class) Slice featuresMap, @SqlType(RegressorType.class) Slice modelSlice)
    {
        FeatureVector features = ModelUtils.jsonToFeatures(featuresMap);
        Model model = getOrLoadModel(modelSlice);
        checkArgument(model instanceof Regressor && model.getType().equals(REGRESSOR), "model is not a regressor");
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
    @SqlType(VarcharType.class)
    public static Slice features(@SqlType(DoubleType.class) double f1)
    {
        return featuresHelper(f1);
    }

    @ScalarFunction
    @SqlType(VarcharType.class)
    public static Slice features(@SqlType(DoubleType.class) double f1, @SqlType(DoubleType.class) double f2)
    {
        return featuresHelper(f1, f2);
    }

    @ScalarFunction
    @SqlType(VarcharType.class)
    public static Slice features(@SqlType(DoubleType.class) double f1, @SqlType(DoubleType.class) double f2, @SqlType(DoubleType.class) double f3)
    {
        return featuresHelper(f1, f2, f3);
    }

    @ScalarFunction
    @SqlType(VarcharType.class)
    public static Slice features(@SqlType(DoubleType.class) double f1, @SqlType(DoubleType.class) double f2, @SqlType(DoubleType.class) double f3, @SqlType(DoubleType.class) double f4)
    {
        return featuresHelper(f1, f2, f3, f4);
    }

    @ScalarFunction
    @SqlType(VarcharType.class)
    public static Slice features(@SqlType(DoubleType.class) double f1, @SqlType(DoubleType.class) double f2, @SqlType(DoubleType.class) double f3, @SqlType(DoubleType.class) double f4, @SqlType(DoubleType.class) double f5)
    {
        return featuresHelper(f1, f2, f3, f4, f5);
    }

    @ScalarFunction
    @SqlType(VarcharType.class)
    public static Slice features(@SqlType(DoubleType.class) double f1, @SqlType(DoubleType.class) double f2, @SqlType(DoubleType.class) double f3, @SqlType(DoubleType.class) double f4, @SqlType(DoubleType.class) double f5, @SqlType(DoubleType.class) double f6)
    {
        return featuresHelper(f1, f2, f3, f4, f5, f6);
    }

    @ScalarFunction
    @SqlType(VarcharType.class)
    public static Slice features(@SqlType(DoubleType.class) double f1, @SqlType(DoubleType.class) double f2, @SqlType(DoubleType.class) double f3, @SqlType(DoubleType.class) double f4, @SqlType(DoubleType.class) double f5, @SqlType(DoubleType.class) double f6, @SqlType(DoubleType.class) double f7)
    {
        return featuresHelper(f1, f2, f3, f4, f5, f6, f7);
    }

    @ScalarFunction
    @SqlType(VarcharType.class)
    public static Slice features(@SqlType(DoubleType.class) double f1, @SqlType(DoubleType.class) double f2, @SqlType(DoubleType.class) double f3, @SqlType(DoubleType.class) double f4, @SqlType(DoubleType.class) double f5, @SqlType(DoubleType.class) double f6, @SqlType(DoubleType.class) double f7, @SqlType(DoubleType.class) double f8)
    {
        return featuresHelper(f1, f2, f3, f4, f5, f6, f7, f8);
    }

    @ScalarFunction
    @SqlType(VarcharType.class)
    public static Slice features(@SqlType(DoubleType.class) double f1, @SqlType(DoubleType.class) double f2, @SqlType(DoubleType.class) double f3, @SqlType(DoubleType.class) double f4, @SqlType(DoubleType.class) double f5, @SqlType(DoubleType.class) double f6, @SqlType(DoubleType.class) double f7, @SqlType(DoubleType.class) double f8, @SqlType(DoubleType.class) double f9)
    {
        return featuresHelper(f1, f2, f3, f4, f5, f6, f7, f8, f9);
    }

    @ScalarFunction
    @SqlType(VarcharType.class)
    public static Slice features(@SqlType(DoubleType.class) double f1, @SqlType(DoubleType.class) double f2, @SqlType(DoubleType.class) double f3, @SqlType(DoubleType.class) double f4, @SqlType(DoubleType.class) double f5, @SqlType(DoubleType.class) double f6, @SqlType(DoubleType.class) double f7, @SqlType(DoubleType.class) double f8, @SqlType(DoubleType.class) double f9, @SqlType(DoubleType.class) double f10)
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
