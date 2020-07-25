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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.ml.type.RegressorType;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.hash.HashCode;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.facebook.presto.ml.type.ClassifierType.BIGINT_CLASSIFIER;
import static com.facebook.presto.ml.type.ClassifierType.VARCHAR_CLASSIFIER;
import static com.facebook.presto.ml.type.RegressorType.REGRESSOR;
import static com.google.common.base.Preconditions.checkArgument;

public final class MLFunctions
{
    private static final Cache<HashCode, Model> MODEL_CACHE = CacheBuilder.newBuilder().maximumSize(5).build();
    private static final String MAP_BIGINT_DOUBLE = "map(bigint,double)";

    private MLFunctions()
    {
    }

    @ScalarFunction("classify")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice varcharClassify(@SqlType(MAP_BIGINT_DOUBLE) Block featuresMap, @SqlType("Classifier<varchar>") Slice modelSlice)
    {
        FeatureVector features = ModelUtils.toFeatures(featuresMap);
        Model model = getOrLoadModel(modelSlice);
        checkArgument(model.getType().equals(VARCHAR_CLASSIFIER), "model is not a classifier<varchar>");
        Classifier<String> varcharClassifier = (Classifier) model;
        return Slices.utf8Slice(varcharClassifier.classify(features));
    }

    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long classify(@SqlType(MAP_BIGINT_DOUBLE) Block featuresMap, @SqlType("Classifier<bigint>") Slice modelSlice)
    {
        FeatureVector features = ModelUtils.toFeatures(featuresMap);
        Model model = getOrLoadModel(modelSlice);
        checkArgument(model.getType().equals(BIGINT_CLASSIFIER), "model is not a classifier<bigint>");
        Classifier<Integer> classifier = (Classifier) model;
        return classifier.classify(features);
    }

    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double regress(@SqlType(MAP_BIGINT_DOUBLE) Block featuresMap, @SqlType(RegressorType.NAME) Slice modelSlice)
    {
        FeatureVector features = ModelUtils.toFeatures(featuresMap);
        Model model = getOrLoadModel(modelSlice);
        checkArgument(model.getType().equals(REGRESSOR), "model is not a regressor");
        Regressor regressor = (Regressor) model;
        return regressor.regress(features);
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
}
