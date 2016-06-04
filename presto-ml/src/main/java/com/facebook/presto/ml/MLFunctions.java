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

import com.facebook.presto.ml.type.RegressorType;
import com.facebook.presto.operator.scalar.ScalarFunction;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.InterleavedBlockBuilder;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.HashCode;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.facebook.presto.ml.type.ClassifierType.BIGINT_CLASSIFIER;
import static com.facebook.presto.ml.type.ClassifierType.VARCHAR_CLASSIFIER;
import static com.facebook.presto.ml.type.RegressorType.REGRESSOR;
import static com.facebook.presto.util.Types.checkType;
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
        Classifier<String> varcharClassifier = checkType(model, Classifier.class, "model");
        return Slices.utf8Slice(varcharClassifier.classify(features));
    }

    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long classify(@SqlType(MAP_BIGINT_DOUBLE) Block featuresMap, @SqlType("Classifier<bigint>") Slice modelSlice)
    {
        FeatureVector features = ModelUtils.toFeatures(featuresMap);
        Model model = getOrLoadModel(modelSlice);
        checkArgument(model.getType().equals(BIGINT_CLASSIFIER), "model is not a classifier<bigint>");
        Classifier<Integer> classifier = checkType(model, Classifier.class, "model");
        return classifier.classify(features);
    }

    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double regress(@SqlType(MAP_BIGINT_DOUBLE) Block featuresMap, @SqlType(RegressorType.NAME) Slice modelSlice)
    {
        FeatureVector features = ModelUtils.toFeatures(featuresMap);
        Model model = getOrLoadModel(modelSlice);
        checkArgument(model.getType().equals(REGRESSOR), "model is not a regressor");
        Regressor regressor = checkType(model, Regressor.class, "model");
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

    @ScalarFunction
    @SqlType(MAP_BIGINT_DOUBLE)
    public static Block features(@SqlType(StandardTypes.DOUBLE) double f1)
    {
        return featuresHelper(f1);
    }

    @ScalarFunction
    @SqlType(MAP_BIGINT_DOUBLE)
    public static Block features(@SqlType(StandardTypes.DOUBLE) double f1, @SqlType(StandardTypes.DOUBLE) double f2)
    {
        return featuresHelper(f1, f2);
    }

    @ScalarFunction
    @SqlType(MAP_BIGINT_DOUBLE)
    public static Block features(@SqlType(StandardTypes.DOUBLE) double f1, @SqlType(StandardTypes.DOUBLE) double f2, @SqlType(StandardTypes.DOUBLE) double f3)
    {
        return featuresHelper(f1, f2, f3);
    }

    @ScalarFunction
    @SqlType(MAP_BIGINT_DOUBLE)
    public static Block features(@SqlType(StandardTypes.DOUBLE) double f1, @SqlType(StandardTypes.DOUBLE) double f2, @SqlType(StandardTypes.DOUBLE) double f3, @SqlType(StandardTypes.DOUBLE) double f4)
    {
        return featuresHelper(f1, f2, f3, f4);
    }

    @ScalarFunction
    @SqlType(MAP_BIGINT_DOUBLE)
    public static Block features(@SqlType(StandardTypes.DOUBLE) double f1, @SqlType(StandardTypes.DOUBLE) double f2, @SqlType(StandardTypes.DOUBLE) double f3, @SqlType(StandardTypes.DOUBLE) double f4, @SqlType(StandardTypes.DOUBLE) double f5)
    {
        return featuresHelper(f1, f2, f3, f4, f5);
    }

    @ScalarFunction
    @SqlType(MAP_BIGINT_DOUBLE)
    public static Block features(@SqlType(StandardTypes.DOUBLE) double f1, @SqlType(StandardTypes.DOUBLE) double f2, @SqlType(StandardTypes.DOUBLE) double f3, @SqlType(StandardTypes.DOUBLE) double f4, @SqlType(StandardTypes.DOUBLE) double f5, @SqlType(StandardTypes.DOUBLE) double f6)
    {
        return featuresHelper(f1, f2, f3, f4, f5, f6);
    }

    @ScalarFunction
    @SqlType(MAP_BIGINT_DOUBLE)
    public static Block features(@SqlType(StandardTypes.DOUBLE) double f1, @SqlType(StandardTypes.DOUBLE) double f2, @SqlType(StandardTypes.DOUBLE) double f3, @SqlType(StandardTypes.DOUBLE) double f4, @SqlType(StandardTypes.DOUBLE) double f5, @SqlType(StandardTypes.DOUBLE) double f6, @SqlType(StandardTypes.DOUBLE) double f7)
    {
        return featuresHelper(f1, f2, f3, f4, f5, f6, f7);
    }

    @ScalarFunction
    @SqlType(MAP_BIGINT_DOUBLE)
    public static Block features(@SqlType(StandardTypes.DOUBLE) double f1, @SqlType(StandardTypes.DOUBLE) double f2, @SqlType(StandardTypes.DOUBLE) double f3, @SqlType(StandardTypes.DOUBLE) double f4, @SqlType(StandardTypes.DOUBLE) double f5, @SqlType(StandardTypes.DOUBLE) double f6, @SqlType(StandardTypes.DOUBLE) double f7, @SqlType(StandardTypes.DOUBLE) double f8)
    {
        return featuresHelper(f1, f2, f3, f4, f5, f6, f7, f8);
    }

    @ScalarFunction
    @SqlType(MAP_BIGINT_DOUBLE)
    public static Block features(@SqlType(StandardTypes.DOUBLE) double f1, @SqlType(StandardTypes.DOUBLE) double f2, @SqlType(StandardTypes.DOUBLE) double f3, @SqlType(StandardTypes.DOUBLE) double f4, @SqlType(StandardTypes.DOUBLE) double f5, @SqlType(StandardTypes.DOUBLE) double f6, @SqlType(StandardTypes.DOUBLE) double f7, @SqlType(StandardTypes.DOUBLE) double f8, @SqlType(StandardTypes.DOUBLE) double f9)
    {
        return featuresHelper(f1, f2, f3, f4, f5, f6, f7, f8, f9);
    }

    @ScalarFunction
    @SqlType(MAP_BIGINT_DOUBLE)
    public static Block features(@SqlType(StandardTypes.DOUBLE) double f1, @SqlType(StandardTypes.DOUBLE) double f2, @SqlType(StandardTypes.DOUBLE) double f3, @SqlType(StandardTypes.DOUBLE) double f4, @SqlType(StandardTypes.DOUBLE) double f5, @SqlType(StandardTypes.DOUBLE) double f6, @SqlType(StandardTypes.DOUBLE) double f7, @SqlType(StandardTypes.DOUBLE) double f8, @SqlType(StandardTypes.DOUBLE) double f9, @SqlType(StandardTypes.DOUBLE) double f10)
    {
        return featuresHelper(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10);
    }

    private static Block featuresHelper(double... features)
    {
        BlockBuilder blockBuilder = new InterleavedBlockBuilder(ImmutableList.of(BigintType.BIGINT, DoubleType.DOUBLE), new BlockBuilderStatus(), features.length);

        for (int i = 0; i < features.length; i++) {
            BigintType.BIGINT.writeLong(blockBuilder, i);
            DoubleType.DOUBLE.writeDouble(blockBuilder, features[i]);
        }

        return blockBuilder.build();
    }
}
