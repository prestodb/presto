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

import com.facebook.presto.metadata.FunctionFactory;
import com.facebook.presto.metadata.FunctionInfo;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.metadata.FunctionRegistry.FunctionListBuilder;
import static com.facebook.presto.ml.type.ClassifierType.CLASSIFIER;
import static com.facebook.presto.ml.type.RegressorType.REGRESSOR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.type.UnknownType.UNKNOWN;

public class MLFunctionFactory
        implements FunctionFactory
{
    private static final List<FunctionInfo> FUNCTIONS = new FunctionListBuilder()
            .aggregate("learn_classifier", CLASSIFIER, ImmutableList.of(BIGINT, VARCHAR), UNKNOWN, new LearnAggregation(CLASSIFIER, BIGINT))
            .aggregate("learn_classifier", CLASSIFIER, ImmutableList.of(DOUBLE, VARCHAR), UNKNOWN, new LearnAggregation(CLASSIFIER, DOUBLE))
            .aggregate("learn_regressor", REGRESSOR, ImmutableList.of(BIGINT, VARCHAR), UNKNOWN, new LearnAggregation(REGRESSOR, BIGINT))
            .aggregate("learn_regressor", REGRESSOR, ImmutableList.of(DOUBLE, VARCHAR), UNKNOWN, new LearnAggregation(REGRESSOR, DOUBLE))
            .aggregate("learn_libsvm_classifier", CLASSIFIER, ImmutableList.of(BIGINT, VARCHAR, VARCHAR), UNKNOWN, new LearnLibSvmAggregation(CLASSIFIER, BIGINT))
            .aggregate("learn_libsvm_classifier", CLASSIFIER, ImmutableList.of(DOUBLE, VARCHAR, VARCHAR), UNKNOWN, new LearnLibSvmAggregation(CLASSIFIER, DOUBLE))
            .aggregate("learn_libsvm_regressor", REGRESSOR, ImmutableList.of(BIGINT, VARCHAR, VARCHAR), UNKNOWN, new LearnLibSvmAggregation(REGRESSOR, BIGINT))
            .aggregate("learn_libsvm_regressor", REGRESSOR, ImmutableList.of(DOUBLE, VARCHAR, VARCHAR), UNKNOWN, new LearnLibSvmAggregation(REGRESSOR, DOUBLE))
            .aggregate("evaluate_classifier_predictions", VARCHAR, ImmutableList.of(BIGINT, BIGINT), UNKNOWN, new EvaluateClassifierPredictionsAggregation())
            .scalar(MLFunctions.class)
            .getFunctions();

    @Override
    public List<FunctionInfo> listFunctions()
    {
        return FUNCTIONS;
    }
}
