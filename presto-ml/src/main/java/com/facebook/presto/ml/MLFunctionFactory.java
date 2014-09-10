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
import com.facebook.presto.spi.type.TypeManager;

import java.util.List;

import com.facebook.presto.metadata.FunctionListBuilder;
import static com.facebook.presto.ml.type.ClassifierType.CLASSIFIER;
import static com.facebook.presto.ml.type.RegressorType.REGRESSOR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;

public class MLFunctionFactory
        implements FunctionFactory
{
    private final TypeManager typeManager;

    public MLFunctionFactory(TypeManager typeManager)
    {
        this.typeManager = typeManager;
    }

    @Override
    public List<FunctionInfo> listFunctions()
    {
        return new FunctionListBuilder(typeManager)
                .aggregate(new LearnAggregation(CLASSIFIER, BIGINT))
                .aggregate(new LearnAggregation(CLASSIFIER, DOUBLE))
                .aggregate(new LearnAggregation(REGRESSOR, BIGINT))
                .aggregate(new LearnAggregation(REGRESSOR, DOUBLE))
                .aggregate(new LearnLibSvmAggregation(CLASSIFIER, BIGINT))
                .aggregate(new LearnLibSvmAggregation(CLASSIFIER, DOUBLE))
                .aggregate(new LearnLibSvmAggregation(REGRESSOR, BIGINT))
                .aggregate(new LearnLibSvmAggregation(REGRESSOR, DOUBLE))
                .aggregate(EvaluateClassifierPredictionsAggregation.class)
                .scalar(MLFunctions.class)
                .getFunctions();
    }
}
