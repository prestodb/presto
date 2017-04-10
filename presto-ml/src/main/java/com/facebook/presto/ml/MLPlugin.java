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

import com.facebook.presto.ml.type.ClassifierParametricType;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.type.ParametricType;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

import static com.facebook.presto.ml.type.ModelType.MODEL;
import static com.facebook.presto.ml.type.RegressorType.REGRESSOR;

public class MLPlugin
        implements Plugin
{
    @Override
    public Iterable<Type> getTypes()
    {
        return ImmutableList.of(MODEL, REGRESSOR);
    }

    @Override
    public Iterable<ParametricType> getParametricTypes()
    {
        return ImmutableList.of(new ClassifierParametricType());
    }

    @Override
    public Set<Class<?>> getFunctions()
    {
        return ImmutableSet.<Class<?>>builder()
                .add(LearnClassifierAggregation.class)
                .add(LearnVarcharClassifierAggregation.class)
                .add(LearnRegressorAggregation.class)
                .add(LearnLibSvmClassifierAggregation.class)
                .add(LearnLibSvmVarcharClassifierAggregation.class)
                .add(LearnLibSvmRegressorAggregation.class)
                .add(EvaluateClassifierPredictionsAggregation.class)
                .add(MLFunctions.class)
                .build();
    }
}
