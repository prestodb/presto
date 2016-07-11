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
import com.facebook.presto.ml.type.ClassifierParametricType;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.ParametricType;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.ml.type.ModelType.MODEL;
import static com.facebook.presto.ml.type.RegressorType.REGRESSOR;

public class MLPlugin
        implements Plugin
{
    @Override
    public <T> List<T> getServices(Class<T> type)
    {
        if (type == FunctionFactory.class) {
            return ImmutableList.of(type.cast(new MLFunctionFactory()));
        }
        else if (type == Type.class) {
            return ImmutableList.of(type.cast(MODEL), type.cast(REGRESSOR));
        }
        else if (type == ParametricType.class) {
            return ImmutableList.of(type.cast(new ClassifierParametricType()));
        }
        return ImmutableList.of();
    }
}
