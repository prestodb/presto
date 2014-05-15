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

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.metadata.FunctionFactory;
import com.facebook.presto.ml.type.ClassifierType;
import com.facebook.presto.ml.type.ModelType;
import com.facebook.presto.ml.type.RegressorType;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class MLPlugin
        implements Plugin
{
    private Map<String, String> optionalConfig = ImmutableMap.of();

    @Override
    public synchronized void setOptionalConfig(Map<String, String> optionalConfig)
    {
        this.optionalConfig = ImmutableMap.copyOf(checkNotNull(optionalConfig, "optionalConfig is null"));
    }

    public synchronized Map<String, String> getOptionalConfig()
    {
        return optionalConfig;
    }

    @Inject
    public void setBlockEncodingManager(BlockEncodingManager blockEncodingManager)
    {
        checkNotNull(blockEncodingManager, "blockEncodingManager is null");
        blockEncodingManager.addBlockEncodingFactory(ModelType.BLOCK_ENCODING_FACTORY);
        blockEncodingManager.addBlockEncodingFactory(ClassifierType.BLOCK_ENCODING_FACTORY);
        blockEncodingManager.addBlockEncodingFactory(RegressorType.BLOCK_ENCODING_FACTORY);
    }

    @Override
    public synchronized <T> List<T> getServices(Class<T> type)
    {
        if (type == FunctionFactory.class) {
            return ImmutableList.of(type.cast(new MLFunctionFactory()));
        }
        else if (type == Type.class) {
            return ImmutableList.of(type.cast(ModelType.MODEL), type.cast(ClassifierType.CLASSIFIER), type.cast(RegressorType.REGRESSOR));
        }
        return ImmutableList.of();
    }
}
