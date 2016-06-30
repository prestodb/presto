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
package com.facebook.presto.mongodb;

import com.facebook.presto.metadata.FunctionFactory;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.mongodb.ObjectIdType.OBJECT_ID;
import static java.util.Objects.requireNonNull;

public class MongoPlugin
        implements Plugin
{
    private Map<String, String> optionalConfig = ImmutableMap.of();
    private TypeManager typeManager;

    @Override
    public void setOptionalConfig(Map<String, String> optionalConfig)
    {
        this.optionalConfig = ImmutableMap.copyOf(requireNonNull(optionalConfig, "optionalConfig is null"));
    }

    @Inject
    public synchronized void setTypeManager(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public <T> List<T> getServices(Class<T> type)
    {
        if (type == ConnectorFactory.class) {
            return ImmutableList.of(type.cast(new MongoConnectorFactory("mongodb", typeManager, optionalConfig)));
        }
        if (type == Type.class) {
            return ImmutableList.of(type.cast(OBJECT_ID));
        }
        if (type == FunctionFactory.class) {
            return ImmutableList.of(type.cast(new MongoFunctionFactory()));
        }
        return ImmutableList.of();
    }
}
