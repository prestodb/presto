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

package com.facebook.presto.hudi;

import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.collect.ImmutableList;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class HudiPlugin
        implements Plugin
{
    private final String name;
    private final Optional<ExtendedHiveMetastore> metastore;

    public HudiPlugin()
    {
        this("hudi", Optional.empty());
    }

    public HudiPlugin(String name, Optional<ExtendedHiveMetastore> metastore)
    {
        this.name = requireNonNull(name, "name is null");
        this.metastore = requireNonNull(metastore, "metastore is null");
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new HudiConnectorFactory(name, getClassLoader(), metastore));
    }

    private static ClassLoader getClassLoader()
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = HudiPlugin.class.getClassLoader();
        }
        return classLoader;
    }
}
