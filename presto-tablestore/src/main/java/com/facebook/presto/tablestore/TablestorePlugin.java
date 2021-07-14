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
package com.facebook.presto.tablestore;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.collect.ImmutableList;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

public class TablestorePlugin
        implements Plugin
{
    private static final Logger log = Logger.get(TablestorePlugin.class);

    private final String name;

    public TablestorePlugin()
    {
        this("tablestore");
    }

    public TablestorePlugin(String name)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or empty");
        this.name = name;
        log.info("TablestorePlugin created.");
    }

    private static ClassLoader getClassLoader()
    {
        ClassLoader tcl = Thread.currentThread().getContextClassLoader();
        return firstNonNull(TablestorePlugin.class.getClassLoader(), tcl);
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new TablestoreConnectorFactory(name, getClassLoader()));
    }
}
