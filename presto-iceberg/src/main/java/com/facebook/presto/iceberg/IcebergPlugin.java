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
package com.facebook.presto.iceberg;

import com.facebook.presto.iceberg.function.changelog.ApplyChangelogFunction;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.management.MBeanServer;

import java.lang.management.ManagementFactory;
import java.util.Set;

public class IcebergPlugin
        implements Plugin
{
    private final MBeanServer mBeanServer;

    public IcebergPlugin()
    {
        this(ManagementFactory.getPlatformMBeanServer());
    }

    public IcebergPlugin(MBeanServer mBeanServer)
    {
        this.mBeanServer = mBeanServer;
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new IcebergConnectorFactory(mBeanServer));
    }

    @Override
    public Set<Class<?>> getFunctions()
    {
        return ImmutableSet.<Class<?>>builder()
                .add(ApplyChangelogFunction.class)
                .build();
    }
}
