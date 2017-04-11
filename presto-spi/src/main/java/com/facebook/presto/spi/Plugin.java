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
package com.facebook.presto.spi;

import com.facebook.presto.spi.block.BlockEncodingFactory;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.eventlistener.EventListenerFactory;
import com.facebook.presto.spi.resourceGroups.ResourceGroupConfigurationManagerFactory;
import com.facebook.presto.spi.security.SystemAccessControlFactory;
import com.facebook.presto.spi.type.ParametricType;
import com.facebook.presto.spi.type.Type;

import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;

public interface Plugin
{
    default Iterable<ConnectorFactory> getConnectorFactories()
    {
        return emptyList();
    }

    default Iterable<BlockEncodingFactory<?>> getBlockEncodingFactories(BlockEncodingSerde serde)
    {
        return emptyList();
    }

    default Iterable<Type> getTypes()
    {
        return emptyList();
    }

    default Iterable<ParametricType> getParametricTypes()
    {
        return emptyList();
    }

    default Set<Class<?>> getFunctions()
    {
        return emptySet();
    }

    default Iterable<SystemAccessControlFactory> getSystemAccessControlFactories()
    {
        return emptyList();
    }

    default Iterable<EventListenerFactory> getEventListenerFactories()
    {
        return emptyList();
    }

    default Iterable<ResourceGroupConfigurationManagerFactory> getResourceGroupConfigurationManagerFactories()
    {
        return emptyList();
    }
}
