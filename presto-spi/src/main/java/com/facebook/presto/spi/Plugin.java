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

import com.facebook.presto.common.block.BlockEncoding;
import com.facebook.presto.common.type.ParametricType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.eventlistener.EventListenerFactory;
import com.facebook.presto.spi.function.FunctionNamespaceManagerFactory;
import com.facebook.presto.spi.prerequisites.QueryPrerequisitesFactory;
import com.facebook.presto.spi.resourceGroups.ResourceGroupConfigurationManagerFactory;
import com.facebook.presto.spi.security.PasswordAuthenticatorFactory;
import com.facebook.presto.spi.security.SystemAccessControlFactory;
import com.facebook.presto.spi.session.SessionPropertyConfigurationManagerFactory;
import com.facebook.presto.spi.storage.TempStorageFactory;
import com.facebook.presto.spi.ttl.ClusterTtlProviderFactory;
import com.facebook.presto.spi.ttl.NodeTtlFetcherFactory;

import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;

public interface Plugin
{
    default Iterable<ConnectorFactory> getConnectorFactories()
    {
        return emptyList();
    }

    default Iterable<BlockEncoding> getBlockEncodings()
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

    default Iterable<PasswordAuthenticatorFactory> getPasswordAuthenticatorFactories()
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

    default Iterable<SessionPropertyConfigurationManagerFactory> getSessionPropertyConfigurationManagerFactories()
    {
        return emptyList();
    }

    default Iterable<FunctionNamespaceManagerFactory> getFunctionNamespaceManagerFactories()
    {
        return emptyList();
    }

    default Iterable<TempStorageFactory> getTempStorageFactories()
    {
        return emptyList();
    }

    default Iterable<QueryPrerequisitesFactory> getQueryPrerequisitesFactories()
    {
        return emptyList();
    }

    default Iterable<NodeTtlFetcherFactory> getNodeTtlFetcherFactories()
    {
        return emptyList();
    }

    default Iterable<ClusterTtlProviderFactory> getClusterTtlProviderFactories()
    {
        return emptyList();
    }
}
