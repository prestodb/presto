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
package com.facebook.presto.session.sessionpropertyprovidermanagers;

import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.session.SessionPropertyContext;
import com.facebook.presto.spi.session.SystemSessionPropertyProvider;
import com.facebook.presto.spi.session.SystemSessionPropertyProviderFactory;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static com.facebook.presto.util.PropertiesUtil.loadProperties;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class SystemSessionPropertyProviderManager
{
    private static final File SESSION_PROPERTY_PROVIDER_CONFIG = new File("etc/session-property-provider.properties");
    private SystemSessionPropertyProviderFactory providerFactory;
    private SystemSessionPropertyProvider provider;
    private boolean isSessionProviderAdded;
    private final NodeManager nodeManager;
    private final TypeManager typeManager;

    @Inject
    public SystemSessionPropertyProviderManager(
            NodeManager nodeManager,
            TypeManager typeManager)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.provider = providerFactory.create(getConfig(), new SessionPropertyContext(typeManager, nodeManager));
    }

    public void addSessionPropertyProviderFactory(SystemSessionPropertyProviderFactory providerFactory)
    {
        this.providerFactory = requireNonNull(providerFactory, "providerFactory is null");
    }

    public void loadSessionPropertyProvider()
    {
        checkState(!isSessionProviderAdded, "SystemSessionPropertyProvider can only be set once");
        this.provider = this.providerFactory.create(getConfig(), new SessionPropertyContext(typeManager, nodeManager));
        this.isSessionProviderAdded = true;
    }

    public SystemSessionPropertyProvider getSessionPropertyProvider()
    {
        return this.provider;
    }

    private Map<String, String> getConfig()
    {
        Map<String, String> result;
        if (SESSION_PROPERTY_PROVIDER_CONFIG.exists()) {
            try {
                result = loadProperties(SESSION_PROPERTY_PROVIDER_CONFIG);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        else {
            result = ImmutableMap.of();
        }
        return result;
    }
}
