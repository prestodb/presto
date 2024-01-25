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

import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.spi.NodeState;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.session.SystemSessionPropertyProvider;
import com.facebook.presto.spi.session.SystemSessionPropertyProviderFactory;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.util.PropertiesUtil.loadProperties;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class SystemSessionPropertyProviderManager
{
    private static final File SESSION_PROPERTY_PROVIDER_CONFIG = new File("etc/session-property-provider.properties");
    private SystemSessionPropertyProviderFactory providerFactory;
    private SystemSessionPropertyProvider provider;
    private boolean isSessionProviderAdded;
    private final InternalNodeManager nodeManager;

    @Inject
    public SystemSessionPropertyProviderManager(InternalNodeManager nodeManager)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    }

    public void addSessionPropertyProviderFactory(SystemSessionPropertyProviderFactory providerFactory)
    {
        this.providerFactory = requireNonNull(providerFactory, "providerFactory is null");
    }

    public void loadSessionPropertyProvider()
            throws IOException
    {
        if (this.providerFactory == null) {
            return;
        }
        checkState(!isSessionProviderAdded, "SystemSessionPropertyProvider can only be set once");
        this.provider = this.providerFactory.create(getConfig(), getNativeNodeUri());
        this.isSessionProviderAdded = true;
    }

    public SystemSessionPropertyProvider getSessionPropertyProvider()
    {
        return this.provider;
    }

    private Map<String, String> getConfig()
            throws IOException
    {
        Map<String, String> result;
        if (SESSION_PROPERTY_PROVIDER_CONFIG.exists()) {
            result = loadProperties(SESSION_PROPERTY_PROVIDER_CONFIG);
        }
        else {
            result = ImmutableMap.of();
        }
        return result;
    }

    private URI getNativeNodeUri()
    {
        Set<InternalNode> nodes = nodeManager.getNodes(NodeState.ACTIVE);

        // TODO: Need to identity native worker.
        // Temporary picking random worker for now.
        InternalNode nativeNode = nodes.stream()
                .filter(node -> !node.isCoordinator())
                .filter(node -> !node.isCatalogServer())
                .findFirst()
                .orElseThrow(() -> new PrestoException(NOT_FOUND, "Failed to find native node"));

        return nativeNode.getInternalUri();
    }
}
