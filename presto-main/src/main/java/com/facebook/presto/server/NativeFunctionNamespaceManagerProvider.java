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
package com.facebook.presto.server;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.NodeManager;
import com.google.inject.Inject;

import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class NativeFunctionNamespaceManagerProvider
{
    private final NodeManager nodeManager;

    private final Metadata metadata;

    @Inject
    public NativeFunctionNamespaceManagerProvider(
            NodeManager nodeManager,
            Metadata metadata)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    public NodeManager getNodeManager()
    {
        return nodeManager;
    }

    public void loadNativeFunctionNamespaceManager(
            String nativeFunctionNamespaceManagerName,
            String catalogName,
            Map<String, String> properties)
    {
        metadata.getFunctionAndTypeManager().loadFunctionNamespaceManager(nativeFunctionNamespaceManagerName, catalogName, properties, Optional.ofNullable(getNodeManager()));
    }
}
