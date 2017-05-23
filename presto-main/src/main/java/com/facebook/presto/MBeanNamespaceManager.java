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
package com.facebook.presto;

import javax.management.MBeanServer;
import javax.management.openmbean.KeyAlreadyExistsException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.String.format;
import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static java.util.Objects.requireNonNull;

public final class MBeanNamespaceManager
{
    private final Map<String, NamespacedMBeanServer> registeredEntries = new ConcurrentHashMap();

    private final String baseNamespace;

    public static final String GLOBAL = "";

    private MBeanNamespaceManager(String baseName)
    {
        requireNonNull(baseName, "baseName is null");
        this.baseNamespace = baseName;
    }

    public static MBeanNamespaceManager createMBeanNamespaceManager()
    {
        return new MBeanNamespaceManager(GLOBAL);
    }

    public static MBeanNamespaceManager createMBeanNamespaceManager(String baseName)
    {
        requireNonNull(baseName, "baseName is null");
        return new MBeanNamespaceManager(baseName);
    }

    public NamespacedMBeanServer createMBeanServer(String namespace, MBeanServer parent)
    {
        requireNonNull(namespace, "namespace is null");
        requireNonNull(parent, "parent is null");
        String fullNamespace = namespace;
        if (!isGlobal(baseNamespace)) {
            fullNamespace = baseNamespace + NamespacedMBeanServer.SEPARATOR + namespace;
        }

        NamespacedMBeanServer namespaceManager = new NamespacedMBeanServer(fullNamespace, parent);
        if (registeredEntries.putIfAbsent(namespace, namespaceManager) != null) {
            throw new KeyAlreadyExistsException(format("MBean namespace %s already registered in %s", namespace, baseNamespace));
        }
        return namespaceManager;
    }

    public NamespacedMBeanServer createMBeanServer(String namespace)
    {
        requireNonNull(namespace, "namespace is null");
       return createMBeanServer(namespace, getPlatformMBeanServer());
    }

    private static boolean isGlobal(String namespace)
    {
       if (namespace.equals(GLOBAL) || namespace.toLowerCase().equals("global")) {
           return true;
       }
       return false;
    }
}
