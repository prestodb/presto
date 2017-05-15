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

import java.util.concurrent.ConcurrentHashMap;

import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static java.util.Objects.requireNonNull;

public final class MBeanNamespaceManager
{
    ConcurrentHashMap<String, NamespacedMBeanServer> registeredEntries = new ConcurrentHashMap();

    public NamespacedMBeanServer createMBeanServer(String namespace, MBeanServer parent)
    {
        requireNonNull(namespace, "namespace is null");
        requireNonNull(parent, "parent is null");
        if (registeredEntries.containsKey(namespace)) {
            // already registered
            throw new RuntimeException(String.format("MBean namespace %s already registered!", namespace));
        }
        try {
            NamespacedMBeanServer namespaceManager = new NamespacedMBeanServer(namespace, parent);
            registeredEntries.put(namespace, namespaceManager);
            return namespaceManager;
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to create namespaced MBeanServer", e);
        }
    }
    public NamespacedMBeanServer createMBeanServer(String namespace)
    {
        requireNonNull(namespace, "namespace is null");
       return createMBeanServer(namespace, getPlatformMBeanServer());
    }
}
