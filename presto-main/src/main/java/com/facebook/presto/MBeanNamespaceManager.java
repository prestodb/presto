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

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.management.ManagementFactory.getPlatformMBeanServer;

public final class MBeanNamespaceManager
{
    Set<String> registeredEntries = new ConcurrentHashMap().keySet();

    public NamespacedMBeanServer createMBeanServer(String proposedId)
    {
        if (registeredEntries.contains(proposedId)) {
            // already registered
            throw new RuntimeException(String.format("MBean namespace %s already registered!", proposedId));
        }
        registeredEntries.add(proposedId);
        try {
            return new NamespacedMBeanServer(proposedId, getPlatformMBeanServer());
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to create namespaced MBeanServer", e);
        }
    }
}
