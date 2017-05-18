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

import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;

import javax.management.MBeanServer;
import javax.management.openmbean.KeyAlreadyExistsException;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static java.util.Objects.requireNonNull;

public final class MBeanNamespaceManager
{
    private final ConcurrentHashMap<String, NamespacedMBeanServer> registeredEntries = new ConcurrentHashMap();

    @Inject(optional = true)
    @BaseNamespace
    private String baseNamespace = GLOBAL;

    public static final String GLOBAL = "";

    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.FIELD, ElementType.PARAMETER})
    @BindingAnnotation
    public @interface BaseNamespace {}

    public MBeanNamespaceManager()
    {
        this(GLOBAL);
    }

    public MBeanNamespaceManager(String baseName)
    {
        this.baseNamespace = baseName;
    }

    public NamespacedMBeanServer createMBeanServer(String namespace, MBeanServer parent)
            throws KeyAlreadyExistsException
    {
        requireNonNull(namespace, "namespace is null");
        requireNonNull(parent, "parent is null");
        String fullNamespace = namespace;
        if (baseNamespace != null && !baseNamespace.equals(GLOBAL)) {
            fullNamespace = baseNamespace + NamespacedMBeanServer.SEPARATOR + namespace;
        }

        if (registeredEntries.containsKey(namespace)) {
            // already registered
            throw new KeyAlreadyExistsException(String.format("MBean namespace %s already registered in %s!", namespace, baseNamespace));
        }
        NamespacedMBeanServer namespaceManager = new NamespacedMBeanServer(fullNamespace, parent);
        registeredEntries.put(namespace, namespaceManager);
        return namespaceManager;
    }
    public NamespacedMBeanServer createMBeanServer(String namespace)
            throws KeyAlreadyExistsException
    {
        requireNonNull(namespace, "namespace is null");
       return createMBeanServer(namespace, getPlatformMBeanServer());
    }
}
