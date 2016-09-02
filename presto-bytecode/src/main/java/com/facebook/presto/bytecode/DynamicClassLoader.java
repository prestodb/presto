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
package com.facebook.presto.bytecode;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

import java.lang.invoke.MethodHandle;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class DynamicClassLoader
        extends ClassLoader
{
    private final ConcurrentMap<String, byte[]> pendingClasses = new ConcurrentHashMap<>();
    private final Map<Long, MethodHandle> callSiteBindings;
    private final Optional<ClassLoader> overrideClassLoader;

    public DynamicClassLoader(ClassLoader parentClassLoader)
    {
        this(parentClassLoader, ImmutableMap.of());
    }

    // TODO: this is a hack that should be removed
    @Deprecated
    public DynamicClassLoader(ClassLoader overrideClassLoader, ClassLoader parentClassLoader)
    {
        super(parentClassLoader);
        this.callSiteBindings = ImmutableMap.of();
        this.overrideClassLoader = Optional.of(overrideClassLoader);
    }

    public DynamicClassLoader(ClassLoader parentClassLoader, Map<Long, MethodHandle> callSiteBindings)
    {
        super(parentClassLoader);
        this.callSiteBindings = ImmutableMap.copyOf(callSiteBindings);
        this.overrideClassLoader = Optional.empty();
    }

    public Class<?> defineClass(String className, byte[] bytecode)
    {
        return defineClass(className, bytecode, 0, bytecode.length);
    }

    public Map<String, Class<?>> defineClasses(Map<String, byte[]> newClasses)
    {
        SetView<String> conflicts = Sets.intersection(pendingClasses.keySet(), newClasses.keySet());
        Preconditions.checkArgument(conflicts.isEmpty(), "The classes %s have already been defined", conflicts);

        pendingClasses.putAll(newClasses);
        try {
            Map<String, Class<?>> classes = new HashMap<>();
            for (String className : newClasses.keySet()) {
                try {
                    Class<?> clazz = loadClass(className);
                    classes.put(className, clazz);
                }
                catch (ClassNotFoundException e) {
                    // this should never happen
                    throw Throwables.propagate(e);
                }
            }
            return classes;
        }
        finally {
            pendingClasses.keySet().removeAll(newClasses.keySet());
        }
    }

    public Map<Long, MethodHandle> getCallSiteBindings()
    {
        return callSiteBindings;
    }

    @Override
    protected Class<?> findClass(String name)
            throws ClassNotFoundException
    {
        byte[] bytecode = pendingClasses.get(name);
        if (bytecode == null) {
            throw new ClassNotFoundException(name);
        }

        return defineClass(name, bytecode);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve)
            throws ClassNotFoundException
    {
        // grab the magic lock
        synchronized (getClassLoadingLock(name)) {
            // Check if class is in the loaded classes cache
            Class<?> cachedClass = findLoadedClass(name);
            if (cachedClass != null) {
                return resolveClass(cachedClass, resolve);
            }

            try {
                Class<?> clazz = findClass(name);
                return resolveClass(clazz, resolve);
            }
            catch (ClassNotFoundException ignored) {
                // not a local class
            }

            if (overrideClassLoader.isPresent()) {
                try {
                    return resolveClass(overrideClassLoader.get().loadClass(name), resolve);
                }
                catch (ClassNotFoundException e) {
                    // not in override loader
                }
            }

            Class<?> clazz = getParent().loadClass(name);
            return resolveClass(clazz, resolve);
        }
    }

    private Class<?> resolveClass(Class<?> clazz, boolean resolve)
    {
        if (resolve) {
            resolveClass(clazz);
        }
        return clazz;
    }
}
