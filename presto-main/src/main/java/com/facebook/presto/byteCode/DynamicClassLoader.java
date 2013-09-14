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
package com.facebook.presto.byteCode;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class DynamicClassLoader
        extends ClassLoader
{
    private final ConcurrentMap<ParameterizedType, byte[]> pendingClasses = new ConcurrentHashMap<>();

    public DynamicClassLoader()
    {
        this(null);
    }

    public DynamicClassLoader(ClassLoader parentClassLoader)
    {
        super(resolveClassLoader(parentClassLoader));
    }

    public Class<?> defineClass(String className, byte[] byteCode)
    {
        return super.defineClass(className, byteCode, 0, byteCode.length);
    }

    public Map<String, Class<?>> defineClasses(Map<ParameterizedType, byte[]> newClasses)
    {
        SetView<ParameterizedType> conflicts = Sets.intersection(pendingClasses.keySet(), newClasses.keySet());
        Preconditions.checkArgument(conflicts.isEmpty(), "The classes %s have already been defined", conflicts);

        pendingClasses.putAll(newClasses);
        try {
            Map<String, Class<?>> classes = new HashMap<>();
            for (ParameterizedType type : newClasses.keySet()) {
                try {
                    Class<?> clazz = loadClass(type.getJavaClassName());
                    classes.put(type.getJavaClassName(), clazz);
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

    @Override
    protected Class<?> findClass(String name)
            throws ClassNotFoundException
    {
        byte[] byteCode = pendingClasses.get(ParameterizedType.typeFromJavaClassName(name));
        if (byteCode == null) {
            throw new ClassNotFoundException(name);
        }

        return defineClass(name, byteCode);
    }

    private static ClassLoader resolveClassLoader(ClassLoader parentClassLoader)
    {
        if (parentClassLoader == null) {
            parentClassLoader = Thread.currentThread().getContextClassLoader();
        }
        if (parentClassLoader == null) {
            parentClassLoader = DynamicClassLoader.class.getClassLoader();
        }
        if (parentClassLoader == null) {
            parentClassLoader = ClassLoader.getSystemClassLoader();
        }
        return parentClassLoader;
    }
}
