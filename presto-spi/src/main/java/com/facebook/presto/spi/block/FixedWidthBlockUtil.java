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
package com.facebook.presto.spi.block;

import com.facebook.presto.spi.type.FixedWidthType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class FixedWidthBlockUtil
{
    private FixedWidthBlockUtil()
    {
    }

    public interface FixedWidthBlockBuilderFactory
    {
        BlockBuilder createFixedWidthBlockBuilder(BlockBuilderStatus blockBuilderStatus);

        BlockBuilder createFixedWidthBlockBuilder(int positionCount);

        BlockEncodingFactory<?> getBlockEncodingFactory();
    }

    public static FixedWidthBlockBuilderFactory createIsolatedFixedWidthBlockBuilderFactory(FixedWidthType type)
    {
        Class<? extends FixedWidthBlockBuilderFactory> functionClass = isolateClass(
                FixedWidthBlockBuilderFactory.class,
                IsolatedFixedWidthBlockBuilderFactory.class,

                AbstractFixedWidthBlock.class,
                FixedWidthBlock.class,
                FixedWidthBlockBuilder.class,

                FixedWidthBlockCursor.class,

                FixedWidthBlockEncoding.class);

        try {
            return functionClass
                    .getConstructor(FixedWidthType.class)
                    .newInstance(type);
        }
        catch (Error | RuntimeException e) {
            throw e;
        }
        catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private static <T> Class<? extends T> isolateClass(Class<T> publicBaseClass, Class<? extends T> implementationClass, Class<?>... additionalClasses)
    {
        Map<String, byte[]> classByteCode = new HashMap<>();
        classByteCode.put(implementationClass.getName(), getByteCode(implementationClass));
        for (Class<?> additionalClass : additionalClasses) {
            classByteCode.put(additionalClass.getName(), getByteCode(additionalClass));
        }

        // load classes into a private class loader
        DynamicClassLoader dynamicClassLoader = new DynamicClassLoader(publicBaseClass.getClassLoader());
        Map<String, Class<?>> isolatedClasses = dynamicClassLoader.defineClasses(classByteCode);
        Class<?> isolatedClass = isolatedClasses.get(implementationClass.getName());

        // verify the isolated class
        if (isolatedClass == null) {
            throw new IllegalArgumentException(String.valueOf("Could load class " + implementationClass.getName()));
        }
        if (!publicBaseClass.isAssignableFrom(isolatedClass)) {
            throw new IllegalArgumentException(String.valueOf(String.format("Error isolating class %s, newly loaded class is not a sub type of %s",
                    implementationClass.getName(),
                    publicBaseClass.getName())));
        }
        if (isolatedClass == implementationClass) {
            throw new IllegalStateException(String.valueOf("Isolation failed"));
        }

        return isolatedClass.asSubclass(publicBaseClass);
    }

    private static byte[] getByteCode(Class<?> clazz)
    {
        InputStream stream = clazz.getClassLoader().getResourceAsStream(clazz.getName().replace('.', '/') + ".class");
        if (stream == null) {
            throw new IllegalArgumentException(String.valueOf("Could not obtain byte code for class " + clazz.getName()));
        }
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buffer = new byte[4096];
            for (int bytes = stream.read(buffer); bytes > 0; bytes = stream.read(buffer)) {
                out.write(buffer, 0, bytes);
            }
            return out.toByteArray();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static class DynamicClassLoader
            extends ClassLoader
    {
        private final ConcurrentMap<String, byte[]> pendingClasses = new ConcurrentHashMap<>();

        public DynamicClassLoader(ClassLoader parentClassLoader)
        {
            super(resolveClassLoader(parentClassLoader));
        }

        public Class<?> defineClass(String className, byte[] byteCode)
        {
            return defineClass(className, byteCode, 0, byteCode.length);
        }

        public Map<String, Class<?>> defineClasses(Map<String, byte[]> newClasses)
        {
            Set<String> conflicts = new TreeSet<>(newClasses.keySet());
            conflicts.retainAll(pendingClasses.keySet());
            if (!conflicts.isEmpty()) {
                throw new IllegalArgumentException(String.valueOf("The classes " + conflicts + " have already been defined"));
            }

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
                        throw new RuntimeException(e);
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
            byte[] byteCode = pendingClasses.get(name);
            if (byteCode == null) {
                throw new ClassNotFoundException(name);
            }

            return defineClass(name, byteCode);
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
}
