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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;

import static java.util.Objects.requireNonNull;

class PluginClassLoader
        extends URLClassLoader
{
    private final List<String> hiddenClasses;
    private final List<String> parentFirstClasses;
    private final List<String> hiddenResources;
    private final List<String> parentFirstResources;

    public PluginClassLoader(List<URL> urls,
            ClassLoader parent,
            Iterable<String> hiddenClasses,
            Iterable<String> parentFirstClasses)
    {
        this(urls,
                parent,
                hiddenClasses,
                parentFirstClasses,
                Iterables.transform(hiddenClasses, PluginClassLoader::classNameToResource),
                Iterables.transform(parentFirstClasses, PluginClassLoader::classNameToResource));
    }

    public PluginClassLoader(List<URL> urls,
            ClassLoader parent,
            Iterable<String> hiddenClasses,
            Iterable<String> parentFirstClasses,
            Iterable<String> hiddenResources,
            Iterable<String> parentFirstResources)
    {
        // child first requires a parent class loader
        super(urls.toArray(new URL[urls.size()]), requireNonNull(parent, "parent is null"));
        this.hiddenClasses = ImmutableList.copyOf(hiddenClasses);
        this.parentFirstClasses = ImmutableList.copyOf(parentFirstClasses);
        this.hiddenResources = ImmutableList.copyOf(hiddenResources);
        this.parentFirstResources = ImmutableList.copyOf(parentFirstResources);
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

            // If this is not a parent first class, look for the class locally
            if (!isParentFirstClass(name)) {
                try {
                    Class<?> clazz = findClass(name);
                    return resolveClass(clazz, resolve);
                }
                catch (ClassNotFoundException ignored) {
                    // not a local class
                }
            }

            // Check parent class loader for parent first or non-hidden classes
            if (isParentFirstClass(name) || !isHiddenClass(name)) {
                try {
                    Class<?> clazz = getParent().loadClass(name);
                    return resolveClass(clazz, resolve);
                }
                catch (ClassNotFoundException ignored) {
                    // this parent didn't have the class
                }
            }

            // If this is a parent first class, now look for the class locally
            if (isParentFirstClass(name)) {
                Class<?> clazz = findClass(name);
                return resolveClass(clazz, resolve);
            }

            throw new ClassNotFoundException(name);
        }
    }

    private Class<?> resolveClass(Class<?> clazz, boolean resolve)
    {
        if (resolve) {
            resolveClass(clazz);
        }
        return clazz;
    }

    private boolean isParentFirstClass(String name)
    {
        for (String nonOverridableClass : parentFirstClasses) {
            // todo maybe make this more precise and only match base package
            if (name.startsWith(nonOverridableClass)) {
                return true;
            }
        }
        return false;
    }

    private boolean isHiddenClass(String name)
    {
        for (String hiddenClass : hiddenClasses) {
            // todo maybe make this more precise and only match base package
            if (name.startsWith(hiddenClass)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public URL getResource(String name)
    {
        // If this is not a parent first resource, check local resources first
        if (!isParentFirstResource(name)) {
            URL url = findResource(name);
            if (url != null) {
                return url;
            }
        }

        // Check parent class loaders
        if (!isHiddenResource(name)) {
            URL url = getParent().getResource(name);
            if (url != null) {
                return url;
            }
        }

        // If this is a parent first resource, now check local resources
        if (isParentFirstResource(name)) {
            URL url = findResource(name);
            if (url != null) {
                return url;
            }
        }

        return null;
    }

    @Override
    public Enumeration<URL> getResources(String name)
            throws IOException
    {
        List<Iterator<URL>> resources = new ArrayList<>();

        // If this is not a parent first resource, add resources from local urls first
        if (!isParentFirstResource(name)) {
            Iterator<URL> myResources = Iterators.forEnumeration(findResources(name));
            resources.add(myResources);
        }

        // Add parent resources
        if (!isHiddenResource(name)) {
            Iterator<URL> parentResources = Iterators.forEnumeration(getParent().getResources(name));
            resources.add(parentResources);
        }

        // If this is a parent first resource, now add resources from local urls
        if (isParentFirstResource(name)) {
            Iterator<URL> myResources = Iterators.forEnumeration(findResources(name));
            resources.add(myResources);
        }

        return Iterators.asEnumeration(Iterators.concat(resources.iterator()));
    }

    private boolean isParentFirstResource(String name)
    {
        for (String nonOverridableResource : parentFirstResources) {
            if (name.startsWith(nonOverridableResource)) {
                return true;
            }
        }
        return false;
    }

    private boolean isHiddenResource(String name)
    {
        for (String hiddenResource : hiddenResources) {
            if (name.startsWith(hiddenResource)) {
                return true;
            }
        }
        return false;
    }

    private static String classNameToResource(String className)
    {
        return className.replace('.', '/');
    }
}
