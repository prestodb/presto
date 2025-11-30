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
package com.facebook.presto.iceberg.vectors;

import com.facebook.airlift.log.Logger;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.vector.ArrayVectorFloat;
import io.github.jbellis.jvector.vector.types.VectorFloat;

import java.lang.reflect.Constructor;
import java.util.List;

/**
 * Implementation of RandomAccessVectorValues that wraps a list of vectors.
 * This is used to provide random access to vectors read from Iceberg tables.
 */
public class ListRandomAccessVectorValues
        implements RandomAccessVectorValues
{
    private static final Logger log = Logger.get(ListRandomAccessVectorValues.class);
    private final List<float[]> vectors;
    private final int dimension;
    private final Constructor<?> arrayVectorFloatConstructor;

    public ListRandomAccessVectorValues(List<float[]> vectors, int dimension)
    {
        this.vectors = vectors;
        this.dimension = dimension;
        // Trying to get the ArrayVectorFloat constructor using reflection with multiple approaches
        Constructor<?> constructor = null;
        // First approach: direct class name
        try {
            Class<?> arrayVectorFloatClass = ArrayVectorFloat.class;
            constructor = arrayVectorFloatClass.getDeclaredConstructor(float[].class);
            constructor.setAccessible(true);
            log.info("Found ArrayVectorFloat constructor using direct class name");
        }
        catch (Exception e) {
            log.warn("Could not get ArrayVectorFloat constructor using direct class name: %s", e.getMessage());
            // Second approach: try to find the class from VectorFloat package
            try {
                // Get the package from VectorFloat
                Package vectorPackage = VectorFloat.class.getPackage();
                String packageName = vectorPackage.getName();
                // Try to load the class from the same package
                Class<?> arrayVectorFloatClass = Class.forName(packageName + ".ArrayVectorFloat");
                constructor = arrayVectorFloatClass.getDeclaredConstructor(float[].class);
                constructor.setAccessible(true);
                log.info("Found ArrayVectorFloat constructor using package name: %s", packageName);
            }
            catch (Exception e2) {
                log.warn("Could not get ArrayVectorFloat constructor using package approach: %s", e2.getMessage());
            }
        }
        this.arrayVectorFloatConstructor = constructor;
        // Log whether we found the constructor
        if (constructor != null) {
            log.info("Successfully found ArrayVectorFloat constructor");
        }
        else {
            log.warn("Could not find ArrayVectorFloat constructor, will use CustomVectorFloat as fallback");
        }
    }

    @Override
    public int size()
    {
        return vectors.size();
    }

    @Override
    public int dimension()
    {
        return dimension;
    }

    @Override
    public VectorFloat<?> getVector(int ord)
    {
        // Get the vector data
        float[] data = vectors.get(ord);
        // Try to create an ArrayVectorFloat using reflection
        if (arrayVectorFloatConstructor != null) {
            try {
                Object vectorInstance = arrayVectorFloatConstructor.newInstance((Object) data);
                log.debug("Successfully created ArrayVectorFloat instance for ord=%d", ord);
                return (VectorFloat<?>) vectorInstance;
            }
            catch (Exception e) {
                // Log the full stack trace for better debugging
                log.warn("Could not create ArrayVectorFloat: %s", e.getMessage());
                log.debug("Exception creating ArrayVectorFloat", e);
                // Try again with a copy of the data
                try {
                    float[] dataCopy = new float[data.length];
                    System.arraycopy(data, 0, dataCopy, 0, data.length);
                    Object vectorInstance = arrayVectorFloatConstructor.newInstance((Object) dataCopy);
                    log.debug("Successfully created ArrayVectorFloat with data copy for ord=%d", ord);
                    return (VectorFloat<?>) vectorInstance;
                }
                catch (Exception e2) {
                    log.warn("Could not create ArrayVectorFloat with data copy: %s", e2.getMessage());
                }
            }
        }
        // Fall back to custom implementation if reflection fails
        log.debug("Using CustomVectorFloat fallback for ord=%d", ord);
        return new CustomVectorFloat(data);
    }

    @Override
    public boolean isValueShared()
    {
        return false;
    }

    @Override
    public RandomAccessVectorValues copy()
    {
        return this;
    }
}
