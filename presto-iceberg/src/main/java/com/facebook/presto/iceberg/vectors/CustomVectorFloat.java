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
import io.github.jbellis.jvector.vector.types.VectorFloat;

import java.lang.reflect.Constructor;

/**
 * Custom implementation of VectorFloat that wraps a float[] array.
 * This is used because ArrayVectorFloat constructors are not public in JVector 4.0.0-rc.4.
 * This class also provides methods to convert to ArrayVectorFloat when needed.
 */
public class CustomVectorFloat
        implements VectorFloat<CustomVectorFloat>
{
    private static final Logger log = Logger.get(CustomVectorFloat.class);
    private final float[] values;
    private static Constructor<?> arrayVectorFloatConstructor;
    // Static initializer to find the ArrayVectorFloat constructor
    static {
        try {
            Class<?> arrayVectorFloatClass = Class.forName("io.github.jbellis.jvector.vector.ArrayVectorFloat");
            arrayVectorFloatConstructor = arrayVectorFloatClass.getDeclaredConstructor(float[].class);
            arrayVectorFloatConstructor.setAccessible(true);
        }
        catch (Exception e) {
            try {
                Package vectorPackage = VectorFloat.class.getPackage();
                String packageName = vectorPackage.getName();
                Class<?> arrayVectorFloatClass = Class.forName(packageName + ".ArrayVectorFloat");
                arrayVectorFloatConstructor = arrayVectorFloatClass.getDeclaredConstructor(float[].class);
                arrayVectorFloatConstructor.setAccessible(true);
            }
            catch (Exception e2) {
                log.warn("Could not get ArrayVectorFloat constructor using package approach: %s", e2.getMessage());
                arrayVectorFloatConstructor = null;
            }
        }
    }

    public CustomVectorFloat(float[] values)
    {
        this.values = values;
    }
    /**
     * Attempts to convert this CustomVectorFloat to an ArrayVectorFloat instance.
     *
     * @return An ArrayVectorFloat instance if conversion is possible, or this instance if not
     */
    public VectorFloat<?> toArrayVectorFloat()
    {
        if (arrayVectorFloatConstructor != null) {
            try {
                float[] copy = new float[values.length];
                System.arraycopy(values, 0, copy, 0, values.length);
                return (VectorFloat<?>) arrayVectorFloatConstructor.newInstance((Object) copy);
            }
            catch (Exception e) {
                log.warn("Failed to convert to ArrayVectorFloat: %s", e.getMessage());
            }
        }
        return this;
    }

    /**
     * Returns the vector instance.
     * Required by the VectorFloat interface.
     */
    @Override
    public CustomVectorFloat get()
    {
        return this;
    }

    @Override
    public float get(int index)
    {
        return values[index];
    }

    @Override
    public void set(int index, float value)
    {
        values[index] = value;
    }
    /**
     * Returns the length (dimension) of this vector.
     */
    @Override
    public int length()
    {
        return values.length;
    }

    @Override
    public void copyFrom(VectorFloat src, int srcOffset, int destOffset, int length)
    {
        if (src instanceof CustomVectorFloat) {
            float[] srcValues = ((CustomVectorFloat) src).values;
            System.arraycopy(srcValues, srcOffset, this.values, destOffset, length);
        }
        else {
            for (int i = 0; i < length; i++) {
                this.values[destOffset + i] = src.get(srcOffset + i);
            }
        }
    }
    @Override
    public void zero()
    {
        for (int i = 0; i < values.length; i++) {
            values[i] = 0.0f;
        }
    }

    /**
     * Returns a hash code value for this vector.
     */
    @Override
    public int getHashCode()
    {
        int result = 1;
        for (int i = 0; i < this.length(); ++i) {
            if (this.get(i) != 0.0F) {
                result = 31 * result + Float.hashCode(this.get(i));
            }
        }
        return result;
    }

    /**
     * Returns the memory usage of this vector in bytes.
     * Required by the Accountable interface.
     */
    @Override
    public long ramBytesUsed()
    {
        // Base object overhead (16 bytes) + array reference (8 bytes) + float array size
        return 16 + 8 + (long) values.length * Float.BYTES;
    }

    /**
     * Returns a copy of the internal float array.
     */
    public float[] vectorValue()
    {
        float[] copy = new float[values.length];
        System.arraycopy(values, 0, copy, 0, values.length);
        return copy;
    }

    /**
     * Creates a copy of this vector with its own independent float array.
     * @return A new CustomVectorFloat with a copy of the data
     */
    @Override
    public CustomVectorFloat copy()
    {
        float[] copy = new float[values.length];
        System.arraycopy(values, 0, copy, 0, values.length);
        return new CustomVectorFloat(copy);
    }
    /**
     * Returns the raw float array backing this vector.
     * @return The internal float array (not a copy)
     */
    public float[] getFloatArray()
    {
        return values;
    }
}
