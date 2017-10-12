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
package com.facebook.presto.array;

import io.airlift.slice.SizeOf;
import it.unimi.dsi.fastutil.objects.Object2IntOpenCustomHashMap;
import org.openjdk.jol.info.ClassLayout;

public final class ReferenceCountMap
        extends Object2IntOpenCustomHashMap<Object>
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ReferenceCountMap.class).instanceSize();

    /**
     * Two different blocks can share the same underlying data
     * Use the map to avoid memory over counting
     */
    public ReferenceCountMap()
    {
        super(new ObjectStrategy());
    }

    /**
     * Increments the reference count of an object by 1 and returns the updated reference count
     */
    public int incrementAndGet(Object key)
    {
        return addTo(key, 1) + 1;
    }

    /**
     * Decrements the reference count of an object by 1 and returns the updated reference count
     */
    public int decrementAndGet(Object key)
    {
        int previousCount = addTo(key, -1);
        if (previousCount == 1) {
            remove(key);
        }
        return previousCount - 1;
    }

    /**
     * Returns the size of this map in bytes.
     */
    public long sizeOf()
    {
        return INSTANCE_SIZE + SizeOf.sizeOf(key) + SizeOf.sizeOf(value) + SizeOf.sizeOf(used);
    }

    private static final class ObjectStrategy
            implements Strategy<Object>
    {
        @Override
        public int hashCode(Object object)
        {
            return System.identityHashCode(object);
        }

        @Override
        public boolean equals(Object left, Object right)
        {
            return left == right;
        }
    }
}
