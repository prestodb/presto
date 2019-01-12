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
package io.prestosql.array;

import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.prestosql.spi.block.Block;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import org.openjdk.jol.info.ClassLayout;

import static java.lang.String.format;
import static java.lang.reflect.Array.getLength;

// This class tracks how many times an objects have been referenced
// in order not to over count memory in complex objects such as SliceBigArray or BlockBigArray.
// Ideally the key set should be the object references themselves.
// But it turns out such implementation can lead to long concurrent marking time with G1GC.
// With several attempts including breaking down the object arrays into object big arrays,
// the problem can be solved but tracking overhead is still high.
// Benchmark results show using hash maps with primitive arrays perform way better than object arrays.
// Therefore, we use concatenation of identity hash code and size of the object to identify an object.
// This may lead to hash collision, in which case we will under count memory usage.
// But we believe the performance boost is worth the cost.
public final class ReferenceCountMap
        extends Long2IntOpenHashMap
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ReferenceCountMap.class).instanceSize();

    /**
     * Increments the reference count of an object by 1 and returns the updated reference count
     */
    public int incrementAndGet(Object key)
    {
        return addTo(getHashCode(key), 1) + 1;
    }

    /**
     * Decrements the reference count of an object by 1 and returns the updated reference count
     */
    public int decrementAndGet(Object key)
    {
        long hashCode = getHashCode(key);
        int previousCount = addTo(hashCode, -1);
        if (previousCount == 1) {
            remove(hashCode);
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

    /**
     * Get the 64-bit hash code for an object
     */
    private static long getHashCode(Object key)
    {
        // identityHashCode of two objects are not guaranteed to be different.
        // Any additional identity information can reduce collisions.
        // In the most cases below, we use size of an object to be the extra identity.
        // Experiments show that with 100 million objects created, using identityHashCode has a collision rate around 2.5%.
        // However, if these 100 million objects are combined with 10 different sizes, the collision rate is around 0.1%.
        // The collision rate can be lower than 0.001% if there are 1000 different sizes.
        int extraIdentity;
        if (key == null) {
            extraIdentity = 0;
        }
        else if (key instanceof Block) {
            extraIdentity = (int) ((Block) key).getRetainedSizeInBytes();
        }
        else if (key instanceof Slice) {
            extraIdentity = (int) ((Slice) key).getRetainedSize();
        }
        else if (key.getClass().isArray()) {
            extraIdentity = getLength(key);
        }
        else {
            throw new IllegalArgumentException(format("Unsupported type for %s", key));
        }
        return (((long) System.identityHashCode(key)) << Integer.SIZE) + extraIdentity;
    }
}
