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
package com.facebook.presto.operator;

import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class AriaHash
{
    private static final Unsafe unsafe;

    static {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);
            if (unsafe == null) {
                throw new RuntimeException("Unsafe access not available");
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean recycleTable;

    private static List<Slice> sliceReserve = new ArrayList<>();

    static void clearAllocCache()
    {
        sliceReserve.clear();
    }

    private AriaHash()
    { }

    static Slice getSlice()
    {
        if (recycleTable) {
            synchronized (sliceReserve) {
                if (!sliceReserve.isEmpty()) {
                    return sliceReserve.remove(sliceReserve.size() - 1);
                }
            }
        }
        return Slices.allocate((128 * 1024));
    }

    static void releaseSlice(Slice slice)
    {
        if (recycleTable) {
            synchronized (sliceReserve) {
                sliceReserve.add(slice);
            }
        }
    }

    static boolean supportsLayout(List<Type> types, List<Integer> hashChannels, List<Integer> outputChannels)
    {
        return supportsLayout(
                hashChannels.stream().map(types::get).collect(toImmutableList()),
                outputChannels.stream().map(types::get).collect(toImmutableList()));
    }

    public static boolean supportsLayout(LookupSourceFactory lookupSourceFactory)
    {
        if (!(lookupSourceFactory instanceof PartitionedLookupSourceFactory)) {
            return false;
        }
        PartitionedLookupSourceFactory partitionedFactory = (PartitionedLookupSourceFactory) lookupSourceFactory;
        List<Type> hashChannelTypes = partitionedFactory.getHashChannelTypes();
        List<Type> outputChannelTypes = lookupSourceFactory.getOutputTypes();
        return supportsLayout(hashChannelTypes, outputChannelTypes);
    }

    private static boolean supportsLayout(List<Type> hashChannelTypes, List<Type> outputChannelTypes)
    {
        return hashChannelTypes.size() == 2
                && outputChannelTypes.size() == 1
                && hashChannelTypes.get(0) == BIGINT
                && hashChannelTypes.get(1) == BIGINT
                && outputChannelTypes.get(0) == DOUBLE;
    }
}
