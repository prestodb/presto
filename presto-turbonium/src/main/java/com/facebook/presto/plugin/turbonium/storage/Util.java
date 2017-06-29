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
package com.facebook.presto.plugin.turbonium.storage;

import com.facebook.presto.plugin.turbonium.stats.Stats;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.primitives.Primitives;
import org.openjdk.jol.info.ClassLayout;

import java.util.BitSet;

public class Util
{
    private Util() {}

    private static final int BITSET_SIZE = ClassLayout.parseClass(BitSet.class).instanceSize();
    private static final int ADDRESS_BITS = 3;

    private static long longValuesBytes(long bits)
    {
        return bits >> ADDRESS_BITS;
    }

    public static long sizeOfBitSet(BitSet bitSet)
    {
        return BITSET_SIZE + longValuesBytes(bitSet.size());
    }

    public static <T> Domain createDomain(Type type, Stats<T> stats)
    {
        // If encoding is disabled and segment contains only nulls or is empty
        if (!stats.getMin().isPresent()) {
            if (stats.size() == 0) {
                return Domain.none(type);
            }
            else if (stats.getNonNullCount() == 0) {
                return Domain.onlyNull(type);
            }
            else {
                return Domain.all(type);
            }
        }
        T min = stats.getMin().get();
        T max = stats.getMax().get();
        if (min.getClass().equals(Integer.class) || min.getClass().equals(Short.class) || min.getClass().equals(Byte.class)) {
            return Domain.create(
                    ValueSet.ofRanges(
                            Range.range(
                                    type,
                                    Primitives.wrap(type.getJavaType()).cast(((Number) min).longValue()),
                                    true,
                                    Primitives.wrap(type.getJavaType()).cast(((Number) max).longValue()),
                                    true)), true);
        }
        return Domain.create(
                ValueSet.ofRanges(
                        Range.range(
                                type,
                                min,
                                true,
                                max,
                                true)), true);
    }
}
