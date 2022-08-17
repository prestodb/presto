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
package com.facebook.presto.tpch.statistics;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import io.airlift.tpch.TpchColumnType;
import net.agkn.hll.HLL;
import net.agkn.hll.HLLType;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

class ColumnStatisticsRecorder
{
    final MinMaxSet<Object> minMaxValues = new MinMaxSet<>();

    /**
     * We use a HLL that provides an exact count till 2^18 values.
     * Choice for {@code log2m} and {@code regwidth} parameters was determined empirically to keep total memory usage in check
     */
    final HLL hll = new HLL(25, 8, 18, true, HLLType.EMPTY);
    private final TpchColumnType type;
    private final HashFunction hashFunction = Hashing.murmur3_128();
    long varCharSize;

    public ColumnStatisticsRecorder(TpchColumnType type)
    {
        this.type = requireNonNull(type, "type is null");
    }

    void record(Comparable<?> value)
    {
        if (value != null) {
            final Hasher hasher = hashFunction.newHasher();

            switch (type.getBase()) {
                case IDENTIFIER:
                    hasher.putLong((Long) value);
                    break;
                case INTEGER:
                case DATE:
                    hasher.putInt((Integer) value);
                    break;
                case DOUBLE:
                    hasher.putDouble((Double) value);
                    break;
                case VARCHAR:
                    hasher.putString((String) value, StandardCharsets.UTF_8);
                    break;
            }
            hll.addRaw(hasher.hash().asLong());

            minMaxValues.add(value);

            if (type.getBase() == TpchColumnType.Base.VARCHAR) {
                varCharSize += ((String) value).length();
            }
        }
    }

    /**
     * Merge statistics from another {@link ColumnStatisticsRecorder} into the current object
     *
     * @param other
     * @return
     */
    public ColumnStatisticsRecorder mergeWith(ColumnStatisticsRecorder other)
    {
        checkArgument(type.equals(other.type), "Merging incompatible column statistics");
        varCharSize += other.varCharSize;
        other.minMaxValues.getMin().ifPresent(minMaxValues::add);
        other.minMaxValues.getMax().ifPresent(minMaxValues::add);
        hll.union(other.hll);
        return this;
    }

    ColumnStatisticsData getRecording()
    {
        return new ColumnStatisticsData(
                Optional.of(getUniqueValuesCount()),
                getLowestValue(),
                getHighestValue(),
                getDataSize());
    }

    private long getUniqueValuesCount()
    {
        return hll.cardinality();
    }

    private Optional<Object> getLowestValue()
    {
        return minMaxValues.getMin();
    }

    private Optional<Object> getHighestValue()
    {
        return minMaxValues.getMax();
    }

    public Optional<Long> getDataSize()
    {
        if (type.getBase() == TpchColumnType.Base.VARCHAR) {
            return Optional.of(varCharSize);
        }
        return Optional.empty();
    }
}
