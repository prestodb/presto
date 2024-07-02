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
package com.facebook.presto.iceberg.statistics;

import com.facebook.presto.common.type.ShortDecimalType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.statistics.ConnectorHistogram;
import com.facebook.presto.spi.statistics.Estimate;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.datasketches.common.ArrayOfBooleansSerDe;
import org.apache.datasketches.common.ArrayOfDoublesSerDe;
import org.apache.datasketches.common.ArrayOfItemsSerDe;
import org.apache.datasketches.common.ArrayOfLongsSerDe;
import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.kll.KllItemsSketch;
import org.apache.datasketches.memory.Memory;

import java.util.Comparator;
import java.util.function.Function;

import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.TypeUtils.isExactNumericType;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Verify.verify;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Objects.requireNonNull;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.EXCLUSIVE;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.INCLUSIVE;

public class KllHistogram
        implements ConnectorHistogram
{
    // since the actual type parameter is only known at runtime, we can't concretely specify it
    private final KllItemsSketch<Object> sketch;
    private final Type type;
    private final Function<Object, Double> toDouble;
    private final Function<Double, Object> fromDouble;

    @SuppressWarnings({"unchecked", "rawtypes"})
    @JsonCreator
    public KllHistogram(@JsonProperty("sketch") Slice bytes, @JsonProperty("type") Type type)
    {
        verify(isKllHistogramSupportedType(type), "histograms do not currently support type " + type.getDisplayName());
        this.type = requireNonNull(type, "type is null");
        SketchParameters parameters = getSketchParameters(type);
        // the actual sketch can only accept the same object types which generated it
        // however, the API can only accept or generate double types. We cast the inputs
        // and results to/from double to satisfy the underlying sketch type.
        if (type.getJavaType().isAssignableFrom(double.class)) {
            toDouble = x -> (double) x;
            fromDouble = x -> x;
        }
        else if (type.getJavaType().isAssignableFrom(long.class)) {
            // dual cast to auto-box/unbox from Double/Long for sketch
            toDouble = x -> (double) (long) x;
            fromDouble = x -> (long) (double) x;
        }
        else {
            throw new PrestoException(INVALID_ARGUMENTS, "can't create kll sketch from type: " + type);
        }
        sketch = KllItemsSketch.wrap(Memory.wrap(bytes.toByteBuffer(), LITTLE_ENDIAN), parameters.getComparator(), parameters.getSerde());
    }

    public static boolean isKllHistogramSupportedType(Type type)
    {
        return isExactNumericType(type) ||
                type.equals(DOUBLE) ||
                type.equals(DATE) ||
                type instanceof ShortDecimalType;
    }

    @JsonProperty
    public Slice getSketch()
    {
        return Slices.wrappedBuffer(sketch.toByteArray());
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @VisibleForTesting
    @SuppressWarnings("rawtypes")
    public KllItemsSketch getKllSketch()
    {
        return sketch;
    }

    @Override
    public Estimate cumulativeProbability(double value, boolean inclusive)
    {
        return Estimate.of(sketch.getRank(fromDouble.apply(value), inclusive ? INCLUSIVE : EXCLUSIVE));
    }

    @Override
    public Estimate inverseCumulativeProbability(double percentile)
    {
        return Estimate.of(toDouble.apply(sketch.getQuantile(percentile)));
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("type", type)
                .add("k", this.sketch.getK())
                .add("N", this.sketch.getN())
                .add("retained", this.sketch.getNumRetained())
                .add("min", this.sketch.getMinItem())
                .add("max", this.sketch.getMaxItem())
                .add("p50", sketch.getQuantile(0.5))
                .add("p75", sketch.getQuantile(0.75))
                .add("p90", sketch.getQuantile(0.90))
                .add("p99", sketch.getQuantile(0.99))
                .add("p99.9", sketch.getQuantile(0.999))
                .toString();
    }

    private static class SketchParameters<T>
    {
        private final Comparator<T> comparator;
        private final ArrayOfItemsSerDe<T> serde;

        public SketchParameters(Comparator<T> comparator, ArrayOfItemsSerDe<T> serde)
        {
            this.comparator = comparator;
            this.serde = serde;
        }

        public Comparator<T> getComparator()
        {
            return comparator;
        }

        public ArrayOfItemsSerDe<T> getSerde()
        {
            return serde;
        }
    }

    private static SketchParameters<?> getSketchParameters(Type type)
    {
        if (type.getJavaType().isAssignableFrom(double.class)) {
            return new SketchParameters<>(Double::compareTo, new ArrayOfDoublesSerDe());
        }
        else if (type.getJavaType().isAssignableFrom(long.class)) {
            return new SketchParameters<>(Long::compareTo, new ArrayOfLongsSerDe());
        }
        else if (type.getJavaType().isAssignableFrom(Slice.class)) {
            return new SketchParameters<>(String::compareTo, new ArrayOfStringsSerDe());
        }
        else if (type.getJavaType().isAssignableFrom(boolean.class)) {
            return new SketchParameters<>(Boolean::compareTo, new ArrayOfBooleansSerDe());
        }
        else {
            throw new PrestoException(INVALID_ARGUMENTS, "failed to deserialize KLL Sketch. No suitable type found for " + type);
        }
    }
}
