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

import com.facebook.presto.common.type.AbstractIntType;
import com.facebook.presto.common.type.AbstractLongType;
import com.facebook.presto.common.type.AbstractVarcharType;
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
import org.openjdk.jol.info.ClassLayout;

import java.util.Comparator;
import java.util.function.Function;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.Decimals.isLongDecimal;
import static com.facebook.presto.common.type.Decimals.isShortDecimal;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.TypeUtils.isNumericType;
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
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(KllHistogram.class).instanceSize();
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
        if (parameters.getSerde().getClassOfT().equals(Double.class)) {
            toDouble = x -> (double) x;
            fromDouble = x -> x;
        }
        else if (parameters.getSerde().getClassOfT().equals(Long.class)) {
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
        try {
            return isNumericType(type) ||
                    type instanceof AbstractIntType;
        }
        catch (PrestoException e) {
            return false;
        }
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

    /**
     * The memory utilization is dominated by the size of the sketch. This estimate
     * doesn't account for the other fields in the class.
     */
    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE + sketch.getSerializedSizeBytes();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("type", type)
                .add("k", this.sketch.getK())
                .add("N", this.sketch.getN())
                .add("retained", this.sketch.getNumRetained())
                .add("mingetSerialized", this.sketch.getMinItem())
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
        if (type.equals(REAL)) {
            return new SketchParameters<>(Double::compareTo, new ArrayOfDoublesSerDe());
        }
        else if (isShortDecimal(type)) {
            return new SketchParameters<>(Double::compareTo, new ArrayOfDoublesSerDe());
        }
        else if (isLongDecimal(type)) {
            return new SketchParameters<>(Double::compareTo, new ArrayOfDoublesSerDe());
        }
        else if (type.equals(DOUBLE)) {
            return new SketchParameters<>(Double::compareTo, new ArrayOfDoublesSerDe());
        }
        else if (type.equals(BOOLEAN)) {
            return new SketchParameters<>(Boolean::compareTo, new ArrayOfBooleansSerDe());
        }
        else if (type instanceof AbstractIntType || type instanceof AbstractLongType || type.equals(SMALLINT) || type.equals(TINYINT)) {
            return new SketchParameters<>(Long::compareTo, new ArrayOfLongsSerDe());
        }
        else if (type instanceof AbstractVarcharType) {
            return new SketchParameters<>(String::compareTo, new ArrayOfStringsSerDe());
        }
        else {
            throw new PrestoException(INVALID_ARGUMENTS, "Unsupported type for KLL sketch: " + type);
        }
    }
}
