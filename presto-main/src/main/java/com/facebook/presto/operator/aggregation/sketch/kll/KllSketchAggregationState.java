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
package com.facebook.presto.operator.aggregation.sketch.kll;

import com.facebook.presto.common.array.ObjectBigArray;
import com.facebook.presto.common.type.AbstractIntType;
import com.facebook.presto.common.type.AbstractLongType;
import com.facebook.presto.common.type.AbstractVarcharType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.SqlDecimal;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.aggregation.sketch.theta.ThetaSketchStateFactory;
import com.facebook.presto.operator.aggregation.state.AbstractGroupedAccumulatorState;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.AccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateMetadata;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import org.apache.datasketches.common.ArrayOfBooleansSerDe;
import org.apache.datasketches.common.ArrayOfDoublesSerDe;
import org.apache.datasketches.common.ArrayOfItemsSerDe;
import org.apache.datasketches.common.ArrayOfLongsSerDe;
import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.kll.KllItemsSketch;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.math.BigInteger;
import java.util.Comparator;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.Decimals.isLongDecimal;
import static com.facebook.presto.common.type.Decimals.isShortDecimal;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static java.util.Objects.requireNonNull;

@AccumulatorStateMetadata(stateFactoryClass = KllSketchStateFactory.class, stateSerializerClass = KllSketchStateSerializer.class)
public interface KllSketchAggregationState
        extends AccumulatorState
{
    /**
     * These parameters were created by running a linear regression against the current datasketches
     * library version and the true in-memory size with k = 200. Upgrades to the datasketches
     * library should attempt to re-compute these parameters.
     */
    Map<Class<?>, double[]> SIZE_ESTIMATOR_PARAMETERS = ImmutableMap.<Class<?>, double[]>builder()
            .put(double.class, new double[] {929.0746716729514, 3.3328879969131457})
            .put(long.class, new double[] {929.920850240635, 3.3325974295999283})
            .put(Slice.class, new double[] {1008.5483794752541, 4.234749475485517})
            .put(boolean.class, new double[] {345.60095530779705, 14.12193288555095})
            .build();

    long SKETCH_INSTANCE_SIZE = ClassLayout.parseClass(KllItemsSketch.class).instanceSize();

    @Nullable
    <T> KllItemsSketch<T> getSketch();

    void addMemoryUsage(Supplier<Long> usage);

    Type getType();

    <T> void setSketch(KllItemsSketch<T> sketch);

    void setConversion(Function<Object, Object> conversion);

    <T> void update(T item);

    class SketchParameters<T>
    {
        private final Comparator<T> comparator;
        private final ArrayOfItemsSerDe<T> serde;
        private final Function<Object, Object> conversion;

        public SketchParameters(Comparator<T> comparator, ArrayOfItemsSerDe<T> serde, Function<Object, Object> conversion)
        {
            this.comparator = comparator;
            this.serde = serde;
            this.conversion = conversion;
        }

        public SketchParameters(Comparator<T> comparator, ArrayOfItemsSerDe<T> serde)
        {
            this(comparator, serde, Function.identity());
        }

        public Comparator<T> getComparator()
        {
            return comparator;
        }

        public ArrayOfItemsSerDe<T> getSerde()
        {
            return serde;
        }

        public Function<Object, Object> getConversion()
        {
            return conversion;
        }
    }

    class Single
            implements KllSketchAggregationState
    {
        private static final long INSTANCE_SIZE = ClassLayout.parseClass(Single.class).instanceSize();
        @Nullable
        private KllItemsSketch sketch;
        private final Type type;
        private Function<Object, Object> conversion = Function.identity();

        public Single(Type type)
        {
            this.type = requireNonNull(type, "type is null");
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> KllItemsSketch<T> getSketch()
        {
            return sketch;
        }

        @Override
        public <T> void setSketch(KllItemsSketch<T> sketch)
        {
            this.sketch = sketch;
        }

        @Override
        public void setConversion(Function<Object, Object> conversion)
        {
            this.conversion = conversion;
        }

        @Override
        public <T> void update(T item)
        {
            sketch.update(conversion.apply(item));
        }

        @Override
        public void addMemoryUsage(Supplier<Long> usage)
        {
            // noop
        }

        @Override
        public Type getType()
        {
            return type;
        }

        @Override
        public long getEstimatedSize()
        {
            if (sketch != null) {
                return SKETCH_INSTANCE_SIZE +
                        INSTANCE_SIZE +
                        getEstimatedKllInMemorySize(sketch, type.getJavaType());
            }

            return INSTANCE_SIZE;
        }
    }

    class Grouped
            extends AbstractGroupedAccumulatorState
            implements KllSketchAggregationState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(ThetaSketchStateFactory.GroupedThetaSketchState.class).instanceSize();
        private final ObjectBigArray<KllItemsSketch> sketches = new ObjectBigArray<>();
        private long accumulatedSizeInBytes;

        private final Type type;
        private Function<Object, Object> conversion = Function.identity();

        public Grouped(Type type)
        {
            this.type = requireNonNull(type, "type is null");
        }

        @Override
        public <T> KllItemsSketch<T> getSketch()
        {
            return sketches.get(getGroupId());
        }

        @Override
        public void addMemoryUsage(Supplier<Long> usage)
        {
            accumulatedSizeInBytes += usage.get();
        }

        @Override
        public Type getType()
        {
            return type;
        }

        @Override
        public <T> void setSketch(KllItemsSketch<T> sketch)
        {
            sketches.set(getGroupId(), requireNonNull(sketch, "kll sketch is null"));
        }

        @Override
        public void setConversion(Function<Object, Object> conversion)
        {
            this.conversion = conversion;
        }

        @Override
        public <T> void update(T item)
        {
            getSketch().update(conversion.apply(item));
        }

        @Override
        public long getEstimatedSize()
        {
            return sketches.sizeOf() + INSTANCE_SIZE + accumulatedSizeInBytes;
        }

        @Override
        public void ensureCapacity(long size)
        {
            sketches.ensureCapacity(size);
        }
    }

    static long getEstimatedKllInMemorySize(@Nullable KllItemsSketch<?> sketch, Class<?> type)
    {
        if (sketch == null) {
            return 0;
        }
        double[] parameters = SIZE_ESTIMATOR_PARAMETERS.get(type);
        if (parameters == null) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "unsupported parameter class: " + type.getName());
        }
        double linear = parameters[1];
        double constant = parameters[0];
        return (long) ((linear * sketch.getSerializedSizeBytes()) + constant);
    }

    static SketchParameters<?> getSketchParameters(Type type)
    {
        // catch specific cases first (e.g. real and short decimal), moving to wider comparisons
        // later
        if (type.equals(REAL)) {
            return new SketchParameters<>(Double::compareTo, new ArrayOfDoublesSerDe(),
                    (Object intValue) -> (double) Float.intBitsToFloat(((Long) intValue).intValue()));
        }
        else if (isShortDecimal(type)) {
            DecimalType decimalType = (DecimalType) type;
            return new SketchParameters<>(Double::compareTo, new ArrayOfDoublesSerDe(),
                    (Object longValue) -> new SqlDecimal(BigInteger.valueOf((long) longValue), decimalType.getScale(), decimalType.getScale()).toBigDecimal().doubleValue());
        }
        else if (isLongDecimal(type)) {
            DecimalType decimalType = (DecimalType) type;
            return new SketchParameters<>(Double::compareTo, new ArrayOfDoublesSerDe(),
                    (Object encodedDecimal) -> new SqlDecimal(Decimals.decodeUnscaledValue((Slice) encodedDecimal), decimalType.getScale(), decimalType.getPrecision()).toBigDecimal().doubleValue());
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
            return new SketchParameters<>(String::compareTo, new ArrayOfStringsSerDe(), (Object slice) -> ((Slice) slice).toStringUtf8());
        }
        else {
            throw new PrestoException(INVALID_ARGUMENTS, "Unsupported type for KLL sketch: " + type);
        }
    }
}
