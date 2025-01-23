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
package com.facebook.presto.operator.aggregation.noisyaggregation;

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.operator.aggregation.noisyaggregation.sketch.SfmSketch;
import com.facebook.presto.spi.PrestoException;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.facebook.presto.util.Failures.internalError;

public final class SfmSketchAggregationUtils
{
    public static final int DEFAULT_BUCKET_COUNT = 4096;
    public static final int DEFAULT_PRECISION = 24;

    private SfmSketchAggregationUtils() {}

    public static void ensureStateInitialized(SfmSketchState state, double epsilon, long numberOfBuckets, long precision)
    {
        if (state.getSketch() != null) {
            return; // already initialized
        }

        validateSketchParameters(epsilon, (int) numberOfBuckets, (int) precision);
        SfmSketch sketch = SfmSketch.create((int) numberOfBuckets, (int) precision);
        state.setSketch(sketch);
        state.setEpsilon(epsilon);
    }

    public static void validateSketchParameters(double epsilon, int numberOfBuckets, int precision)
    {
        checkCondition(epsilon > 0,
                INVALID_FUNCTION_ARGUMENT, "epsilon must be positive");
        checkCondition(numberOfBuckets > 0 && (numberOfBuckets & (numberOfBuckets - 1)) == 0,
                INVALID_FUNCTION_ARGUMENT, "numberOfBuckets must be a power of 2");
        checkCondition(precision > 0 && precision % Byte.SIZE == 0,
                INVALID_FUNCTION_ARGUMENT, "precision must be a multiple of %s", Byte.SIZE);
    }

    public static void addHashToSketch(SfmSketchState state, long hash, double epsilon, long numberOfBuckets, long precision)
    {
        ensureStateInitialized(state, epsilon, numberOfBuckets, precision);
        state.getSketch().addHash(hash);
    }

    public static void addIndexAndZerosToSketch(SfmSketchState state, long index, long zeros, double epsilon, long numberOfBuckets, long precision)
    {
        ensureStateInitialized(state, epsilon, numberOfBuckets, precision);
        state.getSketch().addIndexAndZeros(index, zeros);
    }

    public static long hashLong(MethodHandle methodHandle, long value)
    {
        long hash;
        try {
            hash = (long) methodHandle.invokeExact(value);
        }
        catch (Throwable t) {
            throw internalError(t);
        }
        return hash;
    }

    public static long hashDouble(MethodHandle methodHandle, double value)
    {
        long hash;
        try {
            hash = (long) methodHandle.invokeExact(value);
        }
        catch (Throwable t) {
            throw internalError(t);
        }
        return hash;
    }

    public static long hashSlice(MethodHandle methodHandle, Slice value)
    {
        long hash;
        try {
            hash = (long) methodHandle.invokeExact(value);
        }
        catch (Throwable t) {
            throw internalError(t);
        }
        return hash;
    }

    public static void mergeStates(SfmSketchState state, SfmSketchState otherState)
    {
        SfmSketch sketch = state.getSketch();
        SfmSketch otherSketch = otherState.getSketch();
        if (sketch == null) {
            state.setSketch(otherSketch);
            state.setEpsilon(otherState.getEpsilon());
        }
        else {
            try {
                // Throws if the sketches are incompatible (e.g., different bucket counts/size)
                // Catch and throw a PrestoException
                state.mergeSketch(otherSketch);
            }
            catch (IllegalArgumentException e) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e.getMessage(), e);
            }
        }
    }

    public static void writeCardinality(SfmSketchState state, BlockBuilder out)
    {
        SfmSketch sketch = state.getSketch();
        if (sketch == null) {
            // In the event we process no rows, output NULL.
            // Note: Although the SfmSketch is differentially private, this particular output is not.
            // We cannot output anything DP here because the state will not include epsilon if we processed no rows.
            out.appendNull();
        }
        else {
            sketch.enablePrivacy(state.getEpsilon());
            BIGINT.writeLong(out, sketch.cardinality());
        }
    }

    public static void writeSketch(SfmSketchState state, BlockBuilder out)
    {
        SfmSketch sketch = state.getSketch();
        if (sketch == null) {
            // In the event we process no rows, output NULL.
            // Note: Although the SfmSketch is differentially private, this particular output is not.
            // We cannot output anything DP here because the state will not include epsilon if we processed no rows.
            out.appendNull();
        }
        else {
            sketch.enablePrivacy(state.getEpsilon());
            VARBINARY.writeSlice(out, sketch.serialize());
        }
    }
}
