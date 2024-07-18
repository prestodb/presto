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
package com.facebook.presto.orc.reader.vector;

import com.facebook.presto.common.predicate.vector.TupleDomainFilterVector;
import com.facebook.presto.orc.stream.DoubleInputStream;
import jdk.incubator.vector.DoubleVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorSpecies;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;

import static java.lang.Double.doubleToLongBits;

public class DoubleVectorSelectiveReadAndFilterSpecies512
        implements DoubleVectorSelectiveReadAndFilter
{
    private static final VectorSpecies<Double> speciesDouble = DoubleVector.SPECIES_512;
    private static final VectorSpecies<Integer> speciesInt = IntVector.SPECIES_256;

    @Override
    public int firstLevelReadAndFilter(TupleDomainFilterVector filter,
            DoubleInputStream inputStream,
            int[] positions,
            int positionCount,
            int[] outputPositions)
            throws IOException
    {
        int batchSize = speciesDouble.length();
        int upperBound = speciesInt.loopBound(positionCount);
        // get the memory segment
        MemorySegment segment = getMemorySegmentWrapped(upperBound, inputStream);
        DoubleVector valuesVector = null;
        int i = 0;
        int outputPositionCount = 0;
        // vectorized logic for filtering
        for (; i < upperBound; i += batchSize) {
            valuesVector = DoubleVector.fromMemorySegment(speciesDouble,
                    segment, i * Double.BYTES, ByteOrder.LITTLE_ENDIAN);
            // vectorized filtering
            VectorMask<Double> passing = filter.testDoubleVector(valuesVector);
            // positions updation
            IntVector positionsVector = IntVector.fromArray(speciesInt, positions, i);
            IntVector outputPositionsVector = positionsVector.compress(passing.cast(speciesInt));
            outputPositionsVector.intoArray(outputPositions, outputPositionCount);
            outputPositionCount += passing.trueCount();
        }
        // tail part for data which could not be fit into vectorized logic
        for (; i < positionCount; i++) {
            double value = inputStream.next();
            if (filter.testDouble(value)) {
                outputPositions[outputPositionCount] = positions[i];
                outputPositionCount++;
            }
        }

        return outputPositionCount;
    }

    @Override
    public int firstLevelReadAndFilterWithOutputRequired(TupleDomainFilterVector filter, DoubleInputStream inputStream, int[] positions, int positionCount, int[] outputPositions, long[] values)
            throws IOException
    {
        int batchSize = speciesDouble.length();
        int upperBound = speciesInt.loopBound(positionCount);

        DoubleVector valuesVector = null;
        int i = 0;
        int outputPositionCount = 0;
        // get the memory segment
        MemorySegment segment = getMemorySegmentWrapped(upperBound, inputStream);
        // vectorized logic for filtering
        for (; i < upperBound; i += batchSize) {
            valuesVector = DoubleVector.fromMemorySegment(speciesDouble,
                    segment, i * Double.BYTES, ByteOrder.LITTLE_ENDIAN);
            // vectorized filtering
            VectorMask<Double> passing = filter.testDoubleVector(valuesVector);
            // vectorized way of filling the output values
            DoubleVector outputDoubleValuesVector = valuesVector.compress(passing);
            LongVector outputLongValuesVector = outputDoubleValuesVector.viewAsIntegralLanes();
            outputLongValuesVector.intoArray(values, outputPositionCount);
            //vectorized way of handling the positions update
            IntVector positionsVector = IntVector.fromArray(speciesInt, positions, i);
            IntVector outputPositionsVector = positionsVector.compress(passing.cast(speciesInt));
            outputPositionsVector.intoArray(outputPositions, outputPositionCount);
            outputPositionCount += passing.trueCount();
        }
        // tail part for data which could not be fit into vectorized logic
        for (; i < positionCount; i++) {
            double value = inputStream.next();
            if (filter.testDouble(value)) {
                values[outputPositionCount] = doubleToLongBits(value);
                outputPositions[outputPositionCount] = positions[i];
                outputPositionCount++;
            }
        }

        return outputPositionCount;
    }

    MemorySegment getMemorySegmentWrapped(int count,
            DoubleInputStream input)
            throws IOException
    {
        byte[] doubleDataInBytes = input.readDoubleDataInBytes(count);
        MemorySegment memorySegment = MemorySegment.ofArray(doubleDataInBytes);
        return memorySegment;
    }
}
