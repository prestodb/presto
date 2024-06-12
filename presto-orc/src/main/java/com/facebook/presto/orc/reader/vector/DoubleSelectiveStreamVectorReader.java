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

import com.facebook.presto.common.predicate.TupleDomainFilter;
import com.facebook.presto.common.predicate.vector.DoubleRangeVector;
import com.facebook.presto.common.predicate.vector.TupleDomainFilterVector;
import com.facebook.presto.orc.OrcLocalMemoryContext;
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.reader.DoubleSelectiveStreamReader;
import jdk.incubator.vector.DoubleVector;
import jdk.incubator.vector.VectorShape;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.util.Optional;

public class DoubleSelectiveStreamVectorReader
        extends DoubleSelectiveStreamReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(DoubleSelectiveStreamVectorReader.class).instanceSize();

    public DoubleSelectiveStreamVectorReader(
            StreamDescriptor streamDescriptor,
            Optional<TupleDomainFilter> filter,
            boolean outputRequired,
            OrcLocalMemoryContext systemMemoryContext)
    {
        super(streamDescriptor, filter, outputRequired, systemMemoryContext);
    }

    @Override
    protected int readWithFilter(int[] positions, int positionCount)
            throws IOException
    {
        outputPositionCount = 0;
        if ((positions[positionCount - 1] == positionCount - 1)
                && (null == presentStream)
                && (VectorShape.S_512_BIT.vectorBitSize() == DoubleVector.SPECIES_PREFERRED.vectorBitSize())
                && !dataStream.isDataCompressedOrEncrypted()) {
            TupleDomainFilterVector vectorFilter = new DoubleRangeVector((TupleDomainFilter.DoubleRange) filter);
            // no skipping and no nulls
            // get optimized filter and reader
            DoubleVectorSelectiveReadAndFilter optimizedReadAndFilter = new DoubleVectorSelectiveReadAndFilterSpecies512();
            if (outputRequired) {
                // output required case
                outputPositionCount = optimizedReadAndFilter.firstLevelReadAndFilterWithOutputRequired(vectorFilter,
                        dataStream, positions, positionCount, outputPositions, values);
            }
            else {
                outputPositionCount = optimizedReadAndFilter.firstLevelReadAndFilter(vectorFilter,
                        dataStream, positions, positionCount, outputPositions);
            }
            return positionCount;
        }
        return super.readWithFilter(positions, positionCount);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return super.getRetainedSizeInBytes() + INSTANCE_SIZE;
    }
}
