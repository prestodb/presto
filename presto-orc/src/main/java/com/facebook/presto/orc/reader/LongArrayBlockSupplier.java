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
package com.facebook.presto.orc.reader;

import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.OrcDataSourceId;
import com.facebook.presto.orc.stream.BooleanInputStream;
import com.facebook.presto.orc.stream.LongInputStream;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.LongArrayBlock;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;

import java.io.IOException;
import java.util.Optional;

import static com.google.common.base.Verify.verify;

public class LongArrayBlockSupplier
        implements BlockSupplier
{
    @Override
    public Block provideFromDirectStream(OrcDataSourceId dataSourceId, int batchSize, BooleanInputStream presentStream, LongInputStream dataStream)
            throws IOException
    {
        if (presentStream == null) {
            if (dataStream == null) {
                throw new OrcCorruptionException(dataSourceId, "Value is not null but data stream is not present");
            }
            return populateNonNullBlock(batchSize, dataStream);
        }
        boolean[] isNull = new boolean[batchSize];
        int nullCount = presentStream.getUnsetBits(batchSize, isNull);
        if (nullCount == 0) {
            verify(dataStream != null);
            return populateNonNullBlock(batchSize, dataStream);
        }
        if (nullCount != batchSize) {
            verify(dataStream != null);
            return populateBlockWithNull(batchSize, dataStream, isNull);
        }
        return new RunLengthEncodedBlock(
                new LongArrayBlock(1,
                        Optional.of(new boolean[] {true}),
                        new long[] {0}), batchSize);
    }

    private Block populateNonNullBlock(int batchSize, LongInputStream dataStream)
            throws IOException
    {
        long[] vector = new long[batchSize];
        dataStream.nextLongVector(batchSize, vector);
        return new LongArrayBlock(batchSize, Optional.empty(), vector);
    }

    private Block populateBlockWithNull(int batchSize, LongInputStream dataStream, boolean[] isNull)
            throws IOException
    {
        long[] vector = new long[batchSize];
        verify(dataStream != null);
        dataStream.nextLongVector(batchSize, vector, isNull);
        return new LongArrayBlock(batchSize, Optional.of(isNull), vector);
    }

    @Override
    public Block provideFromDictionaryStream(OrcDataSourceId dataSourceId, int batchSize, BooleanInputStream presentStream, LongInputStream dataStream, long[] dictionary, BooleanInputStream inDictionaryStream)
            throws IOException
    {
        if (presentStream == null) {
            // Data doesn't have nulls
            if (dataStream == null) {
                throw new OrcCorruptionException(dataSourceId, "Value is not null but data stream is not present");
            }
            return populateNonNullBlock(batchSize, dataStream, dictionary, inDictionaryStream);
        }
        boolean[] isNull = new boolean[batchSize];
        int nullValues = presentStream.getUnsetBits(batchSize, isNull);
        // Data has nulls
        if (dataStream == null) {
            // The only valid case for dataStream is null when data has nulls is that all values are nulls.
            if (nullValues != batchSize) {
                throw new OrcCorruptionException(dataSourceId, "Value is not null but data stream is not present");
            }
            return new RunLengthEncodedBlock(new LongArrayBlock(1, Optional.of(new boolean[] {true}),
                    new long[] {0}), batchSize);
        }
        if (nullValues == 0) {
            return populateNonNullBlock(batchSize, dataStream, dictionary, inDictionaryStream);
        }
        return populateBlockWithNull(batchSize, isNull, dataStream, dictionary, inDictionaryStream);
    }

    private Block populateNonNullBlock(int batchSize, LongInputStream dataStream, long[] dictionary, BooleanInputStream inDictionaryStream)
            throws IOException
    {
        long[] vector = new long[batchSize];
        if (inDictionaryStream == null) {
            for (int i = 0; i < batchSize; i++) {
                vector[i] = dictionary[((int) dataStream.next())];
            }
        }
        else {
            boolean[] inDictionary = new boolean[batchSize];
            int inCount = inDictionaryStream.getSetBits(batchSize, inDictionary);
            if (inCount == 0) {
                dataStream.nextLongVector(batchSize, vector);
            }
            for (int i = 0; i < batchSize; i++) {
                long id = dataStream.next();
                if (inDictionary[i]) {
                    vector[i] = dictionary[(int) id];
                }
                else {
                    vector[i] = id;
                }
            }
        }
        return new LongArrayBlock(batchSize, Optional.empty(), vector);
    }

    private Block populateBlockWithNull(int batchSize, boolean[] isNull, LongInputStream dataStream, long[] dictionary, BooleanInputStream inDictionaryStream)
            throws IOException
    {
        long[] vector = new long[batchSize];
        if (inDictionaryStream == null) {
            for (int i = 0; i < batchSize; i++) {
                if (!isNull[i]) {
                    vector[i] = dictionary[((int) dataStream.next())];
                }
            }
        }
        else {
            boolean[] inDictionary = new boolean[batchSize];
            int inCount = inDictionaryStream.getSetBits(batchSize, inDictionary);
            if (inCount == 0) {
                dataStream.nextLongVector(batchSize, vector, isNull);
            }
            for (int i = 0; i < batchSize; i++) {
                long id = dataStream.next();
                if (inDictionary[i]) {
                    vector[i] = dictionary[(int) id];
                }
                else {
                    vector[i] = id;
                }
            }
        }
        return new LongArrayBlock(batchSize, Optional.of(isNull), vector);
    }
}
