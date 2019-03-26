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
package com.facebook.presto.orc.stream;

import com.facebook.presto.orc.checkpoint.LongStreamCheckpoint;
import com.facebook.presto.orc.checkpoint.LongStreamDwrfCheckpoint;
import com.facebook.presto.orc.metadata.OrcType.OrcTypeKind;

import java.io.IOException;

import static com.facebook.presto.orc.stream.LongDecode.readDwrfLong;

public class LongInputStreamDwrf
        implements LongInputStream
{
    private final OrcInputStream input;
    private final OrcTypeKind orcTypeKind;
    private final boolean signed;
    private final boolean usesVInt;

    // Position of the first value of the run in literals from the checkpoint.
    private int currentRunOffset;

    public LongInputStreamDwrf(OrcInputStream input, OrcTypeKind type, boolean signed, boolean usesVInt)
    {
        this.input = input;
        this.orcTypeKind = type;
        this.signed = signed;
        this.usesVInt = usesVInt;
    }

    @Override
    public Class<LongStreamDwrfCheckpoint> getCheckpointType()
    {
        return LongStreamDwrfCheckpoint.class;
    }

    @Override
    public void seekToCheckpoint(LongStreamCheckpoint checkpoint)
            throws IOException
    {
        LongStreamDwrfCheckpoint dwrfCheckpoint = (LongStreamDwrfCheckpoint) checkpoint;
        input.seekToCheckpoint(dwrfCheckpoint.getInputStreamCheckpoint());
        currentRunOffset = 0;
    }

    @Override
    public void skip(long items)
            throws IOException
    {
        // there is no fast way to skip values
        for (long i = 0; i < items; i++) {
            next();
        }
    }

    @Override
    public long next()
            throws IOException
    {
        currentRunOffset++;
        return readDwrfLong(input, orcTypeKind, signed, usesVInt);
    }

    public int scan(
            int[] offsets,
            int beginOffset,
            int numOffsets,
            int endOffset,
            ResultsConsumer resultsConsumer)
            throws IOException
    {
        int numResults = 0;
        for (int offsetIdx = beginOffset; offsetIdx < beginOffset + numOffsets; offsetIdx++) {
            skip(offsets[offsetIdx] - currentRunOffset);
            if (resultsConsumer.consume(offsetIdx, next())) {
                numResults++;
            }
        }
        skip(endOffset - offsets[beginOffset + numOffsets - 1] - 1);
        return numResults;
    }
}
