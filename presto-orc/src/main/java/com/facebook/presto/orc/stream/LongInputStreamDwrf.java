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

public class LongInputStreamDwrf
        implements LongInputStream
{
    private final OrcInputStream input;
    private final OrcTypeKind orcTypeKind;
    private final boolean signed;
    private final boolean usesVInt;

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
    }

    @Override
    public void skip(long items)
            throws IOException
    {
        if (usesVInt) {
            input.skipVarints(items);
        }
        else {
            input.skipDwrfLong(orcTypeKind, items);
        }
    }

    @Override
    public long next()
            throws IOException
    {
        if (usesVInt) {
            return input.readVarint(signed);
        }
        return input.readDwrfLong(orcTypeKind);
    }

    @Override
    public void next(long[] values, int items)
            throws IOException
    {
        for (int i = 0; i < items; i++) {
            values[i] = next();
        }
    }

    @Override
    public void next(int[] values, int items)
            throws IOException
    {
        for (int i = 0; i < items; i++) {
            values[i] = (int) next();
        }
    }

    @Override
    public void next(short[] values, int items)
            throws IOException
    {
        for (int i = 0; i < items; i++) {
            values[i] = (short) next();
        }
    }
}
