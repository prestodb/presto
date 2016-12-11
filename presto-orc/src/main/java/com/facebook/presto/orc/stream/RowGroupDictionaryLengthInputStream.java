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
import com.facebook.presto.orc.checkpoint.RowGroupDictionaryLengthStreamCheckpoint;

import java.io.IOException;

public class RowGroupDictionaryLengthInputStream
        extends LongInputStreamV1
{
    private int entryCount = -1;

    public RowGroupDictionaryLengthInputStream(OrcInputStream input, boolean signed)
    {
        super(input, signed);
    }

    public int getEntryCount()
    {
        return entryCount;
    }

    @Override
    public Class<RowGroupDictionaryLengthStreamCheckpoint> getCheckpointType()
    {
        return RowGroupDictionaryLengthStreamCheckpoint.class;
    }

    @Override
    public void seekToCheckpoint(LongStreamCheckpoint checkpoint)
            throws IOException
    {
        super.seekToCheckpoint(checkpoint);
        RowGroupDictionaryLengthStreamCheckpoint rowGroupDictionaryLengthStreamCheckpoint = (RowGroupDictionaryLengthStreamCheckpoint) checkpoint;
        entryCount = rowGroupDictionaryLengthStreamCheckpoint.getRowGroupDictionarySize();
    }
}
