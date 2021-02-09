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

import com.facebook.presto.orc.DwrfDataEncryptor;
import com.facebook.presto.orc.OrcOutputBuffer;
import com.facebook.presto.orc.checkpoint.BooleanStreamCheckpoint;
import com.facebook.presto.orc.metadata.CompressionParameters;
import com.facebook.presto.orc.metadata.Stream;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.toIntExact;

public class PresentOutputStream
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(PresentOutputStream.class).instanceSize();
    private final OrcOutputBuffer buffer;

    // boolean stream will only exist if null values being recorded
    @Nullable
    private BooleanOutputStream booleanOutputStream;

    private final List<Integer> groupsCounts = new ArrayList<>();
    private int currentGroupCount;

    private boolean closed;

    public PresentOutputStream(CompressionParameters compressionParameters, Optional<DwrfDataEncryptor> dwrfEncryptor)
    {
        this.buffer = new OrcOutputBuffer(compressionParameters, dwrfEncryptor);
    }

    public void writeBoolean(boolean value)
    {
        checkArgument(!closed);
        if (!value && booleanOutputStream == null) {
            createBooleanOutputStream();
        }

        if (booleanOutputStream != null) {
            booleanOutputStream.writeBoolean(value);
        }
        currentGroupCount++;
    }

    private void createBooleanOutputStream()
    {
        checkState(booleanOutputStream == null);
        booleanOutputStream = new BooleanOutputStream(buffer);
        for (int groupsCount : groupsCounts) {
            booleanOutputStream.writeBooleans(groupsCount, true);
            booleanOutputStream.recordCheckpoint();
        }
        booleanOutputStream.writeBooleans(currentGroupCount, true);
    }

    public void recordCheckpoint()
    {
        checkArgument(!closed);
        groupsCounts.add(currentGroupCount);
        currentGroupCount = 0;

        if (booleanOutputStream != null) {
            booleanOutputStream.recordCheckpoint();
        }
    }

    public void close()
    {
        closed = true;
        if (booleanOutputStream != null) {
            booleanOutputStream.close();
        }
    }

    public Optional<List<BooleanStreamCheckpoint>> getCheckpoints()
    {
        checkArgument(closed);
        if (booleanOutputStream == null) {
            return Optional.empty();
        }
        return Optional.of(booleanOutputStream.getCheckpoints());
    }

    public Optional<StreamDataOutput> getStreamDataOutput(int column)
    {
        checkArgument(closed);
        if (booleanOutputStream == null) {
            return Optional.empty();
        }
        StreamDataOutput streamDataOutput = booleanOutputStream.getStreamDataOutput(column);
        // rewrite the DATA stream created by the boolean output stream to a PRESENT stream
        Stream stream = new Stream(column, PRESENT, toIntExact(streamDataOutput.size()), streamDataOutput.getStream().isUseVInts());
        return Optional.of(new StreamDataOutput(
                sliceOutput -> {
                    streamDataOutput.writeData(sliceOutput);
                    return stream.getLength();
                },
                stream));
    }

    public long getBufferedBytes()
    {
        if (booleanOutputStream == null) {
            return 0;
        }
        return booleanOutputStream.getBufferedBytes();
    }

    public long getRetainedBytes()
    {
        // NOTE: we do not include checkpoints because they should be small and it would be annoying to calculate the size
        if (booleanOutputStream == null) {
            return INSTANCE_SIZE + buffer.getRetainedSize();
        }
        return INSTANCE_SIZE + booleanOutputStream.getRetainedBytes();
    }

    public void reset()
    {
        closed = false;
        booleanOutputStream = null;
        buffer.reset();
        groupsCounts.clear();
        currentGroupCount = 0;
    }
}
