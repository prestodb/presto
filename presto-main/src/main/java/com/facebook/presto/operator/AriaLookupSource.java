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
package com.facebook.presto.operator;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import io.airlift.slice.Slice;

import java.util.Arrays;

import static io.airlift.slice.UnsafeSlice.getLongUnchecked;

public class AriaLookupSource
        implements LookupSource
{
    int statusMask;
    long[] status;
    long[] table;
    Slice[] slices = new Slice[16];
    long[] slabs = new long[16];
    int[] fill = new int[16];
    int currentSlab = -1;
    long[] bloomFilter;
    int bloomFilterSize;

    long nextResult(long entry, int offset)
    {
        Slice slice;
        int offsetMask;
        {
            slice = slices[(int) ((entry) >> 17)];
            offsetMask = (int) (entry) & 0x1ffff;
        }
        return getLongUnchecked(slice, offsetMask + offset);
    }

    public long allocBytes(int bytes)
    {
        if (currentSlab == -1 || fill[currentSlab] + bytes > (128 * 1024)) {
            long w = newSlab();
            fill[currentSlab] = bytes;
            return w;
        }
        int off = fill[currentSlab];
        fill[currentSlab] += bytes;
        return (((currentSlab) << 17) + (off));
    }

    private long newSlab()
    {
        ++currentSlab;
        if (slices.length <= currentSlab) {
            int newSize = slices.length * 2;
            slices = Arrays.copyOf(slices, newSize);
            fill = Arrays.copyOf(fill, newSize);
        }
        Slice s = AriaHash.getSlice();
        slices[currentSlab] = s;
        return (((currentSlab) << 17));
    }

    private void release()
    {
        for (Slice slice : slices) {
            if (slice != null) {
                AriaHash.releaseSlice(slice);
            }
        }
    }

    void setSize(int count)
    {
        int size = 8;
        count *= 1.3;
        while (size < count) {
            size *= 2;
        }
        table = new long[size];
        status = new long[size / 8];
        statusMask = (size >> 3) - 1;
        for (int i = 0; i <= statusMask; ++i) {
            status[i] = 0x8080808080808080L;
        }
        for (int i = 0; i < size; ++i) {
            table[i] = -1;
        }
    }

    @Override
    public long getJoinPositionCount()
    {
        return statusMask + 1;
    }

    @Override
    public long getInMemorySizeInBytes()
    {
        return 8 * (statusMask + 1) + (128 * 1024) * (currentSlab + 1);
    }

    @Override
    public int getChannelCount()
    {
        return 1;
    }

    @Override
    public long joinPositionWithinPartition(long joinPosition)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage, long rawHash)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getNextJoinPosition(long currentJoinPosition, int probePosition, Page allProbeChannelsPage)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isJoinPositionEligible(long currentJoinPosition, int probePosition, Page allProbeChannelsPage)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmpty()
    {
        return statusMask == 0;
    }

    @Override
    public void close()
    {
        release();
        boolean closed = true;
    }
}
