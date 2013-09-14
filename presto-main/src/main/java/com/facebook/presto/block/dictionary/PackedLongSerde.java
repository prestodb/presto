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
package com.facebook.presto.block.dictionary;

import com.google.common.collect.AbstractIterator;
import io.airlift.slice.SizeOf;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import java.util.Iterator;

import static com.google.common.base.Preconditions.checkArgument;

public class PackedLongSerde
{
    private final byte bitWidth;
    private final long min;
    private final long max;

    public PackedLongSerde(int bitWidth)
    {
        checkArgument(bitWidth > 0 && bitWidth <= Long.SIZE);
        this.bitWidth = (byte) bitWidth;
        // Compute min/max two's complement range given bit space
        min = -1L << (bitWidth - 1);
        max = ~(-1L << (bitWidth - 1));
    }

    public void serialize(Iterable<Long> items, SliceOutput sliceOutput)
    {
        int packCapacity = Long.SIZE / bitWidth;
        long mask = -1L >>> (Long.SIZE - bitWidth);

        // Write the packed longs
        int itemCount = 0;
        Iterator<Long> iter = items.iterator();
        while (iter.hasNext()) {
            long pack = 0;
            boolean packUsed = false;
            for (int index = 0; index < packCapacity; index++) {
                if (!iter.hasNext()) {
                    break;
                }

                long rawValue = iter.next();
                checkArgument(min <= rawValue && rawValue <= max, "Provided value does not fit into bitspace");
                long maskedValue = rawValue & mask;
                itemCount++;

                pack |= maskedValue << (bitWidth * index);
                packUsed = true;
            }
            if (packUsed) {
                sliceOutput.writeLong(pack);
            }
        }

        // Write the Footer
        new Footer(itemCount, bitWidth).serialize(sliceOutput);
    }

    public static Iterable<Long> deserialize(final SliceInput sliceInput)
    {
        checkArgument(sliceInput.available() >= Footer.BYTE_SIZE, "sliceInput not large enough to read a footer");
        checkArgument((sliceInput.available() - Footer.BYTE_SIZE) % (SizeOf.SIZE_OF_LONG) == 0, "sliceInput byte alignment incorrect");

        // Extract Footer and then reset slice cursor
        int totalBytes = sliceInput.available();
        sliceInput.skipBytes(totalBytes - Footer.BYTE_SIZE);
        final Footer footer = Footer.deserialize(sliceInput.readSlice(Footer.BYTE_SIZE).getInput());
        sliceInput.setPosition(0);

        final int packCapacity = Long.SIZE / footer.getBitWidth();

        return new Iterable<Long>()
        {
            @Override
            public Iterator<Long> iterator()
            {
                return new AbstractIterator<Long>()
                {
                    private int itemIndex = 0;
                    private int packInternalIndex = 0;
                    private long packValue = 0;

                    @Override
                    protected Long computeNext()
                    {
                        if (itemIndex >= footer.getItemCount()) {
                            return endOfData();
                        }
                        if (packInternalIndex == 0) {
                            packValue = sliceInput.readLong();
                        }
                        // TODO: replace with something more efficient (but needs sign extend)
                        long value = (packValue << (Long.SIZE - ((packInternalIndex + 1) * footer.getBitWidth()))) >> (Long.SIZE - footer.getBitWidth());

                        itemIndex++;
                        packInternalIndex = (packInternalIndex + 1) % packCapacity;
                        return value;
                    }
                };
            }
        };
    }

    private static class Footer
    {
        private static final int BYTE_SIZE = SizeOf.SIZE_OF_INT + SizeOf.SIZE_OF_BYTE;

        private final int itemCount;
        private final byte bitWidth;

        private Footer(int itemCount, byte bitWidth)
        {
            checkArgument(itemCount >= 0, "itemCount must be non-negative");
            checkArgument(bitWidth > 0, "bitWidth must be greater than zero");
            this.itemCount = itemCount;
            this.bitWidth = bitWidth;
        }

        /**
         * @param sliceOutput
         * @return bytes written to sliceOutput
         */
        public int serialize(SliceOutput sliceOutput)
        {
            sliceOutput.writeInt(itemCount);
            sliceOutput.writeByte(bitWidth);
            return BYTE_SIZE;
        }

        public static Footer deserialize(SliceInput sliceInput)
        {
            int itemCount = sliceInput.readInt();
            byte bitWidth = sliceInput.readByte();
            return new Footer(itemCount, bitWidth);
        }

        public int getItemCount()
        {
            return itemCount;
        }

        public byte getBitWidth()
        {
            return bitWidth;
        }
    }
}
