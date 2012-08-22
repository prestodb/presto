package com.facebook.presto;

import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Range;
import com.google.common.collect.Ranges;

import java.util.Iterator;

public class PackedLongSerde
{
    private final byte bitWidth;
    private final Range<Long> allowedRange;

    public PackedLongSerde(int bitWidth)
    {
        Preconditions.checkArgument(bitWidth > 0 && bitWidth <= Long.SIZE);
        this.bitWidth = (byte) bitWidth;
        this.allowedRange = Ranges.closed(-1L << (bitWidth - 1), ~(-1L << (bitWidth - 1)));
    }

    public void serialize(Iterable<Long> items, SliceOutput sliceOutput)
    {
        int packCapacity = Long.SIZE / bitWidth;
        long mask = (~0L) >>> (Long.SIZE - bitWidth);

        // Write the packed longs
        int itemCount = 0;
        Iterator<Long> iter = items.iterator();
        while (iter.hasNext()) {
            long pack = 0;
            boolean packUsed = false;
            for (int idx = 0; idx < packCapacity; idx++) {
                if (!iter.hasNext()) {
                    break;
                }

                long rawValue = iter.next();
                Preconditions.checkArgument(
                        allowedRange.contains(rawValue),
                        "Provided value does not fit into bitspace"
                );
                long maskedValue = rawValue & mask;
                itemCount++;

                pack |= maskedValue << (bitWidth * idx);
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
        Preconditions.checkArgument(
                sliceInput.available() >= Footer.BYTE_SIZE,
                "sliceInput not large enough to read a footer"
        );
        Preconditions.checkArgument(
                (sliceInput.available() - Footer.BYTE_SIZE) % (SizeOf.SIZE_OF_LONG) == 0,
                "sliceInput byte alignment incorrect"
        );
        
        // Extract Footer and then reset slice cursor
        int totalBytes = sliceInput.available();
        sliceInput.skipBytes(totalBytes - Footer.BYTE_SIZE);
        final Footer footer = Footer.deserialize(sliceInput.readSlice(Footer.BYTE_SIZE).input());
        sliceInput.setPosition(0);

        final int packCapacity = Long.SIZE / footer.getBitWidth();

        return new Iterable<Long>()
        {
            @Override
            public Iterator<Long> iterator()
            {
                return new AbstractIterator<Long>()
                {
                    int itemIdx = 0;
                    int packInternalIdx = 0;
                    long packValue = 0;

                    @Override
                    protected Long computeNext()
                    {
                        if (itemIdx >= footer.getItemCount()) {
                            return endOfData();
                        }
                        if (packInternalIdx == 0) {
                            packValue = sliceInput.readLong();
                        }
                        // TODO: replace with something more efficient (but needs sign extend)
                        long value = (packValue << (Long.SIZE - ((packInternalIdx + 1) * footer.getBitWidth()))) >> (Long.SIZE - footer.getBitWidth());

                        itemIdx++;
                        packInternalIdx = (packInternalIdx + 1) % packCapacity;
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
            Preconditions.checkArgument(itemCount >= 0, "itemCount must be non-negative");
            Preconditions.checkArgument(bitWidth > 0, "bitWidth must be greater than zero");
            this.itemCount = itemCount;
            this.bitWidth = bitWidth;
        }

        /**
         * Serialize this Header into the specified SliceOutput
         *
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
