package com.facebook.presto.operator;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.slice.DynamicSliceOutput;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.booleans.BooleanArrays;
import it.unimi.dsi.fastutil.ints.AbstractIntComparator;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongListIterator;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.hive.shaded.com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Preconditions.checkNotNull;

public class NewInMemoryOrderByOperator
        implements Operator
{
    private static final int MAX_BLOCK_SIZE = (int) BlockBuilder.DEFAULT_MAX_BLOCK_SIZE.toBytes();

    private final Operator source;
    private final int expectedPositions;
    private final int orderByChannel;
    private final int[] sortFields;
    private final boolean[] sortOrder;
    private final int valueChannel;
    private final ImmutableList<TupleInfo> tupleInfos;

    public NewInMemoryOrderByOperator(Operator source, int orderByChannel, int valueChannel, int expectedPositions)
    {
        this(source, orderByChannel, valueChannel, expectedPositions, null, null);
    }

    public NewInMemoryOrderByOperator(Operator source, int orderByChannel, int valueChannel, int expectedPositions, @Nullable int[] sortFields, @Nullable boolean[] sortOrder)
    {
        checkNotNull(source, "source is null");

        this.source = source;
        this.expectedPositions = expectedPositions;
        this.orderByChannel = orderByChannel;
        this.sortFields = sortFields;
        this.sortOrder = sortOrder;
        this.valueChannel = valueChannel;
        this.tupleInfos = ImmutableList.of(source.getTupleInfos().get(valueChannel));
    }

    @Override
    public int getChannelCount()
    {
        return 1;
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
    }

    @Override
    public Iterator<Page> iterator()
    {
        return new NewInMemoryOrderByOperatorIterator(source, orderByChannel, valueChannel, expectedPositions, sortFields, sortOrder);
    }

    private static class NewInMemoryOrderByOperatorIterator
            extends AbstractIterator<Page>
    {
        private final LongListIterator sortedOffsets;
        private final TupleInfo tupleInfo;
        private final Slice[] slices;

        private NewInMemoryOrderByOperatorIterator(Operator source,
                int orderByChannel,
                int valueChannel,
                int expectedPositions,
                @Nullable int[] sortFields,
                @Nullable boolean[] sortOrder)
        {
            this.tupleInfo = source.getTupleInfos().get(valueChannel);

            // sort info
            if (sortFields == null) {
                sortFields = new int[tupleInfo.getFieldCount()];
                for (int i = 0; i < sortFields.length; i++) {
                    sortFields[i] = i;
                }
            }
            if (sortOrder == null) {
                sortOrder = new boolean[tupleInfo.getFieldCount()];
                BooleanArrays.fill(sortOrder, true);
            }


            // index all pages
            ObjectArrayList<Slice> orderByBlocks = ObjectArrayList.wrap(new Slice[1024]);
            LongArrayList orderByOffsets = new LongArrayList(expectedPositions);

            ObjectArrayList<Slice> valueBlocks = ObjectArrayList.wrap(new Slice[1024]);
            LongArrayList valueOffsets = new LongArrayList(expectedPositions);

            int blockIndex = 0;
            for (Page page : source) {
                // index order by block
                UncompressedBlock orderByBlock = (UncompressedBlock) page.getBlock(orderByChannel);
                indexBlock(orderByBlocks, blockIndex, orderByBlock, orderByOffsets);

                // index value block
                UncompressedBlock valueBlock = (UncompressedBlock) page.getBlock(valueChannel);
                indexBlock(valueBlocks, blockIndex, valueBlock, valueOffsets);

                blockIndex++;
            }

            // create comparator over indexed data using the sort info
            MultiSliceFieldOrderedTupleComparator comparator = new MultiSliceFieldOrderedTupleComparator(
                    sortFields,
                    sortOrder,
                    source.getTupleInfos().get(orderByChannel),
                    orderByOffsets.elements(),
                    orderByBlocks.elements());

            // create swapper that sorts order by and value channels at the same time
            ParallelLongArraySwapper swapper = new ParallelLongArraySwapper(orderByOffsets.elements(), valueOffsets.elements());

            // sort pages
            Arrays.quickSort(0, orderByOffsets.size(), comparator, swapper);

            this.sortedOffsets = valueOffsets.iterator();
            this.slices = valueBlocks.elements();
        }

        private void indexBlock(ObjectArrayList<Slice> slices, int blockIndex, UncompressedBlock block, LongArrayList offsets)
        {
            slices.add(blockIndex, block.getSlice());
            BlockCursor cursor = block.cursor();
            for (int position = 0; position < block.getPositionCount(); position++) {
                checkState(cursor.advanceNextPosition());
                int offset = cursor.getRawOffset();

                long sliceAddress = (((long) blockIndex) << 32) | offset;

                Preconditions.checkState((int) (sliceAddress >> 32) == blockIndex);
                Preconditions.checkState((int) sliceAddress == offset);


                offsets.add(sliceAddress);
            }
        }

        @Override
        protected Page computeNext()
        {
            if (!sortedOffsets.hasNext()) {
                return endOfData();
            }

            DynamicSliceOutput output = new DynamicSliceOutput((int) (MAX_BLOCK_SIZE * BlockBuilder.DEFAULT_STORAGE_MULTIPLIER));
            int tupleCount = 0;
            while (output.size() < MAX_BLOCK_SIZE && sortedOffsets.hasNext()) {
                // get next slice address
                long sliceAddress = sortedOffsets.nextLong();
                // decode the address
                Slice slice = slices[((int) (sliceAddress >> 32))];
                int offset = (int) sliceAddress;
                // read the tuple length
                int length = tupleInfo.size(slice, offset);
                // copy tuple to output
                output.writeBytes(slice, offset, length);
                tupleCount++;
            }

            if (tupleCount == 0) {
                return endOfData();
            }

            UncompressedBlock block = new UncompressedBlock(tupleCount, tupleInfo, output.slice());
            Page page = new Page(block);
            return page;
        }
    }

    public static class ParallelLongArraySwapper
            implements Swapper
    {
        private final long[] first;
        private final long[] second;

        public ParallelLongArraySwapper(long[] first, long[] second)
        {
            this.first = first;
            this.second = second;
        }

        @Override
        public void swap(int a, int b)
        {
            long temp = first[a];
            first[a] = first[b];
            first[b] = temp;

            temp = second[a];
            second[a] = second[b];
            second[b] = temp;
        }
    }

    public static class MultiSliceFieldOrderedTupleComparator
            extends AbstractIntComparator
    {
        private final TupleInfo tupleInfo;
        private final long[] sliceAddresses;
        private final Slice[] slices;
        private final Type[] types;
        private final int[] sortFields;
        private final boolean[] sortOrder;

        public MultiSliceFieldOrderedTupleComparator(int[] sortFields, boolean[] sortOrder, TupleInfo tupleInfo, long[] sliceAddresses, Slice... slices)
        {
            this.sortFields = sortFields;
            this.sortOrder = sortOrder;
            this.tupleInfo = tupleInfo;
            this.sliceAddresses = sliceAddresses;
            this.slices = slices;
            List<Type> types = tupleInfo.getTypes();
            this.types = types.toArray(new Type[types.size()]);
        }

        @Override
        public int compare(int leftPosition, int rightPosition)
        {
            long leftSliceAddress = sliceAddresses[leftPosition];
            Slice leftSlice = slices[((int) (leftSliceAddress >> 32))];
            int leftOffset = (int) leftSliceAddress;

            long rightSliceAddress = sliceAddresses[rightPosition];
            Slice rightSlice = slices[((int) (rightSliceAddress >> 32))];
            int rightOffset = (int) rightSliceAddress;

            for (int i = 0; i < sortFields.length; i++) {
                int field = sortFields[i];
                Type type = types[field];

                int comparison;
                switch (type) {
                    case FIXED_INT_64:
                        comparison = Long.compare(tupleInfo.getLong(leftSlice, leftOffset, field), tupleInfo.getLong(rightSlice, rightOffset, field));
                        break;
                    case DOUBLE:
                        comparison = Double.compare(tupleInfo.getDouble(leftSlice, leftOffset, field), tupleInfo.getDouble(rightSlice, rightOffset, field));
                        break;
                    case VARIABLE_BINARY:
                        comparison = tupleInfo.getSlice(leftSlice, leftOffset, field).compareTo(tupleInfo.getSlice(rightSlice, rightOffset, field));
                        break;
                    default:
                        throw new AssertionError("unimplemented type: " + type);
                }
                if (comparison != 0) {
                    if (sortOrder[i]) {
                        return comparison;
                    }
                    else {
                        return -comparison;
                    }
                }
            }
            return 0;
        }
    }
}
