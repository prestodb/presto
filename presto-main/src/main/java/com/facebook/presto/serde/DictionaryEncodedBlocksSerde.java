package com.facebook.presto.serde;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.block.BlockUtils;
import com.facebook.presto.block.dictionary.Dictionary;
import com.facebook.presto.block.dictionary.DictionaryEncodedBlock;
import com.facebook.presto.slice.SizeOf;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.SliceOutput;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;

import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;

public class DictionaryEncodedBlocksSerde
        implements BlocksSerde
{
    private final BlocksSerde idSerde;

    public DictionaryEncodedBlocksSerde(BlocksSerde idSerde)
    {
        this.idSerde = checkNotNull(idSerde, "idSerde is null");
    }

    public BlocksSerde getIdSerde()
    {
        return idSerde;
    }

    @Override
    public DictionaryEncodedBlockIterable createBlocksReader(Slice slice, long positionOffset)
    {
        // Get dictionary byte length from tail and reset to beginning
        int dictionaryLength = slice.getInt(slice.length() - SizeOf.SIZE_OF_INT);

        // Slice out dictionary data and extract it
        Slice dictionarySlice = slice.slice(slice.length() - dictionaryLength - SizeOf.SIZE_OF_INT, dictionaryLength);
        final Dictionary dictionary = DictionarySerde.readDictionary(dictionarySlice);

        Slice idSlice = slice.slice(0, slice.length() - dictionaryLength - SizeOf.SIZE_OF_INT);
        BlockIterable idBlocks = idSerde.createBlocksReader(idSlice, positionOffset);
        return new DictionaryEncodedBlockIterable(dictionary, idBlocks);
    }

    public static class DictionaryEncodedBlockIterable implements BlockIterable
    {
        private final Dictionary dictionary;
        private final BlockIterable idBlocks;

        public DictionaryEncodedBlockIterable(Dictionary dictionary, BlockIterable idBlocks)
        {
            this.dictionary = dictionary;
            this.idBlocks = idBlocks;
        }

        public Dictionary getDictionary()
        {
            return dictionary;
        }

        @Override
        public Iterator<Block> iterator()
        {
            return new DictionaryBlockIterator(dictionary, idBlocks.iterator());
        }
    }

    private static class DictionaryBlockIterator extends AbstractIterator<Block>
    {
        private final Iterator<Block> idBlocks;
        private final Dictionary dictionary;

        public DictionaryBlockIterator(Dictionary dictionary, Iterator<Block> idBlocks)
        {
            Preconditions.checkNotNull(idBlocks, "idBlocks is null");
            Preconditions.checkNotNull(dictionary, "dictionary is null");

            this.dictionary = dictionary;
            this.idBlocks = idBlocks;
        }

        @Override
        protected Block computeNext()
        {
            if (!idBlocks.hasNext()) {
                return endOfData();
            }
            Block idBlock = idBlocks.next();
            return new DictionaryEncodedBlock(dictionary, idBlock);
        }
    }

    public void writeBlocks(SliceOutput sliceOutput, Block... blocks)
    {
        writeBlocks(sliceOutput, ImmutableList.copyOf(blocks));
    }

    public void writeBlocks(SliceOutput sliceOutput, Iterable<Block> blocks)
    {
        writeBlocks(sliceOutput, blocks.iterator());
    }

    public void writeBlocks(SliceOutput sliceOutput, Iterator<Block> blocks)
    {
        DictionaryEncodedBlocksWriter blocksWriter = createBlocksWriter(sliceOutput);
        while (blocks.hasNext()) {
            Block block = blocks.next();
            blocksWriter.append(BlockUtils.toTupleIterable(block));
        }
        blocksWriter.finish();
    }

    public DictionaryEncodedBlocksWriter createBlocksWriter(SliceOutput sliceOutput)
    {
        return new DictionaryEncodedBlocksWriter(idSerde.createBlocksWriter(sliceOutput), sliceOutput);
    }
}
