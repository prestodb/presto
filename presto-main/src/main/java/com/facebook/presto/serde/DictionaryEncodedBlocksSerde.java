package com.facebook.presto.serde;

import com.facebook.presto.SizeOf;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.nblock.Block;
import com.facebook.presto.nblock.BlockCursor;
import com.facebook.presto.nblock.BlockIterable;
import com.facebook.presto.nblock.dictionary.Dictionary;
import com.facebook.presto.nblock.dictionary.Dictionary.DictionaryBuilder;
import com.facebook.presto.nblock.dictionary.DictionaryEncodedBlock;
import com.facebook.presto.nblock.uncompressed.UncompressedTupleInfoSerde;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.SliceInput;
import com.facebook.presto.slice.SliceOutput;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Iterator;

import static com.facebook.presto.Tuples.createTuple;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class DictionaryEncodedBlocksSerde
{
    private final BlockSerde idSerde;

    @JsonCreator
    public DictionaryEncodedBlocksSerde(@JsonProperty("idSerde") BlockSerde idSerde)
    {
        this.idSerde = checkNotNull(idSerde, "idSerde is null");
    }

    @JsonProperty
    public BlockSerde getIdSerde()
    {
        return idSerde;
    }

    private static int writeDictionary(SliceOutput sliceOutput, Dictionary dictionary)
    {
        int bytesWritten = UncompressedTupleInfoSerde.serialize(dictionary.getTupleInfo(), sliceOutput);

        sliceOutput.writeInt(dictionary.size());
        bytesWritten += SizeOf.SIZE_OF_INT;
        for (int index = 0; index < dictionary.size(); index++) {
            Slice slice = dictionary.getTupleSlice(index);
            sliceOutput.writeBytes(slice);
            bytesWritten += slice.length();
        }
        return bytesWritten;
    }

    public static Dictionary readDictionary(Slice slice)
    {
        SliceInput sliceInput = slice.getInput();
        TupleInfo tupleInfo = UncompressedTupleInfoSerde.deserialize(sliceInput);

        int dictionarySize = sliceInput.readInt();
        checkArgument(dictionarySize >= 0);

        Slice[] dictionary = new Slice[dictionarySize];

        for (int i = 0; i < dictionarySize; i++) {
            dictionary[i] = tupleInfo.extractTupleSlice(sliceInput);
        }

        return new Dictionary(tupleInfo, dictionary);
    }

    public BlockIterable readBlocks(Slice slice, long startPosition)
    {
        // Get dictionary byte length from tail and reset to beginning
        int dictionaryLength = slice.getInt(slice.length() - SizeOf.SIZE_OF_INT);

        // Slice out dictionary data and extract it
        Slice dictionarySlice = slice.slice(slice.length() - dictionaryLength - SizeOf.SIZE_OF_INT, dictionaryLength);
        Dictionary dictionary = readDictionary(dictionarySlice);

        Slice idSlice = slice.slice(0, slice.length() - dictionaryLength - SizeOf.SIZE_OF_INT);
        BlockIterable idBlocks = BlocksSerde.readBlocks(idSlice, startPosition);
        return readBlocks(dictionary, idBlocks);
    }

    public static BlockIterable readBlocks(final Dictionary dictionary, final BlockIterable idBlocks)
    {
        return new BlockIterable() {
            @Override
            public Iterator<Block> iterator()
            {
                return new DictionaryBlockIterator(dictionary, idBlocks.iterator());
            }
        };
    }

    public static Iterator<Block> readBlocks(SliceInput input, long startPosition)
    {
        return new BlocksReader(input, startPosition);
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
        BlocksWriter idBlocksWriter = createBlocksWriter(sliceOutput);
        DictionaryEncodedBlockWriter blocksWriter = new DictionaryEncodedBlockWriter(idBlocksWriter, sliceOutput);
        while (blocks.hasNext()) {
            Block block = blocks.next();
            blocksWriter.append(block);
        }
        blocksWriter.finish();
    }

    public BlocksWriter createBlocksWriter(SliceOutput sliceOutput)
    {
        return BlocksSerde.createBlocksWriter(sliceOutput, idSerde);
    }

    private static class BlocksReader
            extends AbstractIterator<Block>
    {
        private final SliceInput sliceInput;
        private final BlockSerde blockSerde;
        private final TupleInfo tupleInfo;
        private final long positionOffset;

        private BlocksReader(SliceInput sliceInput, long startPosition)
        {
            this.positionOffset = startPosition;
            this.sliceInput = sliceInput;

            blockSerde = BlockSerdeSerde.readBlockSerde(sliceInput);
            tupleInfo = TupleInfoSerde.readTupleInfo(sliceInput);
        }

        protected Block computeNext()
        {
            if (!sliceInput.isReadable()) {
                return endOfData();
            }

            Block block = blockSerde.readBlock(sliceInput, tupleInfo, positionOffset);
            return block;
        }
    }

    private static class DictionaryEncodedBlockWriter implements BlocksWriter
    {
        private final BlocksWriter idWriter;
        private final SliceOutput sliceOutput;
        private TupleInfo tupleInfo;
        private DictionaryBuilder dictionaryBuilder;
        private boolean finished;

        private DictionaryEncodedBlockWriter(BlocksWriter idWriter, SliceOutput sliceOutput)
        {
            this.idWriter = checkNotNull(idWriter, "idWriter is null");
            this.sliceOutput = checkNotNull(sliceOutput, "sliceOutput is null");
        }

        @Override
        public BlocksWriter append(Tuple tuple)
        {
            checkNotNull(tuple, "tuple is null");
            checkState(!finished, "already finished");

            if (tupleInfo == null) {
                tupleInfo = tuple.getTupleInfo();
                dictionaryBuilder = new DictionaryBuilder(tupleInfo);
            }

            long id = dictionaryBuilder.getId(tuple);
            idWriter.append(createTuple(id));

            return this;
        }

        @Override
        public DictionaryEncodedBlockWriter append(final Block block)
        {
            checkNotNull(block, "block is null");
            checkState(!finished, "already finished");

            if (tupleInfo == null) {
                tupleInfo = block.getTupleInfo();
                dictionaryBuilder = new DictionaryBuilder(tupleInfo);
            }

            BlockCursor cursor = block.cursor();
            while (cursor.advanceNextPosition()) {
                long id = dictionaryBuilder.getId(cursor.getTuple());
                idWriter.append(createTuple(id));
            }

            return this;
        }

        @Override
        public void finish()
        {
            checkState(tupleInfo != null, "nothing appended");
            checkState(!finished, "already finished");
            finished = true;

            idWriter.finish();

            // Serialize dictionary
            int dictionaryBytes = writeDictionary(sliceOutput, dictionaryBuilder.build());

            // Write length of dictionary
            sliceOutput.writeInt(dictionaryBytes);
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
}
