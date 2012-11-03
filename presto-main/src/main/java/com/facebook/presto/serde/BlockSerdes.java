package com.facebook.presto.serde;

import com.facebook.presto.nblock.Block;
import com.facebook.presto.nblock.uncompressed.UncompressedBlock;
import com.facebook.presto.slice.SliceInput;
import com.facebook.presto.slice.SliceOutput;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public final class BlockSerdes
{
    private BlockSerdes()
    {
    }

    public static void writeBlock(Block block, SliceOutput sliceOutput)
    {
        BlocksSerde.writeBlocks(sliceOutput, block);
    }

    public static Block readBlock(SliceInput sliceInput)
    {
        return Iterators.getOnlyElement(BlocksSerde.readBlocks(sliceInput, 0));
    }

    public static BlockSerde getSerdeForBlock(Block block)
    {
        BlockSerde blockSerde;
        if (block instanceof UncompressedBlock) {
            blockSerde = new UncompressedBlockSerde();
        } else {
            throw new IllegalArgumentException("Unsupported block type " + block.getClass().getSimpleName());
        }
        return blockSerde;
    }

    public static enum Encoding
    {
        RAW("raw")
                {
                    @Override
                    public BlockSerde createSerde()
                    {
                        return UncompressedBlockSerde.UNCOMPRESSED_BLOCK_SERDE;
                    }
                };

        private static final Map<String, Encoding> NAME_MAP;

        static {
            ImmutableMap.Builder<String, Encoding> builder = ImmutableMap.builder();
            for (Encoding encoding : Encoding.values()) {
                builder.put(encoding.getName(), encoding);
            }
            NAME_MAP = builder.build();
        }

        // Name should be usable as a filename
        private final String name;

        private Encoding(String name)
        {
            this.name = checkNotNull(name, "name is null");
        }

        public String getName()
        {
            return name;
        }

        public abstract BlockSerde createSerde();

        public static Encoding fromName(String name)
        {
            checkNotNull(name, "name is null");
            Encoding encoding = NAME_MAP.get(name);
            checkArgument(encoding != null, "Invalid type name: %s", name);
            return encoding;
        }
    }
}
