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
package io.prestosql.block;

import com.google.common.collect.ImmutableSet;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.prestosql.spi.block.ArrayBlockEncoding;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockEncoding;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.block.ByteArrayBlockEncoding;
import io.prestosql.spi.block.DictionaryBlockEncoding;
import io.prestosql.spi.block.FixedWidthBlockEncoding;
import io.prestosql.spi.block.IntArrayBlockEncoding;
import io.prestosql.spi.block.LazyBlockEncoding;
import io.prestosql.spi.block.LongArrayBlockEncoding;
import io.prestosql.spi.block.MapBlockEncoding;
import io.prestosql.spi.block.RowBlockEncoding;
import io.prestosql.spi.block.RunLengthBlockEncoding;
import io.prestosql.spi.block.ShortArrayBlockEncoding;
import io.prestosql.spi.block.SingleMapBlockEncoding;
import io.prestosql.spi.block.SingleRowBlockEncoding;
import io.prestosql.spi.block.VariableWidthBlockEncoding;
import io.prestosql.spi.type.TypeManager;

import javax.inject.Inject;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public final class BlockEncodingManager
        implements BlockEncodingSerde
{
    private final ConcurrentMap<String, BlockEncoding> blockEncodings = new ConcurrentHashMap<>();

    public BlockEncodingManager(TypeManager typeManager, BlockEncoding... blockEncodings)
    {
        this(typeManager, ImmutableSet.copyOf(blockEncodings));
    }

    @Inject
    public BlockEncodingManager(TypeManager typeManager, Set<BlockEncoding> blockEncodings)
    {
        // This function should be called from Guice and tests only

        // always add the built-in BlockEncodings
        addBlockEncoding(new VariableWidthBlockEncoding());
        addBlockEncoding(new FixedWidthBlockEncoding());
        addBlockEncoding(new ByteArrayBlockEncoding());
        addBlockEncoding(new ShortArrayBlockEncoding());
        addBlockEncoding(new IntArrayBlockEncoding());
        addBlockEncoding(new LongArrayBlockEncoding());
        addBlockEncoding(new DictionaryBlockEncoding());
        addBlockEncoding(new ArrayBlockEncoding());
        addBlockEncoding(new MapBlockEncoding(typeManager));
        addBlockEncoding(new SingleMapBlockEncoding(typeManager));
        addBlockEncoding(new RowBlockEncoding());
        addBlockEncoding(new SingleRowBlockEncoding());
        addBlockEncoding(new RunLengthBlockEncoding());
        addBlockEncoding(new LazyBlockEncoding());

        for (BlockEncoding blockEncoding : requireNonNull(blockEncodings, "blockEncodings is null")) {
            addBlockEncoding(blockEncoding);
        }
    }

    public void addBlockEncoding(BlockEncoding blockEncoding)
    {
        requireNonNull(blockEncoding, "blockEncoding is null");
        BlockEncoding existingEntry = blockEncodings.putIfAbsent(blockEncoding.getName(), blockEncoding);
        checkArgument(existingEntry == null, "Encoding %s is already registered", blockEncoding.getName());
    }

    @Override
    public Block readBlock(SliceInput input)
    {
        // read the encoding name
        String encodingName = readLengthPrefixedString(input);

        // look up the encoding factory
        BlockEncoding blockEncoding = blockEncodings.get(encodingName);
        checkArgument(blockEncoding != null, "Unknown block encoding %s", encodingName);

        // load read the encoding factory from the output stream
        return blockEncoding.readBlock(this, input);
    }

    @Override
    public void writeBlock(SliceOutput output, Block block)
    {
        while (true) {
            // get the encoding name
            String encodingName = block.getEncodingName();

            // look up the BlockEncoding
            BlockEncoding blockEncoding = blockEncodings.get(encodingName);

            // see if a replacement block should be written instead
            Optional<Block> replacementBlock = blockEncoding.replacementBlockForWrite(block);
            if (replacementBlock.isPresent()) {
                block = replacementBlock.get();
                continue;
            }

            // write the name to the output
            writeLengthPrefixedString(output, encodingName);

            // write the block to the output
            blockEncoding.writeBlock(this, output, block);

            break;
        }
    }

    private static String readLengthPrefixedString(SliceInput input)
    {
        int length = input.readInt();
        byte[] bytes = new byte[length];
        input.readBytes(bytes);
        return new String(bytes, UTF_8);
    }

    private static void writeLengthPrefixedString(SliceOutput output, String value)
    {
        byte[] bytes = value.getBytes(UTF_8);
        output.writeInt(bytes.length);
        output.writeBytes(bytes);
    }
}
