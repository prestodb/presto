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
package com.facebook.presto.block;

import com.facebook.presto.spi.block.ArrayBlockEncoding;
import com.facebook.presto.spi.block.BlockEncoding;
import com.facebook.presto.spi.block.BlockEncodingFactory;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.block.DictionaryBlockEncoding;
import com.facebook.presto.spi.block.FixedWidthBlockEncoding;
import com.facebook.presto.spi.block.InterleavedBlockEncoding;
import com.facebook.presto.spi.block.LazySliceArrayBlockEncoding;
import com.facebook.presto.spi.block.RunLengthBlockEncoding;
import com.facebook.presto.spi.block.SliceArrayBlockEncoding;
import com.facebook.presto.spi.block.VariableWidthBlockEncoding;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import javax.inject.Inject;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public final class BlockEncodingManager
        implements BlockEncodingSerde
{
    private final TypeManager typeManager;
    private final ConcurrentMap<String, BlockEncodingFactory<?>> blockEncodings = new ConcurrentHashMap<>();

    public BlockEncodingManager(TypeManager typeManager, BlockEncodingFactory<?>... blockEncodingFactories)
    {
        this(typeManager, ImmutableSet.copyOf(blockEncodingFactories));
    }

    @Inject
    public BlockEncodingManager(TypeManager typeManager, Set<BlockEncodingFactory<?>> blockEncodingFactories)
    {
        // This function should be called from Guice and tests only

        this.typeManager = requireNonNull(typeManager, "typeManager is null");

        // always add the built-in BlockEncodingFactories
        addBlockEncodingFactory(VariableWidthBlockEncoding.FACTORY);
        addBlockEncodingFactory(FixedWidthBlockEncoding.FACTORY);
        addBlockEncodingFactory(SliceArrayBlockEncoding.FACTORY);
        addBlockEncodingFactory(LazySliceArrayBlockEncoding.FACTORY);
        addBlockEncodingFactory(DictionaryBlockEncoding.FACTORY);
        addBlockEncodingFactory(ArrayBlockEncoding.FACTORY);
        addBlockEncodingFactory(InterleavedBlockEncoding.FACTORY);
        addBlockEncodingFactory(RunLengthBlockEncoding.FACTORY);

        for (BlockEncodingFactory<?> factory : requireNonNull(blockEncodingFactories, "blockEncodingFactories is null")) {
            addBlockEncodingFactory(factory);
        }
    }

    public void addBlockEncodingFactory(BlockEncodingFactory<?> blockEncoding)
    {
        requireNonNull(blockEncoding, "blockEncoding is null");
        BlockEncodingFactory<?> existingEntry = blockEncodings.putIfAbsent(blockEncoding.getName(), blockEncoding);
        checkArgument(existingEntry == null, "Encoding %s is already registered", blockEncoding.getName());
    }

    @Override
    public BlockEncoding readBlockEncoding(SliceInput input)
    {
        // read the encoding name
        String encodingName = readLengthPrefixedString(input);

        // look up the encoding factory
        BlockEncodingFactory<?> blockEncoding = blockEncodings.get(encodingName);
        checkArgument(blockEncoding != null, "Unknown block encoding %s", encodingName);

        // load read the encoding factory from the output stream
        return blockEncoding.readEncoding(typeManager, this, input);
    }

    @Override
    public void writeBlockEncoding(SliceOutput output, BlockEncoding encoding)
    {
        writeBlockEncodingInternal(output, encoding);
    }

    /**
     * This method enables internal implementations to serialize data without holding a BlockEncodingManager.
     * For example, LiteralInterpreter.toExpression serializes data to produce literals.
     */
    public static void writeBlockEncodingInternal(SliceOutput output, BlockEncoding encoding)
    {
        WriteOnlyBlockEncodingManager.INSTANCE.writeBlockEncoding(output, encoding);
    }

    private static class WriteOnlyBlockEncodingManager
            implements BlockEncodingSerde
    {
        static final WriteOnlyBlockEncodingManager INSTANCE = new WriteOnlyBlockEncodingManager();

        private WriteOnlyBlockEncodingManager()
        {
        }

        @Override
        public BlockEncoding readBlockEncoding(SliceInput input)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeBlockEncoding(SliceOutput output, BlockEncoding encoding)
        {
            // get the encoding name
            String encodingName = encoding.getName();

            // look up the encoding factory
            BlockEncodingFactory<BlockEncoding> blockEncoding = encoding.getFactory();

            // write the name to the output
            writeLengthPrefixedString(output, encodingName);

            // write the encoding to the output
            blockEncoding.writeEncoding(this, output, encoding);
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
