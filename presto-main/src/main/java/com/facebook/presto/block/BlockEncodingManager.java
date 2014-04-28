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

import com.facebook.presto.spi.block.BlockEncoding;
import com.facebook.presto.spi.block.BlockEncoding.BlockEncodingFactory;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import javax.inject.Inject;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

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
        this.typeManager = checkNotNull(typeManager, "typeManager is null");
        for (BlockEncodingFactory<?> factory : checkNotNull(blockEncodingFactories, "blockEncodingFactories is null")) {
            addBlockEncodingFactory(factory);
        }
    }

    public void addBlockEncodingFactory(BlockEncodingFactory<?> blockEncoding)
    {
        checkNotNull(blockEncoding, "blockEncoding is null");
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
    public <T extends BlockEncoding> void writeBlockEncoding(SliceOutput output, T encoding)
    {
        // get the encoding name
        String encodingName = encoding.getName();

        // look up the encoding factory
        @SuppressWarnings("unchecked")
        BlockEncodingFactory<T> blockEncoding = (BlockEncodingFactory<T>) blockEncodings.get(encodingName);
        checkArgument(blockEncoding != null, "Unknown block encoding %s", encodingName);

        // write the name to the output
        writeLengthPrefixedString(output, encodingName);

        // write the encoding to the output
        blockEncoding.writeEncoding(this, output, encoding);
    }

    private static String readLengthPrefixedString(SliceInput input)
    {
        int length = input.readInt();
        byte[] bytes = new byte[length];
        input.readBytes(bytes);
        return new String(bytes, Charsets.UTF_8);
    }

    private static void writeLengthPrefixedString(SliceOutput output, String value)
    {
        byte[] bytes = value.getBytes(Charsets.UTF_8);
        output.writeInt(bytes.length);
        output.writeBytes(bytes);
    }
}
