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

import com.facebook.presto.block.BlockEncoding.BlockEncodingFactory;
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
import static com.google.common.base.Preconditions.checkState;

public class BlockEncodingManager
{
    private final ConcurrentMap<String, BlockEncodingFactory<?>> blockEncodings = new ConcurrentHashMap<>();

    public BlockEncodingManager()
    {
    }

    public BlockEncodingManager(BlockEncodingFactory<?>... blockEncodingFactories)
    {
        this(ImmutableSet.copyOf(blockEncodingFactories));
    }

    @Inject
    public BlockEncodingManager(Set<BlockEncodingFactory<?>> blockEncodingFactories)
    {
        for (BlockEncodingFactory<?> blockEncodingFactory : blockEncodingFactories) {
            addBlockEncodingFactory(blockEncodingFactory);
        }
    }

    public void addBlockEncodingFactory(BlockEncodingFactory<?> blockEncoding)
    {
        checkNotNull(blockEncoding, "blockEncoding is null");
        BlockEncodingFactory<?> existingEntry = blockEncodings.putIfAbsent(blockEncoding.getName(), blockEncoding);
        checkArgument(existingEntry == null, "Encoding %s is already registered", blockEncoding.getName());
    }

    public BlockEncoding readBlockEncoding(SliceInput input)
    {
        // read the encoding name
        String encodingName = readLengthPrefixedString(input);

        // look up the encoding factory
        BlockEncodingFactory<?> blockEncoding = blockEncodings.get(encodingName);
        checkState(blockEncoding != null, "Unknown block encoding %s", encodingName);

        // load read the encoding factory from the output stream
        return blockEncoding.readEncoding(this, input);
    }

    public <T extends BlockEncoding> void writeBlockEncoding(SliceOutput output, T encoding)
    {
        // get the encoding name
        String encodingName = encoding.getName();

        // look up the encoding factory
        BlockEncodingFactory<T> blockEncoding = (BlockEncodingFactory<T>) blockEncodings.get(encodingName);
        checkState(blockEncoding != null, "Unknown block encoding %s", encodingName);

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

    private static void writeLengthPrefixedString(SliceOutput output, String string)
    {
        byte[] bytes = string.getBytes(Charsets.UTF_8);
        output.writeInt(bytes.length);
        output.writeBytes(bytes);
    }
}
