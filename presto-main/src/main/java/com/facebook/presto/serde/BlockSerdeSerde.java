/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.serde;

import com.facebook.presto.slice.SliceInput;
import com.facebook.presto.slice.SliceOutput;
import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import io.airlift.json.ObjectMapperProvider;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public final class BlockSerdeSerde {
    private BlockSerdeSerde()
    {
    }

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();

    public static void writeBlockSerde(SliceOutput sliceOutput, BlockSerde blockSerde)
    {
        byte[] header;
        try {
            header = OBJECT_MAPPER.writeValueAsBytes(blockSerde);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }

        sliceOutput.writeInt(header.length);
        sliceOutput.writeBytes(header);
    }

    public static BlockSerde readBlockSerde(SliceInput input)
    {
        try {
            // read header
            int headerLength = input.readInt();
            byte[] header = new byte[headerLength];
            ByteStreams.readFully(input, header);
            InputStream headerInputStream = new ByteArrayInputStream(header);
            BlockSerde blockSerde = OBJECT_MAPPER.readValue(headerInputStream, BlockSerde.class);
            return blockSerde;
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
