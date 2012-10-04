/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.Range;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.slice.ByteArraySlice;
import com.facebook.presto.slice.OutputStreamSliceOutput;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.SliceInput;
import com.facebook.presto.slice.Slices;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import com.google.common.reflect.TypeToken;

import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.List;

import static com.facebook.presto.SizeOf.SIZE_OF_INT;
import static com.facebook.presto.SizeOf.SIZE_OF_LONG;
import static com.facebook.presto.server.PrestoMediaTypes.PRESTO_BLOCKS;
import static com.facebook.presto.server.PrestoMediaTypes.PRESTO_BLOCKS_TYPE;

@Provider
@Consumes(PRESTO_BLOCKS)
@Produces(PRESTO_BLOCKS)
public class UncompressedBlocksMapper implements MessageBodyReader<List<UncompressedBlock>>, MessageBodyWriter<List<UncompressedBlock>>
{
    private static final Type LIST_GENERIC_TOKEN;

    static {
        try {
            LIST_GENERIC_TOKEN = List.class.getMethod("get", int.class).getGenericReturnType();
        }
        catch (NoSuchMethodException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public boolean isReadable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType)
    {
        return List.class.isAssignableFrom(type) &&
                TypeToken.of(genericType).resolveType(LIST_GENERIC_TOKEN).getRawType().equals(UncompressedBlock.class) &&
                mediaType.isCompatible(PRESTO_BLOCKS_TYPE);
    }

    @Override
    public List<UncompressedBlock> readFrom(Class<List<UncompressedBlock>> type,
            Type genericType,
            Annotation[] annotations,
            MediaType mediaType,
            MultivaluedMap<String, String> httpHeaders,
            InputStream input)
            throws IOException, WebApplicationException
    {
        // where does this come from
        TupleInfo tupleInfo = null;
        return readBlocks(tupleInfo, input);
    }

    public static List<UncompressedBlock> readBlocks(TupleInfo tupleInfo, InputStream input)
            throws IOException
    {
        ImmutableList.Builder<UncompressedBlock> blocks = ImmutableList.builder();
        byte[] blockHeader = new byte[SIZE_OF_INT + SIZE_OF_INT + SIZE_OF_LONG];
        while (true) {
            int bytesRead = ByteStreams.read(input, blockHeader, 0, blockHeader.length);
            if (bytesRead == 0) {
                return blocks.build();
            }
            if (bytesRead != blockHeader.length) {
                throw new IOException("Unexpected end of data");
            }

            SliceInput headerInput = Slices.wrappedBuffer(blockHeader).input();

            int blockSize = headerInput.readInt();
            int tupleCount = headerInput.readInt();
            long startPosition = headerInput.readLong();

            ByteArraySlice blockSlice = Slices.allocate(blockSize);
            if (blockSlice.setBytes(0, input, blockSize) != blockSize) {
                throw new IOException("Unexpected end of data");
            }

            Range range = Range.create(startPosition, startPosition + tupleCount - 1);

            blocks.add(new UncompressedBlock(range, tupleInfo, blockSlice));
        }
    }

    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType)
    {
        return List.class.isAssignableFrom(type) &&
                TypeToken.of(genericType).resolveType(LIST_GENERIC_TOKEN).getRawType().equals(UncompressedBlock.class) &&
                mediaType.isCompatible(PRESTO_BLOCKS_TYPE);
    }

    @Override
    public long getSize(List<UncompressedBlock> uncompressedBlocks, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType)
    {
        long size = 0;
        for (UncompressedBlock uncompressedBlock : uncompressedBlocks) {
            size += SIZE_OF_INT + SIZE_OF_INT + SIZE_OF_LONG;
            size += uncompressedBlock.getSlice().length();
        }
        return size;
    }

    @Override
    public void writeTo(List<UncompressedBlock> blocks,
            Class<?> type,
            Type genericType,
            Annotation[] annotations,
            MediaType mediaType,
            MultivaluedMap<String, Object> httpHeaders,
            OutputStream output)
            throws IOException, WebApplicationException
    {
        OutputStreamSliceOutput sliceOutput = new OutputStreamSliceOutput(output);
        for (UncompressedBlock block : blocks) {
            Slice slice = block.getSlice();
            sliceOutput.appendInt(slice.length())
                    .appendInt(block.getCount())
                    .appendLong(block.getRange().getStart())
                    .appendBytes(slice)
                    .flush();
        }
    }
}
