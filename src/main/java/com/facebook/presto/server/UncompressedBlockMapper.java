/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.Range;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.slice.ByteArraySlice;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.SliceInput;
import com.facebook.presto.slice.Slices;
import com.google.common.io.ByteStreams;

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

import static com.facebook.presto.SizeOf.SIZE_OF_INT;
import static com.facebook.presto.SizeOf.SIZE_OF_LONG;
import static com.facebook.presto.server.PrestoMediaTypes.PRESTO_BLOCK;
import static com.facebook.presto.server.PrestoMediaTypes.PRESTO_BLOCK_TYPE;

@Provider
@Consumes(PRESTO_BLOCK)
@Produces(PRESTO_BLOCK)
public class UncompressedBlockMapper implements MessageBodyReader<UncompressedBlock>, MessageBodyWriter<UncompressedBlock>
{
    @Override
    public boolean isReadable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType)
    {
        return UncompressedBlock.class == type && mediaType.isCompatible(PRESTO_BLOCK_TYPE);
    }

    @Override
    public UncompressedBlock readFrom(Class<UncompressedBlock> type,
            Type genericType,
            Annotation[] annotations,
            MediaType mediaType,
            MultivaluedMap<String, String> httpHeaders,
            InputStream input)
            throws IOException, WebApplicationException
    {
        // todo where does this come from?
        // todo do we want to pass this over the wire or should we assume it?
        TupleInfo tupleInfo = null;
        return readBlock(tupleInfo, input);
    }

    public static UncompressedBlock readBlock(TupleInfo tupleInfo, InputStream input)
            throws IOException
    {
        byte[] blockHeader = new byte[SIZE_OF_INT + SIZE_OF_INT + SIZE_OF_LONG];

        int bytesRead = ByteStreams.read(input, blockHeader, 0, blockHeader.length);
        if (bytesRead == 0) {
            return null;
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

        return new UncompressedBlock(range, tupleInfo, blockSlice);
    }

    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType)
    {
        return UncompressedBlock.class == type && mediaType.isCompatible(PRESTO_BLOCK_TYPE);
    }

    @Override
    public long getSize(UncompressedBlock uncompressedBlock, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType)
    {
        return SIZE_OF_INT + SIZE_OF_INT + SIZE_OF_LONG + uncompressedBlock.getSlice().length();
    }

    @Override
    public void writeTo(UncompressedBlock block,
            Class<?> type,
            Type genericType,
            Annotation[] annotations,
            MediaType mediaType,
            MultivaluedMap<String, Object> httpHeaders,
            OutputStream output)
            throws IOException, WebApplicationException
    {
        Slice slice = block.getSlice();

        // write header
        ByteArraySlice header = Slices.allocate(SIZE_OF_INT + SIZE_OF_INT + SIZE_OF_LONG);
        header.output()
                .appendInt(slice.length())
                .appendInt(block.getCount())
                .appendLong(block.getRange().getStart());
        output.write(header.getRawArray());

        // write slice
        slice.getBytes(0, output, slice.length());
    }
}
