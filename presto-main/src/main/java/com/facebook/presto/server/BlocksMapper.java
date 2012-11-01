/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.nblock.Block;
import com.facebook.presto.serde.BlocksSerde;
import com.facebook.presto.slice.InputStreamSliceInput;
import com.facebook.presto.slice.OutputStreamSliceOutput;
import com.facebook.presto.slice.SliceInput;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
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

import static com.facebook.presto.server.PrestoMediaTypes.PRESTO_BLOCKS;
import static com.facebook.presto.server.PrestoMediaTypes.PRESTO_BLOCKS_TYPE;

@Provider
@Consumes(PRESTO_BLOCKS)
@Produces(PRESTO_BLOCKS)
public class BlocksMapper implements MessageBodyReader<List<Block>>, MessageBodyWriter<List<Block>>
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
                TypeToken.of(genericType).resolveType(LIST_GENERIC_TOKEN).getRawType().equals(Block.class) &&
                mediaType.isCompatible(PRESTO_BLOCKS_TYPE);
    }

    @Override
    public List<Block> readFrom(Class<List<Block>> type,
            Type genericType,
            Annotation[] annotations,
            MediaType mediaType,
            MultivaluedMap<String, String> httpHeaders,
            InputStream input)
            throws IOException, WebApplicationException
    {
        SliceInput sliceInput = new InputStreamSliceInput(input);
        return ImmutableList.copyOf(BlocksSerde.readBlocks(sliceInput, 0));
    }

    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType)
    {
        return List.class.isAssignableFrom(type) &&
                TypeToken.of(genericType).resolveType(LIST_GENERIC_TOKEN).getRawType().equals(Block.class) &&
                mediaType.isCompatible(PRESTO_BLOCKS_TYPE);
    }

    @Override
    public long getSize(List<Block> uncompressedBlocks, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType)
    {
        return -1;
    }

    @Override
    public void writeTo(List<Block> blocks,
            Class<?> type,
            Type genericType,
            Annotation[] annotations,
            MediaType mediaType,
            MultivaluedMap<String, Object> httpHeaders,
            OutputStream output)
            throws IOException, WebApplicationException
    {
        OutputStreamSliceOutput sliceOutput = new OutputStreamSliceOutput(output);
        BlocksSerde.writeBlocks(sliceOutput, blocks);
    }
}
