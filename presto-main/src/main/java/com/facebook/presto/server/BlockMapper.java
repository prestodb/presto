/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.nblock.Block;
import com.facebook.presto.serde.BlockSerdes;
import com.facebook.presto.slice.InputStreamSliceInput;
import com.facebook.presto.slice.OutputStreamSliceOutput;

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

import static com.facebook.presto.server.PrestoMediaTypes.PRESTO_BLOCK;
import static com.facebook.presto.server.PrestoMediaTypes.PRESTO_BLOCK_TYPE;

@Provider
@Consumes(PRESTO_BLOCK)
@Produces(PRESTO_BLOCK)
public class BlockMapper implements MessageBodyReader<Block>, MessageBodyWriter<Block>
{
    @Override
    public boolean isReadable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType)
    {
        return Block.class == type && mediaType.isCompatible(PRESTO_BLOCK_TYPE);
    }

    @Override
    public Block readFrom(Class<Block> type,
            Type genericType,
            Annotation[] annotations,
            MediaType mediaType,
            MultivaluedMap<String, String> httpHeaders,
            InputStream input)
            throws IOException, WebApplicationException
    {
        return BlockSerdes.readBlock(new InputStreamSliceInput(input));
    }

    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType)
    {
        return Block.class == type && mediaType.isCompatible(PRESTO_BLOCK_TYPE);
    }

    @Override
    public long getSize(Block uncompressedBlock, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType)
    {
        return -1;
    }

    @Override
    public void writeTo(Block block,
            Class<?> type,
            Type genericType,
            Annotation[] annotations,
            MediaType mediaType,
            MultivaluedMap<String, Object> httpHeaders,
            OutputStream output)
            throws IOException, WebApplicationException
    {
        BlockSerdes.writeBlock(block, new OutputStreamSliceOutput(output));
    }
}
