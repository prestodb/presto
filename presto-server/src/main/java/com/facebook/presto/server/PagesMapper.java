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
package com.facebook.presto.server;

import com.facebook.presto.operator.Page;
import com.facebook.presto.serde.PagesSerde;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeToken;
import io.airlift.slice.InputStreamSliceInput;
import io.airlift.slice.OutputStreamSliceOutput;

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

import static com.facebook.presto.PrestoMediaTypes.PRESTO_PAGES;

@Provider
@Consumes(PRESTO_PAGES)
@Produces(PRESTO_PAGES)
public class PagesMapper
        implements MessageBodyReader<List<Page>>, MessageBodyWriter<List<Page>>
{
    private static final MediaType PRESTO_PAGES_TYPE = MediaType.valueOf(PRESTO_PAGES);
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
                TypeToken.of(genericType).resolveType(LIST_GENERIC_TOKEN).getRawType().equals(Page.class) &&
                mediaType.isCompatible(PRESTO_PAGES_TYPE);
    }

    @Override
    public List<Page> readFrom(Class<List<Page>> type,
            Type genericType,
            Annotation[] annotations,
            MediaType mediaType,
            MultivaluedMap<String, String> httpHeaders,
            InputStream input)
            throws IOException, WebApplicationException
    {
        return ImmutableList.copyOf(PagesSerde.readPages(new InputStreamSliceInput(input)));
    }

    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType)
    {
        return List.class.isAssignableFrom(type) &&
                TypeToken.of(genericType).resolveType(LIST_GENERIC_TOKEN).getRawType().equals(Page.class) &&
                mediaType.isCompatible(PRESTO_PAGES_TYPE);
    }

    @Override
    public long getSize(List<Page> pages, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType)
    {
        return -1;
    }

    @Override
    public void writeTo(List<Page> pages,
            Class<?> type,
            Type genericType,
            Annotation[] annotations,
            MediaType mediaType,
            MultivaluedMap<String, Object> httpHeaders,
            OutputStream output)
            throws IOException, WebApplicationException
    {
        PagesSerde.writePages(new OutputStreamSliceOutput(output), pages);
    }
}
