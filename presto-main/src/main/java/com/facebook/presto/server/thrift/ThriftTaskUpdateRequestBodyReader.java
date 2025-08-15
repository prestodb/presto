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
package com.facebook.presto.server.thrift;

import com.facebook.airlift.json.Codec;
import com.facebook.drift.codec.ThriftCodec;
import com.facebook.presto.server.TaskUpdateRequest;
import com.google.common.io.ByteStreams;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.Provider;

import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

import static com.facebook.airlift.http.client.thrift.ThriftRequestUtils.APPLICATION_THRIFT_BINARY;
import static com.facebook.presto.server.thrift.ThriftCodecWrapper.wrapThriftCodec;
import static java.util.Objects.requireNonNull;

@Provider
@Consumes(APPLICATION_THRIFT_BINARY)
public class ThriftTaskUpdateRequestBodyReader
        implements MessageBodyReader<TaskUpdateRequest>
{
    private final Codec<TaskUpdateRequest> codec;

    @Inject
    public ThriftTaskUpdateRequestBodyReader(ThriftCodec<TaskUpdateRequest> thriftCodec)
    {
        this.codec = wrapThriftCodec(requireNonNull(thriftCodec, "thriftCodec is null"));
    }

    @Override
    public boolean isReadable(Class<?> aClass, Type type, Annotation[] annotations, MediaType mediaType)
    {
        return type.equals(TaskUpdateRequest.class) && mediaType.isCompatible(MediaType.valueOf(APPLICATION_THRIFT_BINARY));
    }

    @Override
    public TaskUpdateRequest readFrom(Class<TaskUpdateRequest> aClass, Type type, Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, String> multivaluedMap, InputStream inputStream)
            throws IOException, WebApplicationException
    {
        return codec.fromBytes(ByteStreams.toByteArray(inputStream));
    }
}
