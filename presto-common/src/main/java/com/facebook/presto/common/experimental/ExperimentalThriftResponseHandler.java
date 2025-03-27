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
package com.facebook.presto.common.experimental;

import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.Response;
import com.facebook.airlift.http.client.ResponseHandler;
import com.facebook.airlift.log.Logger;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Constructor;

import static com.google.common.io.ByteStreams.toByteArray;
import static java.util.Objects.requireNonNull;

public class ExperimentalThriftResponseHandler<T extends TBase<?, ?>>
        implements ResponseHandler<T, RuntimeException>
{
    private static final Logger log = Logger.get(ExperimentalThriftResponseHandler.class);

    private final Class<T> type;

    public ExperimentalThriftResponseHandler(Class<T> type)
    {
        this.type = requireNonNull(type, "type is nul");
    }

    @Override
    public T handleException(Request request, Exception exception)
            throws RuntimeException
    {
        throw new RuntimeException("Thrift request failed" + request.getUri(), exception);
    }

    @Override
    public T handle(Request request, Response response)
            throws RuntimeException
    {
        try {
            if (response.getStatusCode() != 200) {
                throw new RuntimeException("Thrift request failed with response code " + response.getStatusCode());
            }

            byte[] responseBody = readResponseBytes(response);

            if (responseBody == null || responseBody.length == 0) {
                throw new RuntimeException("Thrift request receives empty response");
            }

            Constructor<T> constructor = type.getDeclaredConstructor();
            constructor.setAccessible(true);
            T instance = constructor.newInstance();

            TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
            deserializer.deserialize(instance, responseBody);

            return instance;
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to deserialize thrift response", e);
        }
    }

    private static byte[] readResponseBytes(Response response)
    {
        try {
            return toByteArray(response.getInputStream());
        }
        catch (IOException e) {
            throw new UncheckedIOException("Error reading response from server", e);
        }
    }
}
