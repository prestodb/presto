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

import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.ResponseHandler;

import java.io.IOException;
import java.io.InputStream;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

class StreamingJsonResponseHandler
        implements ResponseHandler<InputStream, RuntimeException>
{
    @Override
    public InputStream handleException(Request request, Exception exception)
    {
        throw new RuntimeException("Request to coordinator failed", exception);
    }

    @Override
    public InputStream handle(Request request, com.facebook.airlift.http.client.Response response)
    {
        try {
            if (APPLICATION_JSON.equals(response.getHeader(CONTENT_TYPE))) {
                return response.getInputStream();
            }
            throw new RuntimeException("Response received was not of type " + APPLICATION_JSON);
        }
        catch (IOException e) {
            throw new RuntimeException("Unable to read response from coordinator", e);
        }
    }
}
