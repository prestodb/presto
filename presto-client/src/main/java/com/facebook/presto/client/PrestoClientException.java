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
package com.facebook.presto.client;

import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.Request;

import static java.lang.String.format;

public class PrestoClientException
        extends RuntimeException
{
    private final String task;
    private final Request request;
    private final JsonResponse<QueryResults> response;

    public PrestoClientException(String task, Request request, JsonResponse<QueryResults> response)
    {
        super(getDefaultMessage(task, request, response));
        this.task = task;
        this.request = request;
        this.response = response;
    }

    private static String getDefaultMessage(String task, Request request, JsonResponse<QueryResults> response)
    {
        StringBuilder message = new StringBuilder();
        message.append(format("Error %s at %s returned HTTP response code %s.\n", task, request.getUri(), response.getStatusCode()));
        message.append(format("Response info:\n%s\n", response));
        message.append(format("Response body:\n%s\n", response.getResponseBody()));
        if (response.hasValue()) {
            message.append(format("Response status:\n%s\n", response.getStatusMessage()));
        }
        return message.toString();
    }

    public String getTask()
    {
        return task;
    }

    public Request getRequest()
    {
        return request;
    }

    public JsonResponse<QueryResults> getResponse()
    {
        return response;
    }
}
