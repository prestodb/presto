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
package com.facebook.presto.hive.metastore.thrift;

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.StringResponseHandler;
import org.apache.thrift.TException;

import javax.inject.Inject;

import java.net.URI;

import static com.facebook.airlift.http.client.HttpStatus.OK;
import static com.facebook.airlift.http.client.StringResponseHandler.createStringResponseHandler;

public class ThriftMetastoreUriFetcher
{
    @Inject
    protected ThriftMetastoreUriFetcher() {}

    public URI getMetastoreUri(HttpClient httpClient, Request request) throws TException
    {
        StringResponseHandler.StringResponse response = httpClient.execute(request, createStringResponseHandler());
        if (response.getStatusCode() == OK.code()) {
            return URI.create(response.getBody());
        }
        throw new TException(String.format("Error in fetching metastore URI. Error code: %s, Error: %s ", response.getStatusCode(), response.getStatusMessage()));
    }
}
