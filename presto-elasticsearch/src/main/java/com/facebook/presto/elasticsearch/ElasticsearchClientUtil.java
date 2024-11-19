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
package com.facebook.presto.elasticsearch;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;

import java.util.Collection;
import java.util.Map;

public class ElasticsearchClientUtil
{
    private ElasticsearchClientUtil()
    {
    }
    public static Request buildRequest(String method, String endpoint, Map<String, String> params, HttpEntity entity,
                                 Collection<Header> headers)
    {
        Request request = new Request(method, endpoint);
        request.addParameters(params);
        request.setEntity(entity);
        RequestOptions.Builder requestOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
        headers.forEach(header -> requestOptionsBuilder.addHeader(header.getName(), header.getValue()));
        request.setOptions(requestOptionsBuilder);
        return request;
    }
}
