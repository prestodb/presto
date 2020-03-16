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
package com.facebook.presto.server.smile;

import com.facebook.airlift.http.client.HeaderName;
import com.google.common.collect.ListMultimap;

import java.util.List;

/**
 * This interface helps simplifying the client code when treating
 * both the JSON and SMILE responses.
 */
public interface BaseResponse<T>
{
    int getStatusCode();

    String getStatusMessage();

    String getHeader(String name);

    List<String> getHeaders(String name);

    ListMultimap<HeaderName, String> getHeaders();

    boolean hasValue();

    T getValue();

    int getResponseSize();

    byte[] getResponseBytes();

    Exception getException();
}
