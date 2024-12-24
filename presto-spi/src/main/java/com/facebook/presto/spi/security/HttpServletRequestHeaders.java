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
package com.facebook.presto.spi.security;

//import javax.servlet.http.HttpServletRequest;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

public class HttpServletRequestHeaders
        implements RequestHeaders
{
    private final RequestHeaders requestHeaders;
//    private final HttpServletRequest request;

    public HttpServletRequestHeaders(RequestHeaders requestHeaders)
    {
        this.requestHeaders = requestHeaders;
    }

    @Override
    public String getHeader(String name)
    {
        return requestHeaders.getHeader(name);
    }

    @Override
    public Enumeration<String> getHeaderNames()
    {
        // Delegated to the underlying RequestHeaders implementation
        return requestHeaders.getHeaderNames();
    }

    @Override
    public Map<String, String> getHeaders()
    {
        Enumeration<String> headerNames = requestHeaders.getHeaderNames();
        Map<String, String> headers = new HashMap<>();
        while (headerNames.hasMoreElements()) {
            String headerName = headerNames.nextElement();
            headers.put(headerName, requestHeaders.getHeader(headerName));
        }
        return headers;
    }
}
