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

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

import java.io.BufferedReader;
import java.io.IOException;

public class ReadOnlyHttpServletRequest
        extends HttpServletRequestWrapper
{
    /**
     * Constructs a request object wrapping the given request.
     *
     * @param request
     * @throws IllegalArgumentException if the request is null
     */
    public ReadOnlyHttpServletRequest(HttpServletRequest request)
    {
        super(request);
    }

    @Override
    public BufferedReader getReader() throws IOException
    {
        throw new UnsupportedOperationException("Reading the request body via getReader is not supported.");
    }

    @Override
    public javax.servlet.ServletInputStream getInputStream() throws IOException
    {
        throw new UnsupportedOperationException("Reading the request body via getInputStream is not supported.");
    }

    @Override
    public void setAttribute(String name, Object o)
    {
        throw new UnsupportedOperationException("Modifying request attributes is not supported.");
    }

    @Override
    public void removeAttribute(String name)
    {
        throw new UnsupportedOperationException("Modifying request attributes is not supported.");
    }
}
