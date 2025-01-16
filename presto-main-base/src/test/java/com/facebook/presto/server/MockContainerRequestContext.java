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

import com.google.common.collect.ListMultimap;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;

import java.io.InputStream;
import java.net.URI;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class MockContainerRequestContext
        implements ContainerRequestContext
{
    private final ListMultimap<String, String> headers;
    private Response response;

    public MockContainerRequestContext(ListMultimap<String, String> headers)
    {
        this.headers = headers;
        this.response = Response.ok().build();
    }

    @Override
    public Object getProperty(String name)
    {
        return null;
    }

    @Override
    public Collection<String> getPropertyNames()
    {
        return null;
    }

    @Override
    public void setProperty(String name, Object object) {}

    @Override
    public void removeProperty(String name) {}

    @Override
    public UriInfo getUriInfo()
    {
        return null;
    }

    @Override
    public void setRequestUri(URI requestUri) {}

    @Override
    public void setRequestUri(URI baseUri, URI requestUri) {}

    @Override
    public Request getRequest()
    {
        return null;
    }

    @Override
    public String getMethod()
    {
        return null;
    }

    @Override
    public void setMethod(String method)
    {}

    @Override
    public MultivaluedMap<String, String> getHeaders()
    {
        return null;
    }

    @Override
    public String getHeaderString(String name)
    {
        if (headers.containsKey(name)) {
            return headers.get(name).get(0);
        }
        return null;
    }

    @Override
    public Date getDate()
    {
        return null;
    }

    @Override
    public Locale getLanguage()
    {
        return null;
    }

    @Override
    public int getLength()
    {
        return 0;
    }

    @Override
    public MediaType getMediaType()
    {
        return null;
    }

    @Override
    public List<MediaType> getAcceptableMediaTypes()
    {
        return null;
    }

    @Override
    public List<Locale> getAcceptableLanguages()
    {
        return null;
    }

    @Override
    public Map<String, Cookie> getCookies()
    {
        return null;
    }

    @Override
    public boolean hasEntity()
    {
        return false;
    }

    @Override
    public InputStream getEntityStream()
    {
        return null;
    }

    @Override
    public void setEntityStream(InputStream input) {}

    @Override
    public SecurityContext getSecurityContext()
    {
        return null;
    }

    @Override
    public void setSecurityContext(SecurityContext context) {}

    @Override
    public void abortWith(Response response)
    {
        this.response = response;
    }

    public Response getResponse()
    {
        return response;
    }
}
