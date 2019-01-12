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
package io.prestosql.server;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;

import javax.servlet.AsyncContext;
import javax.servlet.DispatcherType;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletContext;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.servlet.http.HttpUpgradeHandler;
import javax.servlet.http.Part;

import java.io.BufferedReader;
import java.security.Principal;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Locale;
import java.util.Map;

import static java.util.Collections.emptyEnumeration;
import static java.util.Collections.enumeration;
import static java.util.Objects.requireNonNull;

public class MockHttpServletRequest
        implements HttpServletRequest
{
    private final ListMultimap<String, String> headers;
    private final String remoteAddress;

    public MockHttpServletRequest(ListMultimap<String, String> headers, String remoteAddress)
    {
        this.headers = ImmutableListMultimap.copyOf(requireNonNull(headers, "headers is null"));
        this.remoteAddress = requireNonNull(remoteAddress, "remoteAddress is null");
    }

    @Override
    public String getAuthType()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Cookie[] getCookies()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getDateHeader(String name)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getHeader(String name)
    {
        Enumeration<String> headers = getHeaders(name);
        return headers.hasMoreElements() ? headers.nextElement() : null;
    }

    @Override
    public Enumeration<String> getHeaders(String name)
    {
        for (Map.Entry<String, Collection<String>> entry : headers.asMap().entrySet()) {
            if (entry.getKey().equalsIgnoreCase(name)) {
                return enumeration(entry.getValue());
            }
        }
        return emptyEnumeration();
    }

    @Override
    public Enumeration<String> getHeaderNames()
    {
        return enumeration(headers.keySet());
    }

    @Override
    public int getIntHeader(String name)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getMethod()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getPathInfo()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getPathTranslated()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getContextPath()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getQueryString()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getRemoteUser()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isUserInRole(String role)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Principal getUserPrincipal()
    {
        return null;
    }

    @Override
    public String getRequestedSessionId()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getRequestURI()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public StringBuffer getRequestURL()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getServletPath()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public HttpSession getSession(boolean create)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public HttpSession getSession()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String changeSessionId()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isRequestedSessionIdValid()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isRequestedSessionIdFromCookie()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isRequestedSessionIdFromURL()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isRequestedSessionIdFromUrl()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean authenticate(HttpServletResponse response)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void login(String username, String password)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void logout()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<Part> getParts()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Part getPart(String name)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T extends HttpUpgradeHandler> T upgrade(Class<T> handlerClass)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getAttribute(String name)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Enumeration<String> getAttributeNames()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getCharacterEncoding()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setCharacterEncoding(String env)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getContentLength()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getContentLengthLong()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getContentType()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ServletInputStream getInputStream()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getParameter(String name)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Enumeration<String> getParameterNames()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String[] getParameterValues(String name)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, String[]> getParameterMap()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getProtocol()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getScheme()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getServerName()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getServerPort()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public BufferedReader getReader()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getRemoteAddr()
    {
        return remoteAddress;
    }

    @Override
    public String getRemoteHost()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setAttribute(String name, Object o)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeAttribute(String name)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Locale getLocale()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Enumeration<Locale> getLocales()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isSecure()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public RequestDispatcher getRequestDispatcher(String path)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getRealPath(String path)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getRemotePort()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getLocalName()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getLocalAddr()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getLocalPort()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ServletContext getServletContext()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public AsyncContext startAsync()
            throws IllegalStateException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public AsyncContext startAsync(ServletRequest servletRequest, ServletResponse servletResponse)
            throws IllegalStateException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isAsyncStarted()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isAsyncSupported()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public AsyncContext getAsyncContext()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public DispatcherType getDispatcherType()
    {
        throw new UnsupportedOperationException();
    }
}
