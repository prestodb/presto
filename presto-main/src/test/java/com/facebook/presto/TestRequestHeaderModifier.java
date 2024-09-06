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
package com.facebook.presto;

import com.facebook.airlift.http.server.Authenticator;
import com.facebook.presto.server.MockHttpServletRequest;
import com.facebook.presto.server.security.AuthenticationFilter;
import com.facebook.presto.server.security.SecurityConfig;
import com.facebook.presto.spi.RequestModifier;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterRegistration;
import javax.servlet.RequestDispatcher;
import javax.servlet.Servlet;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.ServletRegistration;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.SessionCookieConfig;
import javax.servlet.SessionTrackingMode;
import javax.servlet.descriptor.JspConfigDescriptor;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpUpgradeHandler;
import javax.servlet.http.Part;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URL;
import java.security.Principal;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.EventListener;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.testng.Assert.assertEquals;

public class TestRequestHeaderModifier
{
    private MockWebServer mockWebServer;
    private HttpServletResponse response;
    private FilterChainStub filterChain;
    private AuthenticationFilter filter;
    private AuthenticatorStub authenticator;
    private RequestModifierManagerStub requestModifierManager;
    private RequestModifierStub requestModifier;

    @BeforeMethod
    public void setUp() throws IOException
    {
        mockWebServer = new MockWebServer();
        mockWebServer.start();

        response = new ConcreteHttpServletResponse();
        filterChain = new FilterChainStub();

        authenticator = new AuthenticatorStub();
        requestModifierManager = new RequestModifierManagerStub();
        requestModifier = new RequestModifierStub();

        List<Authenticator> authenticators = Collections.singletonList(authenticator);
        filter = new AuthenticationFilter(authenticators, new SecurityConfigStub(), requestModifierManager);
    }

    @AfterMethod
    public void tearDown() throws IOException
    {
        mockWebServer.shutdown();
    }

    @Test
    public void testDoFilter_SuccessfulAuthenticationWithHeaderModification() throws ServletException, IOException
    {
        mockWebServer.enqueue(new MockResponse().setBody("Mocked Body").setResponseCode(200));

        ConcreteHttpServletRequest request = new ConcreteHttpServletRequest(ImmutableListMultimap.of("X-Custom-Header1", "CustomValue1"), "http://request-modifier.com", Collections.singletonMap("attribute", "attribute1"));
        request.setPathInfo("/oauth2/token-value/");
        request.setSecure(true);

        PrincipalStub testPrincipal = new PrincipalStub();
        authenticator.setPrincipal(testPrincipal);

        requestModifierManager.setModifiers(Collections.singletonList(requestModifier));
        requestModifier.setHeaderNames(Collections.singletonList("Extra-credential"));
        requestModifier.setExtraHeaders(Collections.singletonMap("X-Custom-Header", "CustomValue"));

        filter.doFilter(request, response, filterChain);

        HttpServletRequest wrappedRequest = (HttpServletRequest) filterChain.getCapturedRequest();
        assertEquals("CustomValue", wrappedRequest.getHeader("X-Custom-Header"));
    }

    abstract static class HttpServletResponseAdapter
            implements HttpServletResponse
    {
        @Override
        public void addCookie(Cookie cookie)
        {
            throw new UnsupportedOperationException();
        }
        @Override
        public boolean containsHeader(String name)
        {
            throw new UnsupportedOperationException();
        }
        @Override
        public String encodeRedirectURL(String url)
        {
            throw new UnsupportedOperationException();
        }
        @Override
        public String encodeRedirectUrl(String url)
        {
            throw new UnsupportedOperationException();
        }
        @Override
        public String encodeURL(String url)
        {
            throw new UnsupportedOperationException();
        }
        @Override
        public String encodeUrl(String url)
        {
            throw new UnsupportedOperationException();
        }
        @Override
        public void sendError(int sc, String msg)
        {
            throw new UnsupportedOperationException();
        }
        @Override
        public void sendError(int sc)
        {
            throw new UnsupportedOperationException();
        }
        @Override
        public void sendRedirect(String location)
        {
            throw new UnsupportedOperationException();
        }
        @Override
        public void setDateHeader(String name, long date)
        {
            throw new UnsupportedOperationException();
        }
        @Override
        public void addDateHeader(String name, long date)
        {
            throw new UnsupportedOperationException();
        }
        @Override
        public void setHeader(String name, String value)
        {
            throw new UnsupportedOperationException();
        }
        @Override
        public void addHeader(String name, String value)
        {
            throw new UnsupportedOperationException();
        }
        @Override
        public void setIntHeader(String name, int value)
        {
            throw new UnsupportedOperationException();
        }
        @Override
        public void addIntHeader(String name, int value)
        {
            throw new UnsupportedOperationException();
        }
        @Override
        public void setContentLength(int len)
        {
            throw new UnsupportedOperationException();
        }
        @Override
        public void setContentLengthLong(long len)
        {
            throw new UnsupportedOperationException();
        }
        @Override
        public void setContentType(String type)
        {
            throw new UnsupportedOperationException();
        }
        @Override
        public void setBufferSize(int size)
        {
            throw new UnsupportedOperationException();
        }
        @Override
        public int getBufferSize()
        {
            throw new UnsupportedOperationException();
        }
        @Override
        public void flushBuffer()
        {
            throw new UnsupportedOperationException();
        }
        @Override
        public void resetBuffer()
        {
            throw new UnsupportedOperationException();
        }
        @Override
        public boolean isCommitted()
        {
            throw new UnsupportedOperationException();
        }
        @Override
        public void reset()
        {
            throw new UnsupportedOperationException();
        }
        @Override
        public void setLocale(Locale loc)
        {
            throw new UnsupportedOperationException();
        }
        @Override
        public Locale getLocale()
        {
            throw new UnsupportedOperationException();
        }
    }

    static class ConcreteHttpServletRequest
            extends MockHttpServletRequest
    {
        private final Map<String, String> headers = new HashMap<>();
        private Principal principal;
        private final Map<String, Object> attributes = new HashMap<>();
        private final ServletContext servletContext = new ServletContext()
        {
            @Override
            public String getContextPath()
            {
                return null;
            }

            @Override
            public ServletContext getContext(String s)
            {
                return null;
            }

            @Override
            public int getMajorVersion()
            {
                return 0;
            }

            @Override
            public int getMinorVersion()
            {
                return 0;
            }

            @Override
            public int getEffectiveMajorVersion()
            {
                return 0;
            }

            @Override
            public int getEffectiveMinorVersion()
            {
                return 0;
            }

            @Override
            public String getMimeType(String s)
            {
                return null;
            }

            @Override
            public Set<String> getResourcePaths(String s)
            {
                return null;
            }

            @Override
            public URL getResource(String s)
            {
                return null;
            }

            @Override
            public InputStream getResourceAsStream(String s)
            {
                return null;
            }

            @Override
            public RequestDispatcher getRequestDispatcher(String s)
            {
                return null;
            }

            @Override
            public RequestDispatcher getNamedDispatcher(String s)
            {
                return null;
            }

            @Override
            public Servlet getServlet(String s)
            {
                return null;
            }

            @Override
            public Enumeration<Servlet> getServlets()
            {
                return null;
            }

            @Override
            public Enumeration<String> getServletNames()
            {
                return null;
            }

            @Override
            public void log(String s)
            {
            }

            @Override
            public void log(Exception e, String s)
            {
            }

            @Override
            public void log(String s, Throwable throwable)
            {
            }

            @Override
            public String getRealPath(String s)
            {
                return null;
            }

            @Override
            public String getServerInfo()
            {
                return null;
            }

            @Override
            public String getInitParameter(String s)
            {
                return null;
            }

            @Override
            public Enumeration<String> getInitParameterNames()
            {
                return null;
            }

            @Override
            public boolean setInitParameter(String s, String s1)
            {
                return false;
            }

            @Override
            public Object getAttribute(String s)
            {
                return null;
            }

            @Override
            public Enumeration<String> getAttributeNames()
            {
                return null;
            }

            @Override
            public void setAttribute(String s, Object o)
            {
            }

            @Override
            public void removeAttribute(String s)
            {
            }

            @Override
            public String getServletContextName()
            {
                return null;
            }

            @Override
            public ServletRegistration.Dynamic addServlet(String s, String s1)
            {
                return null;
            }

            @Override
            public ServletRegistration.Dynamic addServlet(String s, Servlet servlet)
            {
                return null;
            }

            @Override
            public ServletRegistration.Dynamic addServlet(String s, Class<? extends Servlet> aClass)
            {
                return null;
            }

            @Override
            public <T extends Servlet> T createServlet(Class<T> aClass)
            {
                return null;
            }

            @Override
            public ServletRegistration getServletRegistration(String s)
            {
                return null;
            }

            @Override
            public Map<String, ? extends ServletRegistration> getServletRegistrations()
            {
                return null;
            }

            @Override
            public FilterRegistration.Dynamic addFilter(String s, String s1)
            {
                return null;
            }

            @Override
            public FilterRegistration.Dynamic addFilter(String s, Filter filter)
            {
                return null;
            }

            @Override
            public FilterRegistration.Dynamic addFilter(String s, Class<? extends Filter> aClass)
            {
                return null;
            }

            @Override
            public <T extends Filter> T createFilter(Class<T> aClass)
            {
                return null;
            }

            @Override
            public FilterRegistration getFilterRegistration(String s)
            {
                return null;
            }

            @Override
            public Map<String, ? extends FilterRegistration> getFilterRegistrations()
            {
                return null;
            }

            @Override
            public SessionCookieConfig getSessionCookieConfig()
            {
                return null;
            }

            @Override
            public void setSessionTrackingModes(Set<SessionTrackingMode> set)
            {
            }

            @Override
            public Set<SessionTrackingMode> getDefaultSessionTrackingModes()
            {
                return null;
            }

            @Override
            public Set<SessionTrackingMode> getEffectiveSessionTrackingModes()
            {
                return null;
            }

            @Override
            public void addListener(String s)
            {
            }

            @Override
            public <T extends EventListener> void addListener(T t)
            {
            }

            @Override
            public void addListener(Class<? extends EventListener> aClass)
            {
            }

            @Override
            public <T extends EventListener> T createListener(Class<T> aClass)
            {
                return null;
            }

            @Override
            public JspConfigDescriptor getJspConfigDescriptor()
            {
                return null;
            }

            @Override
            public ClassLoader getClassLoader()
            {
                return null;
            }

            @Override
            public void declareRoles(String... strings)
            {
            }

            @Override
            public String getVirtualServerName()
            {
                return null;
            }
        };

        private boolean secure = true;
        private String pathInfo = "/oauth2/token-value/";

        public ConcreteHttpServletRequest(ListMultimap<String, String> headers, String remoteAddress, Map<String, Object> attributes)
        {
            super(headers, remoteAddress, attributes);
        }

        @Override
        public String getHeader(String name)
        {
            return headers.get(name);
        }

        @Override
        public Enumeration<String> getHeaders(String name)
        {
            String header = headers.get(name);
            return header != null ? Collections.enumeration(Collections.singletonList(header)) : Collections.enumeration(Collections.emptyList());
        }

        @Override
        public Enumeration<String> getHeaderNames()
        {
            return Collections.enumeration(headers.keySet());
        }

        @Override
        public void setAttribute(String name, Object o)
        {
            attributes.put(name, o);
        }

        @Override
        public void removeAttribute(String name)
        {
            attributes.remove(name);
        }

        @Override
        public ServletContext getServletContext()
        {
            return servletContext;
        }

        @Override
        public boolean isSecure()
        {
            return secure;
        }

        public void setSecure(boolean secure)
        {
            this.secure = secure;
        }

        @Override
        public String getPathInfo()
        {
            return pathInfo;
        }

        public void setPathInfo(String pathInfo)
        {
            this.pathInfo = pathInfo;
        }

        public void setHeader(String name, String value)
        {
            headers.put(name, value);
        }

        @Override
        public String getRequestURI()
        {
            return "/example";
        }

        @Override
        public boolean authenticate(HttpServletResponse httpServletResponse)
        {
            return false;
        }

        @Override
        public void login(String s, String s1)
        {
        }

        @Override
        public void logout()
        {
        }

        @Override
        public Collection<Part> getParts()
        {
            return null;
        }

        @Override
        public Part getPart(String s)
        {
            return null;
        }

        @Override
        public <T extends HttpUpgradeHandler> T upgrade(Class<T> aClass)
        {
            return null;
        }
    }

    static class ConcreteHttpServletResponse
            extends HttpServletResponseAdapter
    {
        private final PrintWriter writer = new PrintWriter(System.out);
        private int status;
        private String contentType;

        @Override
        public void setStatus(int sc)
        {
            this.status = sc;
        }

        @Override
        public void setStatus(int i, String s)
        {
        }

        @Override
        public int getStatus()
        {
            return 0;
        }

        @Override
        public String getHeader(String s)
        {
            return null;
        }

        @Override
        public Collection<String> getHeaders(String s)
        {
            return null;
        }

        @Override
        public Collection<String> getHeaderNames()
        {
            return null;
        }

        @Override
        public void setContentType(String type)
        {
            this.contentType = type;
        }

        @Override
        public String getCharacterEncoding()
        {
            return null;
        }

        @Override
        public String getContentType()
        {
            return null;
        }

        @Override
        public ServletOutputStream getOutputStream()
        {
            return null;
        }

        @Override
        public PrintWriter getWriter()
        {
            return writer;
        }

        @Override
        public void setCharacterEncoding(String s)
        {
        }

        @Override
        public int getBufferSize()
        {
            return 0;
        }

        @Override
        public void setBufferSize(int size)
        {
        }

        @Override
        public boolean isCommitted()
        {
            return false;
        }

        @Override
        public void resetBuffer()
        {
        }
    }

    static class FilterChainStub
            implements FilterChain
    {
        private boolean filterCalled = true;
        private ServletRequest capturedRequest;

        @Override
        public void doFilter(ServletRequest request, ServletResponse response)
        {
            this.capturedRequest = request;
        }

        public ServletRequest getCapturedRequest()
        {
            return capturedRequest;
        }

        public boolean isFilterCalled()
        {
            return filterCalled;
        }
    }

    static class AuthenticatorStub
            implements Authenticator
    {
        private Principal principal;
        private boolean authenticateCalled;

        @Override
        public Principal authenticate(HttpServletRequest request)
        {
            authenticateCalled = true;
            return principal;
        }

        public void setPrincipal(Principal principal)
        {
            this.principal = principal;
        }

        public boolean isAuthenticateCalled()
        {
            return authenticateCalled;
        }
    }

    static class RequestModifierManagerStub
            extends RequestModifierManager
    {
        private List<RequestModifier> modifiers;

        @Override
        public List<RequestModifier> getRequestModifiers()
        {
            return modifiers;
        }

        public void setModifiers(List<RequestModifier> modifiers)
        {
            this.modifiers = modifiers;
        }
    }

    static class RequestModifierStub
            implements RequestModifier
    {
        private Map<String, String> extraHeaders;
        private List<String> headerNames;

        @Override
        public List<String> getHeaderNames()
        {
            return Collections.singletonList("Authorization");
        }

        @Override
        public <T> Optional<Map<String, String>> getExtraHeaders(T additionalInfo)
        {
            return Optional.of(Collections.singletonMap("X-Custom-Header", "CustomValue"));
        }

        public void setExtraHeaders(Map<String, String> extraHeaders)
        {
            this.extraHeaders = extraHeaders;
        }

        public void setHeaderNames(List<String> headerNames)
        {
            this.headerNames = headerNames;
        }
    }

    static class SecurityConfigStub
            extends SecurityConfig
    {
    }

    static class PrincipalStub
            implements Principal
    {
        @Override
        public String getName()
        {
            return "TestPrincipal";
        }
    }
}
