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
package com.facebook.presto.server.security;

import com.google.common.base.Throwables;
import com.google.common.net.HttpHeaders;
import io.airlift.log.Logger;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.subject.Subject;
import org.eclipse.jetty.util.B64Code;

import javax.inject.Inject;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static javax.servlet.http.HttpServletResponse.SC_UNAUTHORIZED;

public class LdapFilter implements Filter
{
    private static final Logger LOG = Logger.get(LdapFilter.class);
    private final Subject subject;

    @Inject
    public LdapFilter(Subject subject)
    {
        this.subject = subject;
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain nextFilter)
            throws IOException, ServletException
    {
        // skip auth for http
        if (!servletRequest.isSecure()) {
            LOG.debug("Skipping LDAP authentication in HTTP.");
            nextFilter.doFilter(servletRequest, servletResponse);
            return;
        }

        HttpServletRequest request = (HttpServletRequest) servletRequest;
        HttpServletResponse response = (HttpServletResponse) servletResponse;
        String credentials = request.getHeader(HttpHeaders.AUTHORIZATION);

        if (credentials != null) {
            int space = credentials.indexOf(' ');
            if (space > 0) {
                String method = credentials.substring(0, space);
                if ("basic".equalsIgnoreCase(method)) {
                    credentials = credentials.substring(space + 1);
                    credentials = B64Code.decode(credentials, StandardCharsets.ISO_8859_1);
                    int i = credentials.indexOf(':');
                    if (i > 0) {
                        try {
                            String username = credentials.substring(0, i);
                            String password = credentials.substring(i + 1);
                            UsernamePasswordToken token = new UsernamePasswordToken(username, password);
                            token.setRememberMe(true);
                            subject.login(token);

                            // ldap authentication ok, continue
                            nextFilter.doFilter(servletRequest, servletResponse);
                            return;
                        }
                        catch (AuthenticationException e) {
                            LOG.error("LDAP login failed. Check your userid/password");
                            throw Throwables.propagate(e);
                        }
                        catch (Exception e) {
                            LOG.error(e);
                        }
                    }
                }
            }
        }

        // request for user/password for LDAP authentication
        sendChallenge(response);
    }

    private static void sendChallenge(HttpServletResponse response)
    {
        response.setStatus(SC_UNAUTHORIZED);
        response.setHeader(HttpHeaders.WWW_AUTHENTICATE, "basic realm=\"" + "presto" + '"');
    }

    @Override
    public void destroy()
    {
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException
    {
    }
}
