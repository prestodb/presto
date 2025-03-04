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
package com.facebook.presto.router;

import com.facebook.presto.server.testing.TestingPrestoServer;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

import java.io.IOException;

public class TestingPrestoServerRouter
        extends TestingPrestoServer
{
    private final InstanceRequestBlocker blocker;

    public TestingPrestoServerRouter()
            throws Exception
    {
        super();
        this.blocker = new InstanceRequestBlocker(this.getBaseUrl().getPort());
    }

    public static class InstanceRequestBlocker
            implements Filter
    {
        private boolean blocked;
        private final int port;

        public InstanceRequestBlocker(int port)
        {
            this.port = port;
        }

        @Override
        public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
                throws IOException, ServletException
        {
            if (blocked && request.getServerPort() == port) {
                return;
            }
            chain.doFilter(request, response);
        }

        public void block()
        {
            blocked = true;
        }

        public void unblock()
        {
            blocked = false;
        }

        @Override
        public void init(FilterConfig filterConfig)
        {
        }

        @Override
        public void destroy()
        {
        }
    }

    @Override
    public void stopResponding()
    {
        blocker.block();
    }

    @Override
    public void startResponding()
    {
        blocker.unblock();
    }
}
