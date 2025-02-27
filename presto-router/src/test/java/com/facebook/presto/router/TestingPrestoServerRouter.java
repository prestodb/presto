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
    private final InstanceRequestBlocker blocker = new InstanceRequestBlocker();

    public TestingPrestoServerRouter()
            throws Exception
    {
        super();
    }

    private static class InstanceRequestBlocker
            implements Filter
    {
        private final Object monitor = new Object();
        private volatile boolean blocked;

        @Override
        public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
                throws IOException, ServletException
        {
            synchronized (monitor) {
                while (blocked) {
                    try {
                        monitor.wait();
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                }
            }
            chain.doFilter(request, response);
        }

        public void block()
        {
            synchronized (monitor) {
                blocked = true;
            }
        }

        public void unblock()
        {
            synchronized (monitor) {
                blocked = false;
                monitor.notifyAll();
            }
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
