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

import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.buffer.OutputBuffers.OutputBufferId;
import org.testng.annotations.Test;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestAsyncPageTransportServlet
{
    class TestServlet
            extends AsyncPageTransportServlet
    {
        TaskId taskId;
        OutputBufferId bufferId;
        String requestURI;
        long token;

        void parse(String uri) throws IOException
        {
            parseURI(uri, null, null);
        }

        @Override
        protected void processRequest(
                String requestURI, TaskId taskId, OutputBufferId bufferId, long token,
                HttpServletRequest request, HttpServletResponse response)
        {
            this.requestURI = requestURI;
            this.taskId = taskId;
            this.bufferId = bufferId;
            this.token = token;
        }

        @Override
        protected void reportFailure(HttpServletResponse response, String message)
        {
            throw new IllegalArgumentException(message);
        }
    }

    private TestServlet parse(String str)
    {
        TestServlet servlet = new TestServlet();
        try {
            servlet.parse(str);
        }
        catch (IOException e) {
            fail(e.getMessage());
        }
        return servlet;
    }

    @Test
    public void testParsing()
    {
        TestServlet servlet = parse("/v1/task/async/1.2.3.4/results/456/789");
        assertEquals("1.2.3.4", servlet.taskId.toString());
        assertEquals("456", servlet.bufferId.toString());
        assertEquals(789, servlet.token);
    }

    @Test (expectedExceptions = { IllegalArgumentException.class })
    public void testParseTooFewElements()
    {
        parse("/v1/task/async/1.2.3.4/results/456");
    }

    @Test (expectedExceptions = { IllegalArgumentException.class })
    public void testParseTooManyElements()
    {
        parse("/v1/task/async/1.2.3.4/results/456/789/foo");
    }
}
