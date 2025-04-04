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
package com.facebook.presto.operator;

import com.facebook.airlift.http.client.HttpStatus;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.Response;
import com.facebook.airlift.http.client.testing.TestingHttpClient;
import com.facebook.airlift.http.client.testing.TestingResponse;
import com.facebook.presto.common.Page;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.spi.page.PagesSerdeUtil;
import com.google.common.base.Splitter;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import io.airlift.slice.DynamicSliceOutput;

import static com.facebook.presto.PrestoMediaTypes.PRESTO_PAGES;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_BUFFER_COMPLETE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PAGE_NEXT_TOKEN;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PAGE_TOKEN;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_TASK_INSTANCE_ID;
import static com.facebook.presto.execution.buffer.TestingPagesSerdeFactory.testingPagesSerde;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static org.testng.Assert.assertEquals;

public class TestingExchangeHttpClientHandler
        implements TestingHttpClient.Processor
{
    private static final PagesSerde PAGES_SERDE = testingPagesSerde();

    private final LoadingCache<String, TestingTaskBuffer> taskBuffers;

    public TestingExchangeHttpClientHandler(LoadingCache<String, TestingTaskBuffer> taskBuffers)
    {
        this.taskBuffers = requireNonNull(taskBuffers, "taskBuffers is null");
    }

    @Override
    public Response handle(Request request)
    {
        ImmutableList<String> parts = ImmutableList.copyOf(Splitter.on("/").omitEmptyStrings().split(request.getUri().getPath()));
        if (request.getMethod().equals("DELETE")) {
            assertEquals(parts.size(), 1);
            return new TestingResponse(HttpStatus.OK, ImmutableListMultimap.of(), new byte[0]);
        }

        assertEquals(parts.size(), 2);
        String taskId = parts.get(0);
        int pageToken = Integer.parseInt(parts.get(1));

        ImmutableListMultimap.Builder<String, String> headers = ImmutableListMultimap.builder();
        headers.put(PRESTO_TASK_INSTANCE_ID, "task-instance-id");
        headers.put(PRESTO_PAGE_TOKEN, String.valueOf(pageToken));

        TestingTaskBuffer taskBuffer = taskBuffers.getUnchecked(taskId);
        Page page = taskBuffer.getPage(pageToken);
        headers.put(CONTENT_TYPE, PRESTO_PAGES);
        if (page != null) {
            headers.put(PRESTO_PAGE_NEXT_TOKEN, String.valueOf(pageToken + 1));
            headers.put(PRESTO_BUFFER_COMPLETE, String.valueOf(false));
            DynamicSliceOutput output = new DynamicSliceOutput(256);
            PagesSerdeUtil.writePages(PAGES_SERDE, output, page);
            return new TestingResponse(HttpStatus.OK, headers.build(), output.slice().getInput());
        }
        else if (taskBuffer.isFinished()) {
            headers.put(PRESTO_PAGE_NEXT_TOKEN, String.valueOf(pageToken));
            headers.put(PRESTO_BUFFER_COMPLETE, String.valueOf(true));
            return new TestingResponse(HttpStatus.OK, headers.build(), new byte[0]);
        }
        else {
            headers.put(PRESTO_PAGE_NEXT_TOKEN, String.valueOf(pageToken));
            headers.put(PRESTO_BUFFER_COMPLETE, String.valueOf(false));
            return new TestingResponse(HttpStatus.NO_CONTENT, headers.build(), new byte[0]);
        }
    }
}
