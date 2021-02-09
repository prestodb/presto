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

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.HttpUriBuilder;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.StringResponseHandler;
import com.facebook.airlift.http.client.jetty.JettyHttpClient;
import com.facebook.presto.execution.QueryIdGenerator;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tests.ResultWithQueryId;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Optional;
import java.util.stream.Stream;

import static com.facebook.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.airlift.testing.Closeables.closeQuietly;
import static com.facebook.presto.tests.tpch.TpchQueryRunner.createQueryRunner;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestTaskInfoResource
{
    private QueryIdGenerator queryIdGenerator = new QueryIdGenerator();
    private TestingPrestoServer server;
    private DistributedQueryRunner queryRunner;
    private HttpClient client;

    @BeforeClass
    public void setup()
            throws Exception
    {
        this.client = new JettyHttpClient();
        this.queryRunner = createQueryRunner(ImmutableMap.of("query.client.timeout", "10s"));
        this.server = queryRunner.getCoordinator();
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        closeQuietly(server);
        closeQuietly(client);
        server = null;
        client = null;
    }

    @Test
    public void testGetTaskInfo()
    {
        String sql = "SELECT * FROM tpch.sf1.customer WHERE tpch.sf1.customer.nationkey = 1";
        ResultWithQueryId<MaterializedResult> result = queryRunner.executeWithQueryId(queryRunner.getDefaultSession(), sql);
        QueryId queryId = result.getQueryId();
        Optional<StageInfo> stageInfo = server.getQueryManager().getFullQueryInfo(queryId).getOutputStage();

        if (stageInfo.isPresent()) {
            Stream<TaskInfo> latestTaskInfo = stageInfo.get().getAllStages().stream()
                    .flatMap(stage -> stage.getLatestAttemptExecutionInfo().getTasks().stream());
            Iterable<TaskInfo> iterableTaskInfo = latestTaskInfo::iterator;

            for (TaskInfo taskInfo : iterableTaskInfo) {
                URI taskURI = taskUri("v1/taskInfo/", taskInfo.getTaskId().toString());
                Request taskInfoRequest = prepareGet().setUri(taskURI).build();
                TaskInfo responseTaskInfo = client.execute(taskInfoRequest, createJsonResponseHandler(jsonCodec(TaskInfo.class)));
                compareTasks(taskInfo, responseTaskInfo);
            }
        }
        else {
            fail("StageInfo not present");
        }
    }

    @Test
    public void testInvalidQueryId()
    {
        String invalidTaskId = queryIdGenerator.createNextQueryId().toString() + ".0.0.0";
        URI invalidTaskURI = taskUri("v1/taskInfo/", invalidTaskId);
        Request taskInfoRequest = prepareGet().setUri(invalidTaskURI).build();
        StringResponseHandler.StringResponse stringResponse = client.execute(taskInfoRequest, createStringResponseHandler());
        assertEquals(stringResponse.getStatusCode(), 404);
    }

    public void compareTasks(TaskInfo expectedTask, TaskInfo actualTask)
    {
        assertEquals(expectedTask.getTaskId(), actualTask.getTaskId());
        assertEquals(expectedTask.getTaskStatus().getState(), actualTask.getTaskStatus().getState());
        assertEquals(expectedTask.getTaskStatus().getSelf(), actualTask.getTaskStatus().getSelf());
        assertEquals(expectedTask.getStats().getCreateTime(), actualTask.getStats().getCreateTime());
    }

    public URI taskUri(String path, String taskId)
    {
        String taskUri = path + taskId;
        return HttpUriBuilder.uriBuilderFrom(server.getBaseUrl()).replacePath(taskUri).build();
    }
}
