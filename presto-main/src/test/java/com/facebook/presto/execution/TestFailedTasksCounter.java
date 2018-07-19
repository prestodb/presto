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
package com.facebook.presto.execution;

import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Module;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.assertEquals;

public class TestFailedTasksCounter
{
    private TestingPrestoServer server;
    private MBeanServer mBeanServer;
    private static final AtomicInteger nextTaskId = new AtomicInteger();
    private static SqlTaskManager sqlTaskManager;

    @BeforeTest
    public void setup() throws Exception
    {
        server = createServer();
        mBeanServer = server.getMbeanServer();
        sqlTaskManager = (SqlTaskManager) server.getTaskManager();
    }

    public static TestingPrestoServer createServer() throws Exception
    {
        ImmutableMap.Builder<String, String> propertiesBuilder =
                ImmutableMap.<String, String>builder().put("http-server.http.port", "45454");

        HashMap<String, String> properties = new HashMap<>(propertiesBuilder.build());

        TestingPrestoServer server =
                new TestingPrestoServer(true, properties, null, null, new SqlParserOptions(), ImmutableList.<Module>builder().build());

        return server;
    }

    private class FailedSqlTaskRunner
            implements Runnable
    {
        @Override
        public void run()
        {
            TaskId taskId = new TaskId("query", 0, nextTaskId.incrementAndGet());
            sqlTaskManager.failTask(taskId);
        }
    }

    @Test
    public void testJMXObjectEmission()
    {
        List<String> attributeNames = new ArrayList<>();

        attributeNames.add("FailedTasks.FifteenMinute.Count");
        attributeNames.add("FailedTasks.FifteenMinute.Rate");

        attributeNames.add("FailedTasks.FiveMinute.Count");
        attributeNames.add("FailedTasks.FiveMinute.Rate");

        attributeNames.add("FailedTasks.OneMinute.Count");
        attributeNames.add("FailedTasks.OneMinute.Rate");

        attributeNames.add("FailedTasks.TotalCount");

        try {
            for (String attributeName : attributeNames) {
                mBeanServer.getAttribute(new ObjectName("com.facebook.presto.execution:name=TaskManager"),
                        attributeName);
            }
        }
        catch (MalformedObjectNameException | MBeanException | InstanceNotFoundException | ReflectionException | AttributeNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testTotalCount() throws Exception
    {
        int numTasks = 5;
        long initialCounter = getFailedTasksCounter();

        for (int i = 0; i < numTasks; i++) {
            Thread thread = new Thread(new FailedSqlTaskRunner());
            thread.run();
        }

        // Waiting for some time to ensure that the statemachines finish running all their statechangelisteners
        TimeUnit.MILLISECONDS.sleep(100);

        long finalCounter = getFailedTasksCounter();
        assertEquals(finalCounter - initialCounter, numTasks);
    }

    private long getFailedTasksCounter()
    {
        try {
            Object failedTasksTotalCountObject = mBeanServer.getAttribute(new ObjectName("com.facebook.presto.execution:name=TaskManager"), "FailedTasks.TotalCount");
            return Long.parseLong(failedTasksTotalCountObject.toString());
        }
        catch (MalformedObjectNameException | MBeanException | InstanceNotFoundException | ReflectionException | AttributeNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterClass
    public void destroy() throws Exception
    {
        server.close();
    }
}
