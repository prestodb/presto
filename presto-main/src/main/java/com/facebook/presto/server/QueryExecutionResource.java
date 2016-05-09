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

import com.facebook.presto.execution.BufferInfo;
import com.facebook.presto.execution.QueryId;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskStatus;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.airlift.units.DataSize;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import static com.google.common.io.Resources.getResource;
import static java.util.Objects.requireNonNull;

@Path("/")
public class QueryExecutionResource
{
    // synthetic task id used by the output buffer of the top task
    private static final TaskId OUTPUT_TASK_ID = new TaskId("output", "buffer", "id");

    private final QueryManager manager;

    @Inject
    public QueryExecutionResource(QueryManager manager)
    {
        requireNonNull(manager, "manager is null");
        this.manager = manager;
    }

    @GET
    @Path("/ui/plan")
    @Produces(MediaType.TEXT_HTML)
    public String getPlanUi()
            throws IOException
    {
        return Resources.toString(getResource(getClass(), "plan.html"), StandardCharsets.UTF_8);
    }

    @GET
    @Path("/ui/query-execution")
    @Produces(MediaType.TEXT_HTML)
    public String getUi()
            throws IOException
    {
        return Resources.toString(getResource(getClass(), "query-execution.html"), StandardCharsets.UTF_8);
    }

    @GET
    @Path("/v1/query-execution/{queryId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getTaskInfo(@PathParam("queryId") String queryId)
    {
        QueryInfo query;
        try {
            query = manager.getQueryInfo(QueryId.valueOf(queryId));
        }
        catch (NoSuchElementException e) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }

        List<StageInfo> stages = collectStages(query.getOutputStage());

        List<Task> tasks = new ArrayList<>();
        List<Flow> flows = new ArrayList<>();
        for (StageInfo stage : stages) {
            for (TaskInfo task : stage.getTasks()) {
                int bufferedPages = 0;
                TaskStatus taskStatus = task.getTaskStatus();
                for (BufferInfo bufferInfo : task.getOutputBuffers().getBuffers()) {
                    bufferedPages += bufferInfo.getBufferedPages();

                    if (!bufferInfo.getBufferId().equals(OUTPUT_TASK_ID)) {
                        flows.add(new Flow(
                                taskStatus.getTaskId().toString(),
                                bufferInfo.getBufferId().toString(),
                                bufferInfo.getPageBufferInfo().getPagesAdded(),
                                bufferInfo.getBufferedPages(),
                                bufferInfo.isFinished()));
                    }
                }

                long last = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
                if (task.getStats().getEndTime() != null) {
                    last = task.getStats().getEndTime().getMillis();
                }

                tasks.add(new Task(taskStatus.getTaskId().toString(),
                        taskStatus.getState().toString(),
                        taskStatus.getSelf().getHost(),
                        last - task.getStats().getCreateTime().getMillis(),
                        task.getStats().getTotalCpuTime().roundTo(TimeUnit.MILLISECONDS),
                        task.getStats().getTotalBlockedTime().roundTo(TimeUnit.MILLISECONDS),
                        task.getStats().getRawInputDataSize().roundTo(DataSize.Unit.BYTE),
                        task.getStats().getRawInputPositions(),
                        task.getStats().getOutputDataSize().roundTo(DataSize.Unit.BYTE),
                        task.getStats().getOutputPositions(),
                        task.getStats().getMemoryReservation().roundTo(DataSize.Unit.BYTE),
                        task.getStats().getQueuedDrivers(),
                        task.getStats().getRunningDrivers(),
                        task.getStats().getCompletedDrivers(),
                        bufferedPages));
            }
        }

        Map<String, Object> result = ImmutableMap.<String, Object>builder()
                .put("tasks", tasks)
                .put("flows", flows)
                .build();

        return Response.ok(result).build();
    }

    private static List<StageInfo> collectStages(StageInfo stage)
    {
        ImmutableList.Builder<StageInfo> result = ImmutableList.builder();
        result.add(stage);
        for (StageInfo child : stage.getSubStages()) {
            result.addAll(collectStages(child));
        }

        return result.build();
    }

    public static class Flow
    {
        private final String from;
        private final String to;
        private final long pagesSent;
        private final int bufferedPages;
        private final boolean finished;

        public Flow(String from, String to, long pagesSent, int bufferedPages, boolean finished)
        {
            this.from = from;
            this.to = to;
            this.pagesSent = pagesSent;
            this.bufferedPages = bufferedPages;
            this.finished = finished;
        }

        @JsonProperty
        public String getFrom()
        {
            return from;
        }

        @JsonProperty
        public String getTo()
        {
            return to;
        }

        @JsonProperty
        public long getPagesSent()
        {
            return pagesSent;
        }

        @JsonProperty
        public int getBufferedPages()
        {
            return bufferedPages;
        }

        @JsonProperty
        public boolean isFinished()
        {
            return finished;
        }
    }

    public static class Task
    {
        private final String taskId;
        private final String state;
        private final String host;
        private final long uptime;
        private final long cpuMillis;
        private final long blockedMillis;
        private final long inputBytes;
        private final long inputRows;
        private final long outputBytes;
        private final long outputRows;
        private final long usedMemoryBytes;
        private final int queuedSplits;
        private final int runningSplits;
        private final int completedSplits;
        private final int bufferedPages;

        public Task(
                String taskId,
                String state,
                String host,
                long uptimeMillis,
                long cpuMillis,
                long blockedMillis,
                long inputBytes,
                long inputRows,
                long outputBytes,
                long outputRows,
                long usedMemoryBytes,
                int queuedSplits,
                int runningSplits,
                int completedSplits,
                int bufferedPages)
        {
            this.taskId = taskId;
            this.state = state;
            this.host = host;
            this.uptime = uptimeMillis;
            this.cpuMillis = cpuMillis;
            this.blockedMillis = blockedMillis;
            this.inputBytes = inputBytes;
            this.inputRows = inputRows;
            this.outputBytes = outputBytes;
            this.outputRows = outputRows;
            this.usedMemoryBytes = usedMemoryBytes;
            this.queuedSplits = queuedSplits;
            this.runningSplits = runningSplits;
            this.completedSplits = completedSplits;
            this.bufferedPages = bufferedPages;
        }

        @JsonProperty
        public String getTaskId()
        {
            return taskId;
        }

        @JsonProperty
        public String getState()
        {
            return state;
        }

        @JsonProperty
        public String getHost()
        {
            return host;
        }

        @JsonProperty
        public long getUptime()
        {
            return uptime;
        }

        @JsonProperty
        public long getCpuMillis()
        {
            return cpuMillis;
        }

        @JsonProperty
        public long getBlockedMillis()
        {
            return blockedMillis;
        }

        @JsonProperty
        public long getInputBytes()
        {
            return inputBytes;
        }

        @JsonProperty
        public long getInputRows()
        {
            return inputRows;
        }

        @JsonProperty
        public long getOutputBytes()
        {
            return outputBytes;
        }

        @JsonProperty
        public long getOutputRows()
        {
            return outputRows;
        }

        @JsonProperty
        public long getUsedMemoryBytes()
        {
            return usedMemoryBytes;
        }

        @JsonProperty
        public int getQueuedSplits()
        {
            return queuedSplits;
        }

        @JsonProperty
        public int getRunningSplits()
        {
            return runningSplits;
        }

        @JsonProperty
        public int getCompletedSplits()
        {
            return completedSplits;
        }

        @JsonProperty
        public int getBufferedPages()
        {
            return bufferedPages;
        }
    }
}
