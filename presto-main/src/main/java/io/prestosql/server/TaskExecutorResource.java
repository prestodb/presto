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

import io.prestosql.execution.executor.TaskExecutor;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import static java.util.Objects.requireNonNull;

@Path("/v1/maxActiveSplits")
public class TaskExecutorResource
{
    private final TaskExecutor taskExecutor;

    @Inject
    public TaskExecutorResource(
            TaskExecutor taskExecutor)
    {
        this.taskExecutor = requireNonNull(taskExecutor, "taskExecutor is null");
    }

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String getMaxActiveSplit()
    {
        return taskExecutor.getMaxActiveSplitsInfo();
    }
}
