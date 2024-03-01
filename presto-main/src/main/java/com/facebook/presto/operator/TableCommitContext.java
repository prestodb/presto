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

import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.TaskId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class TableCommitContext
{
    private final Lifespan lifespan;
    private final TaskId taskId;
    private final PageSinkCommitStrategy pageSinkCommitStrategy;
    private final boolean lastPage;

    @JsonCreator
    public TableCommitContext(
            @JsonProperty("lifespan") Lifespan lifespan,
            @JsonProperty("taskId") TaskId taskId,
            @JsonProperty("pageSinkCommitStrategy") PageSinkCommitStrategy pageSinkCommitStrategy,
            @JsonProperty("lastPage") boolean lastPage)
    {
        this.lifespan = requireNonNull(lifespan, "lifespan is null");
        this.taskId = requireNonNull(taskId, "taskId is null");
        this.pageSinkCommitStrategy = requireNonNull(pageSinkCommitStrategy, "pageSinkCommitStrategy is null");
        this.lastPage = lastPage;
    }

    @JsonProperty
    public Lifespan getLifespan()
    {
        return lifespan;
    }

    @JsonProperty
    public TaskId getTaskId()
    {
        return taskId;
    }

    @JsonProperty
    public PageSinkCommitStrategy getPageSinkCommitStrategy()
    {
        return pageSinkCommitStrategy;
    }

    @JsonProperty
    public boolean isLastPage()
    {
        return lastPage;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("lifespan", lifespan)
                .add("taskId", taskId)
                .add("pageSinkCommitStrategy", pageSinkCommitStrategy)
                .add("lastPage", lastPage)
                .toString();
    }
}
