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
package io.prestosql.spi.eventlistener;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class StageGcStatistics
{
    private final int stageId;
    private final int tasks;

    private final int fullGcTasks;

    private final int minFullGcSec;
    private final int maxFullGcSec;
    private final int totalFullGcSec;
    private final int averageFullGcSec;

    @JsonCreator
    public StageGcStatistics(
            @JsonProperty("stageId") int stageId,
            @JsonProperty("tasks") int tasks,
            @JsonProperty("fullGcTasks") int fullGcTasks,
            @JsonProperty("minFullGcSec") int minFullGcSec,
            @JsonProperty("maxFullGcSec") int maxFullGcSec,
            @JsonProperty("totalFullGcSec") int totalFullGcSec,
            @JsonProperty("averageFullGcSec") int averageFullGcSec)
    {
        this.stageId = stageId;
        this.tasks = tasks;
        this.fullGcTasks = fullGcTasks;
        this.minFullGcSec = minFullGcSec;
        this.maxFullGcSec = maxFullGcSec;
        this.totalFullGcSec = totalFullGcSec;
        this.averageFullGcSec = averageFullGcSec;
    }

    @JsonProperty
    public int getStageId()
    {
        return stageId;
    }

    @JsonProperty
    public int getTasks()
    {
        return tasks;
    }

    @JsonProperty
    public int getFullGcTasks()
    {
        return fullGcTasks;
    }

    @JsonProperty
    public int getMinFullGcSec()
    {
        return minFullGcSec;
    }

    @JsonProperty
    public int getMaxFullGcSec()
    {
        return maxFullGcSec;
    }

    @JsonProperty
    public int getTotalFullGcSec()
    {
        return totalFullGcSec;
    }

    @JsonProperty
    public int getAverageFullGcSec()
    {
        return averageFullGcSec;
    }
}
