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
package com.facebook.presto.spi.eventlistener;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.annotation.Nullable;

import static java.util.Objects.requireNonNull;

public class MaterializedViewRewriteInfo
{
    public enum RewriteStatus
    {
        SUCCESS,
        NO_MV_FOUND,
        INCOMPATIBLE_SHAPE,
        MV_STATUS_CHECK_FAILED,
        EXCEPTION
    }

    private final String baseTableName;
    private final String materializedViewName;
    private final String status;
    private final String failureReason;

    @JsonCreator
    public MaterializedViewRewriteInfo(
            @JsonProperty("baseTableName") String baseTableName,
            @JsonProperty("materializedViewName") String materializedViewName,
            @JsonProperty("status") String status,
            @JsonProperty("failureReason") @Nullable String failureReason)
    {
        this.baseTableName = requireNonNull(baseTableName, "baseTableName is null");
        this.materializedViewName = requireNonNull(materializedViewName, "materializedViewName is null");
        this.status = requireNonNull(status, "status is null");
        this.failureReason = failureReason;
    }

    public MaterializedViewRewriteInfo(
            String baseTableName,
            String materializedViewName,
            RewriteStatus status,
            @Nullable String failureReason)
    {
        this(baseTableName, materializedViewName, status.name(), failureReason);
    }

    @JsonProperty
    public String getBaseTableName()
    {
        return baseTableName;
    }

    @JsonProperty
    public String getMaterializedViewName()
    {
        return materializedViewName;
    }

    @JsonProperty
    public String getStatus()
    {
        return status;
    }

    @Nullable
    @JsonProperty
    public String getFailureReason()
    {
        return failureReason;
    }
}
