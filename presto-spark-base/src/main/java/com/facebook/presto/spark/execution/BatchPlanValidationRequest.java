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
package com.facebook.presto.spark.execution;

import com.facebook.presto.SessionRepresentation;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_ABSENT;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class BatchPlanValidationRequest
{
    private final SessionRepresentation session;
    private final Optional<byte[]> fragment;
    private final Optional<TableWriteInfo> tableWriteInfo;

    @JsonCreator
    public BatchPlanValidationRequest(
            @JsonProperty("session") SessionRepresentation session,
            @JsonProperty("fragment") Optional<byte[]> fragment,
            @JsonProperty("tableWriteInfo") Optional<TableWriteInfo> tableWriteInfo)
    {
        this.session = requireNonNull(session, "session is null");
        this.fragment = requireNonNull(fragment, "fragment is null");
        this.tableWriteInfo = requireNonNull(tableWriteInfo, "tableWriteInfo is null");
    }

    @JsonProperty
    public SessionRepresentation getSession()
    {
        return session;
    }

    @JsonInclude(NON_ABSENT)
    @JsonProperty
    public Optional<byte[]> getFragment()
    {
        return fragment;
    }

    @JsonProperty
    public Optional<TableWriteInfo> getTableWriteInfo()
    {
        return tableWriteInfo;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("session", session)
                .add("fragment", fragment)
                .add("tableWriteInfo", tableWriteInfo)
                .toString();
    }
}
