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
package com.facebook.presto.spark;

import com.facebook.presto.SessionRepresentation;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.sql.planner.PlanFragment;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class PrestoSparkTaskDescriptor
{
    private final SessionRepresentation session;
    private final Map<String, String> extraCredentials;
    private final PlanFragment fragment;
    private final TableWriteInfo tableWriteInfo;

    @JsonCreator
    public PrestoSparkTaskDescriptor(
            @JsonProperty("session") SessionRepresentation session,
            @JsonProperty("extraCredentials") Map<String, String> extraCredentials,
            @JsonProperty("fragment") PlanFragment fragment,
            @JsonProperty("tableWriteInfo") TableWriteInfo tableWriteInfo)
    {
        this.session = requireNonNull(session, "session is null");
        this.extraCredentials = ImmutableMap.copyOf(requireNonNull(extraCredentials, "extraCredentials is null"));
        this.fragment = requireNonNull(fragment);
        this.tableWriteInfo = requireNonNull(tableWriteInfo, "tableWriteInfo is null");
    }

    @JsonProperty
    public SessionRepresentation getSession()
    {
        return session;
    }

    @JsonProperty
    public Map<String, String> getExtraCredentials()
    {
        return extraCredentials;
    }

    @JsonProperty
    public PlanFragment getFragment()
    {
        return fragment;
    }

    @JsonProperty
    public TableWriteInfo getTableWriteInfo()
    {
        return tableWriteInfo;
    }
}
