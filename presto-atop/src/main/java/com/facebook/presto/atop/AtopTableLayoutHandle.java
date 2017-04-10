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
package com.facebook.presto.atop;

import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@Immutable
public class AtopTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final AtopTableHandle tableHandle;
    private final Domain startTimeConstraint;
    private final Domain endTimeConstraint;

    @JsonCreator
    public AtopTableLayoutHandle(
            @JsonProperty("tableHandle") AtopTableHandle tableHandle,
            @JsonProperty("startTimeConstraint") Domain startTimeConstraint,
            @JsonProperty("endTimeConstraint") Domain endTimeConstraint)
    {
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.startTimeConstraint = requireNonNull(startTimeConstraint, "startTimeConstraint is null");
        this.endTimeConstraint = requireNonNull(endTimeConstraint, "endTimeConstraint is null");
    }

    @JsonProperty
    public AtopTableHandle getTableHandle()
    {
        return tableHandle;
    }

    @JsonProperty
    public Domain getStartTimeConstraint()
    {
        return startTimeConstraint;
    }

    @JsonProperty
    public Domain getEndTimeConstraint()
    {
        return endTimeConstraint;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableHandle, startTimeConstraint, endTimeConstraint);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        AtopTableLayoutHandle other = (AtopTableLayoutHandle) obj;
        return Objects.equals(this.tableHandle, other.tableHandle)
                && Objects.equals(this.startTimeConstraint, other.startTimeConstraint)
                && Objects.equals(this.endTimeConstraint, other.endTimeConstraint);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableHandle", tableHandle)
                .add("startTimeConstraint", startTimeConstraint)
                .add("endTimeConstraint", endTimeConstraint)
                .toString();
    }
}
