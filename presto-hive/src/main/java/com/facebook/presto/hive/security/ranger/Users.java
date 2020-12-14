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
package com.facebook.presto.hive.security.ranger;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@Immutable
public class Users
{
    private final Long pageSize;
    private final Long queryTimeMS;
    private final Long resultSize;
    private final String sortBy;
    private final String sortType;
    private final Long startIndex;
    private final Long totalCount;
    private final List<VXUser> vXUsers;

    @JsonCreator
    public Users(
            @JsonProperty("pageSize") Long pageSize,
            @JsonProperty("queryTimeMS") Long queryTimeMS,
            @JsonProperty("resultSize") Long resultSize,
            @JsonProperty("sortBy") String sortBy,
            @JsonProperty("sortType") String sortType,
            @JsonProperty("startIndex") Long startIndex,
            @JsonProperty("totalCount") Long totalCount,
            @JsonProperty("vXUsers") List<VXUser> vXUsers)
    {
        this.pageSize = pageSize;
        this.queryTimeMS = queryTimeMS;
        this.resultSize = resultSize;
        this.sortBy = sortBy;
        this.sortType = sortType;
        this.startIndex = startIndex;
        this.totalCount = totalCount;
        this.vXUsers = ImmutableList.copyOf(requireNonNull(vXUsers, "vXUsers is null"));
    }

    @JsonProperty
    public Long getPageSize()
    {
        return pageSize;
    }

    @JsonProperty
    public Long getQueryTimeMS()
    {
        return queryTimeMS;
    }

    @JsonProperty
    public Long getResultSize()
    {
        return resultSize;
    }

    @JsonProperty
    public String getSortBy()
    {
        return sortBy;
    }

    @JsonProperty
    public String getSortType()
    {
        return sortType;
    }

    @JsonProperty
    public Long getStartIndex()
    {
        return startIndex;
    }

    @JsonProperty
    public Long getTotalCount()
    {
        return totalCount;
    }

    @JsonProperty
    public List<VXUser> getvXUsers()
    {
        return vXUsers;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("taskId", pageSize)
                .add("queryTimeMS", queryTimeMS)
                .add("resultSize", resultSize)
                .add("sortBy", sortBy)
                .add("sortType", sortType)
                .add("startIndex", startIndex)
                .add("totalCount", totalCount)
                .add("vXUsers", vXUsers)
                .toString();
    }
}
