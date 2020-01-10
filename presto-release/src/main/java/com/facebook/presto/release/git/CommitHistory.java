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
package com.facebook.presto.release.git;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class CommitHistory
{
    private final PageInfo pageInfo;
    private final List<Commit> commits;

    @JsonCreator
    public CommitHistory(
            @JsonProperty("pageInfo") PageInfo pageInfo,
            @JsonProperty("edges") List<Map<String, Commit>> edges)
    {
        this.pageInfo = requireNonNull(pageInfo, "pageInfo is null");
        this.commits = edges.stream()
                .map(entry -> entry.get("node"))
                .collect(toImmutableList());
    }

    public PageInfo getPageInfo()
    {
        return pageInfo;
    }

    public List<Commit> getCommits()
    {
        return commits;
    }
}
