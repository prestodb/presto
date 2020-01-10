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
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class Commit
{
    private final String id;
    private final String author;
    private final String title;
    private final List<PullRequest> associatedPullRequests;

    @JsonCreator
    public Commit(
            @JsonProperty("oid") String id,
            @JsonProperty("author") GitActor author,
            @JsonProperty("message") String message,
            @JsonProperty("associatedPullRequests") Map<String, List<PullRequest>> associatedPullRequests)
    {
        this.id = requireNonNull(id, "id is null");
        this.author = requireNonNull(author.getName(), "author is null");
        message = message.trim();
        this.title = message.contains("\n") ? message.substring(0, message.indexOf('\n')) : message;
        this.associatedPullRequests = ImmutableList.copyOf(associatedPullRequests.get("nodes"));
    }

    public String getId()
    {
        return id;
    }

    public String getAuthor()
    {
        return author;
    }

    public String getTitle()
    {
        return title;
    }

    public List<PullRequest> getAssociatedPullRequests()
    {
        return associatedPullRequests;
    }
}
