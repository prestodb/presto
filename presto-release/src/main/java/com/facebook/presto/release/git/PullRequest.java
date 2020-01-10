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

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class PullRequest
{
    private final int id;
    private final String title;
    private final String url;
    private final String description;
    private final String authorLogin;
    private final Optional<String> mergedBy;

    @JsonCreator
    public PullRequest(
            @JsonProperty("number") int id,
            @JsonProperty("title") String title,
            @JsonProperty("url") String url,
            @JsonProperty("bodyText") String description,
            @JsonProperty("author") Actor author,
            @JsonProperty("mergedBy") User mergedBy)
    {
        this.id = id;
        this.title = requireNonNull(title, "title is null");
        this.url = requireNonNull(url, "url is null");
        this.description = requireNonNull(description, "description is null");
        this.authorLogin = requireNonNull(author.getLogin(), "authorLogin is null");
        this.mergedBy = Optional.ofNullable(mergedBy).flatMap(User::getName);
    }

    public int getId()
    {
        return id;
    }

    public String getTitle()
    {
        return title;
    }

    public String getUrl()
    {
        return url;
    }

    public String getDescription()
    {
        return description;
    }

    public String getAuthorLogin()
    {
        return authorLogin;
    }

    public Optional<String> getMergedBy()
    {
        return mergedBy;
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
        PullRequest that = (PullRequest) obj;
        return id == that.id &&
                Objects.equals(title, that.title) &&
                Objects.equals(url, that.url) &&
                Objects.equals(description, that.description) &&
                Objects.equals(authorLogin, that.authorLogin) &&
                Objects.equals(mergedBy, that.mergedBy);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, title, url, description, authorLogin, mergedBy);
    }
}
