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
package com.facebook.presto.hdfs;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.requireNonNull;

/**
 * @author jelly.guodong.jin@gmail.com
 */
public class HDFSDatabase
{
    private final String name;
    private final String comment;
    private final String location;
    private final String owner;

    @JsonCreator
    public HDFSDatabase(
            @JsonProperty("name") String name,
            @JsonProperty("comment") String comment,
            @JsonProperty("location") String location,
            @JsonProperty("owner") String owner)
    {
        this.name = requireNonNull(name, "name is null");
        this.comment = requireNonNull(comment, "comment is null");
        this.location = requireNonNull(location, "location is null");
        this.owner = requireNonNull(owner, "owner is null");
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public String getComment()
    {
        return comment;
    }

    @JsonProperty
    public String getLocation()
    {
        return location;
    }

    @JsonProperty
    public String getOwner()
    {
        return owner;
    }
}
