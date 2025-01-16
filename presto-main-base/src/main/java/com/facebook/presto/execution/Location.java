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
package com.facebook.presto.execution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.net.URI;

import static java.util.Objects.requireNonNull;

public class Location
{
    private final String location;

    @JsonCreator
    public Location(@JsonProperty("location") String location)
    {
        this.location = requireNonNull(location, "location is null");
    }

    @JsonProperty
    public String getLocation()
    {
        return location;
    }

    // TODO: Remove this once URI is replaced
    public URI toURI()
    {
        return URI.create(location);
    }
}
