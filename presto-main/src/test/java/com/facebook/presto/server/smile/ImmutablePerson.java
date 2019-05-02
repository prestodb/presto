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
package com.facebook.presto.server.smile;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static org.testng.Assert.assertEquals;

/**
 * This class is copied from airlift as is. Once smile support is released in airlift
 * smile support in Presto should be entirely removed.
 */
public class ImmutablePerson
{
    private final String name;
    private final boolean rocks;
    private final String notWritable = null;

    public static void validatePersonJsonCodec(JsonCodec<ImmutablePerson> jsonCodec)
    {
        ImmutablePerson expected = new ImmutablePerson("dain", true);

        String json = jsonCodec.toJson(expected);
        assertEquals(jsonCodec.fromJson(json), expected);

        byte[] bytes = jsonCodec.toJsonBytes(expected);
        assertEquals(jsonCodec.fromJson(bytes), expected);
    }

    public static void validatePersonListJsonCodec(JsonCodec<List<ImmutablePerson>> jsonCodec)
    {
        ImmutableList<ImmutablePerson> expected = ImmutableList.of(
                new ImmutablePerson("dain", true),
                new ImmutablePerson("martin", true),
                new ImmutablePerson("mark", true));

        String json = jsonCodec.toJson(expected);
        assertEquals(jsonCodec.fromJson(json), expected);

        byte[] bytes = jsonCodec.toJsonBytes(expected);
        assertEquals(jsonCodec.fromJson(bytes), expected);
    }

    public static void validatePersonMapJsonCodec(JsonCodec<Map<String, ImmutablePerson>> jsonCodec)
    {
        ImmutableMap<String, ImmutablePerson> expected = ImmutableMap.<String, ImmutablePerson>builder()
                .put("dain", new ImmutablePerson("dain", true))
                .put("martin", new ImmutablePerson("martin", true))
                .put("mark", new ImmutablePerson("mark", true))
                .build();

        String json = jsonCodec.toJson(expected);
        assertEquals(jsonCodec.fromJson(json), expected);

        byte[] bytes = jsonCodec.toJsonBytes(expected);
        assertEquals(jsonCodec.fromJson(bytes), expected);
    }

    @JsonCreator
    public ImmutablePerson(
            @JsonProperty("name") String name,
            @JsonProperty("rocks") boolean rocks)
    {
        this.name = name;
        this.rocks = rocks;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public boolean isRocks()
    {
        return rocks;
    }

    @JsonProperty
    public String getNotWritable()
    {
        return notWritable;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        ImmutablePerson o = (ImmutablePerson) obj;
        return Objects.equals(this.name, o.name) &&
                Objects.equals(this.rocks, o.rocks) &&
                Objects.equals(this.notWritable, o.notWritable);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, rocks, notWritable);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("rocks", rocks)
                .toString();
    }
}
