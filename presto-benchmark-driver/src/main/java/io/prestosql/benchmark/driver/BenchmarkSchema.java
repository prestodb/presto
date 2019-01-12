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
package io.prestosql.benchmark.driver;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static com.google.common.base.MoreObjects.toStringHelper;

public class BenchmarkSchema
{
    private final String name;
    private final Map<String, String> tags;

    public BenchmarkSchema(String name)
    {
        this(name, ImmutableMap.of());
    }

    public BenchmarkSchema(String name, Map<String, String> tags)
    {
        this.name = name;
        this.tags = tags;
    }

    public String getName()
    {
        return name;
    }

    public Map<String, String> getTags()
    {
        return tags;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("tags", tags)
                .toString();
    }
}
