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
package com.facebook.presto.benchmark.driver;

import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.base.Splitter.MapSplitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class BenchmarkQuery
{
    private static final Splitter SECTION_SPLITTER = Splitter.on(Pattern.compile("\n\\s*=+\\s*\n", Pattern.MULTILINE)).limit(2).trimResults();
    private static final MapSplitter TAGS_SPLITTER = Splitter.on("\n").trimResults().withKeyValueSeparator('=');

    private final String name;
    private final Map<String, String> tags;
    private final String sql;

    public BenchmarkQuery(File file)
            throws IOException
    {
        requireNonNull(file, "file is null");

        name = Files.getNameWithoutExtension(file.getName());

        // file can have 2 sections separated by a line of equals signs
        String text = Files.toString(file, StandardCharsets.UTF_8);
        List<String> sections = SECTION_SPLITTER.splitToList(text);
        if (sections.size() == 2) {
            this.tags = ImmutableMap.copyOf(TAGS_SPLITTER.split(sections.get(0)));
            this.sql = sections.get(1);
        }
        else {
            // no tags
            this.tags = ImmutableMap.of();
            this.sql = sections.get(0);
        }
    }

    public static Function<BenchmarkQuery, String> queryNameGetter()
    {
        return query -> query.getName();
    }

    public String getName()
    {
        return name;
    }

    public Map<String, String> getTags()
    {
        return tags;
    }

    public String getSql()
    {
        return sql;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("tags", tags)
                .add("sql", sql)
                .toString();
    }
}
