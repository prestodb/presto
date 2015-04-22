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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.json.JsonCodec.mapJsonCodec;

public class Suite
{
    private final String name;
    private final Map<String, String> sessionProperties;
    private final List<RegexTemplate> schemaNameTemplates;
    private final List<Pattern> queryNamePatterns;

    public Suite(String name, Map<String, String> sessionProperties, Iterable<RegexTemplate> schemaNameTemplates, Iterable<Pattern> queryNamePatterns)
    {
        this.name = checkNotNull(name, "name is null");
        this.sessionProperties = sessionProperties == null ? ImmutableMap.<String, String>of() : ImmutableMap.copyOf(sessionProperties);
        this.schemaNameTemplates = ImmutableList.copyOf(checkNotNull(schemaNameTemplates, "schemaNameTemplates is null"));
        this.queryNamePatterns = ImmutableList.copyOf(checkNotNull(queryNamePatterns, "queryNamePatterns is null"));
    }

    public String getName()
    {
        return name;
    }

    public Map<String, String> getSessionProperties()
    {
        return sessionProperties;
    }

    public List<RegexTemplate> getSchemaNameTemplates()
    {
        return schemaNameTemplates;
    }

    public List<Pattern> getQueryNamePatterns()
    {
        return queryNamePatterns;
    }

    public List<BenchmarkSchema> selectSchemas(Iterable<String> schemas)
    {
        if (schemaNameTemplates.isEmpty()) {
            return ImmutableList.of();
        }

        ImmutableList.Builder<BenchmarkSchema> benchmarkSchemas = ImmutableList.builder();
        for (RegexTemplate schemaNameTemplate : schemaNameTemplates) {
            for (String schema : schemas) {
                Optional<Map<String, String>> tags = schemaNameTemplate.parse(schema);
                if (tags.isPresent()) {
                    benchmarkSchemas.add(new BenchmarkSchema(schema, tags.get()));
                }
            }
        }
        return benchmarkSchemas.build();
    }

    public List<BenchmarkQuery> selectQueries(Iterable<BenchmarkQuery> queries)
    {
        if (getQueryNamePatterns().isEmpty()) {
            return ImmutableList.copyOf(queries);
        }

        List<BenchmarkQuery> filteredQueries = StreamSupport.stream(queries.spliterator(), false)
                .filter(query -> getQueryNamePatterns().stream().anyMatch(pattern -> pattern.matcher(query.getName()).matches()))
                .collect(Collectors.toList());

        return ImmutableList.copyOf(filteredQueries);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("name", name)
                .add("sessionProperties", sessionProperties)
                .add("queryNamePatterns", queryNamePatterns)
                .toString();
    }

    public static List<Suite> readSuites(File file)
            throws IOException
    {
        checkNotNull(file, "file is null");
        checkArgument(file.canRead(), "Can not read file: %s" + file);
        byte[] json = Files.readAllBytes(file.toPath());
        Map<String, OptionsJson> options = mapJsonCodec(String.class, OptionsJson.class).fromJson(json);
        ImmutableList.Builder<Suite> runOptions = ImmutableList.builder();
        for (Entry<String, OptionsJson> entry : options.entrySet()) {
            runOptions.add(entry.getValue().toSuite(entry.getKey()));
        }
        return runOptions.build();
    }

    public static class OptionsJson
    {
        private final List<String> schema;
        private final Map<String, String> session;
        private final List<String> query;

        @JsonCreator
        public OptionsJson(
                @JsonProperty("schema") List<String> schema,
                @JsonProperty("session") Map<String, String> session,
                @JsonProperty("query") List<String> query)
        {
            this.schema = checkNotNull(ImmutableList.copyOf(schema), "schema is null");
            this.session = checkNotNull(ImmutableMap.copyOf(session), "session is null");
            this.query = checkNotNull(query, "query is null");
        }

        public Suite toSuite(String name)
        {
            ImmutableList.Builder<Pattern> queryNameTemplates = ImmutableList.builder();
            for (String q : query) {
                queryNameTemplates.add(Pattern.compile(q));
            }
            ImmutableList.Builder<RegexTemplate> schemaNameTemplates = ImmutableList.builder();
            for (String s : schema) {
                schemaNameTemplates.add(new RegexTemplate(s));
            }
            return new Suite(name, session, schemaNameTemplates.build(), queryNameTemplates.build());
        }
    }
}
