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
package com.facebook.presto.tests.localtestdriver;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public class TestSuiteSpec
{
    private final String name;
    private final List<Map<String, String>> testSessionPropertyGroups;
    private final Map<String, List<String>> testSessionPropertyMatrix;
    private final Map<String, String> controlSessionProperties;
    private final Optional<String> backupDirectory;
    private final String directory;
    private final Optional<Boolean> compareFiles;
    private final Optional<String> catalog;
    private final Optional<String> schema;
    private final String sql;

    @JsonCreator
    public TestSuiteSpec(
            @JsonProperty("name") String name,
            @JsonProperty("testSessionPropertyGroups") List<Map<String, String>> testSessionPropertyGroups,
            @JsonProperty("testSessionPropertyMatrix") Map<String, List<String>> testSessionPropertyMatrix,
            @JsonProperty("controlSessionProperties") Map<String, String> controlSessionProperties,
            @JsonProperty("backupDirectory") Optional<String> backupDirectory,
            @JsonProperty("directory") String directory,
            @JsonProperty("compareFiles") Optional<Boolean> compareFiles,
            @JsonProperty("catalog") Optional<String> catalog,
            @JsonProperty("schema") Optional<String> schema,
            @JsonProperty("sql") String sql)
    {
        this.name = requireNonNull(name, "name is null");
        this.testSessionPropertyGroups = requireNonNull(testSessionPropertyGroups, "sessionPropertyGroups is null");
        this.testSessionPropertyMatrix = requireNonNull(testSessionPropertyMatrix, "testSessionPropertyMatrix is null");
        this.controlSessionProperties = requireNonNull(controlSessionProperties, "controlSessionProperties is null");
        this.backupDirectory = requireNonNull(backupDirectory, "backupDirectory is null");
        checkState(!isNullOrEmpty(directory), "parentDirectory is null or empty");
        this.directory = directory;
        this.compareFiles = requireNonNull(compareFiles, "compareFiles is null");
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.sql = requireNonNull(sql, "sql is null");
    }

    public String getName()
    {
        return name;
    }

    public List<Map<String, String>> getSessionPropertyGroups()
    {
        return testSessionPropertyGroups;
    }

    public Map<String, List<String>> getSessionPropertyMatrix()
    {
        return testSessionPropertyMatrix;
    }

    public Map<String, String> getControlSessionProperties()
    {
        return controlSessionProperties;
    }

    public Optional<String> getBackupDirectory()
    {
        return backupDirectory;
    }

    public String getDirectory()
    {
        return directory;
    }

    public Optional<Boolean> getCompareFiles()
    {
        return compareFiles;
    }

    public Optional<String> getCatalog()
    {
        return catalog;
    }

    public Optional<String> getSchema()
    {
        return schema;
    }
    public String getSql()
    {
        return sql;
    }
}
