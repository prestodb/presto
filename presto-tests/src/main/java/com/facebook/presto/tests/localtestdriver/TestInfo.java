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

import java.nio.file.Path;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TestInfo
{
    private static final String SPEC_FILE_FORMAT = "test-spec-%s.json";
    private static final String DATA_FILE_FORMAT = "test-data-%s.tsv";
    private static final String RESULT_INFO_FORMAT = "test-result-info-%s.json";

    private final String name;
    private final TestSpec testSpec;
    private final Path directory;
    private final boolean compareFiles;
    private final Optional<String> catalog;
    private final Optional<String> schema;
    private final String sql;

    public TestInfo(TestSuiteSpec suiteSpec, TestSpec testSpec, Path directory)
    {
        requireNonNull(suiteSpec, "suiteSpec is null");
        this.name = suiteSpec.getName();
        this.testSpec = requireNonNull(testSpec, "testSpec is null");
        this.directory = requireNonNull(directory, "directory is null");
        this.compareFiles = suiteSpec.getCompareFiles().orElse(false);
        this.catalog = suiteSpec.getCatalog();
        this.schema = suiteSpec.getSchema();
        this.sql = suiteSpec.getSql();
    }

    public String getName()
    {
        return name;
    }

    public TestSpec getTestSpec()
    {
        return testSpec;
    }

    public Path getDirectory()
    {
        return directory;
    }

    public boolean getCompareFiles()
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

    public Path getSpecPath()
    {
        return directory.resolve(format(SPEC_FILE_FORMAT, testSpec.getId()));
    }

    public Path getDataPath()
    {
        return directory.resolve(format(DATA_FILE_FORMAT, testSpec.getId()));
    }

    public Path getResultInfoPath()
    {
        return directory.resolve(format(RESULT_INFO_FORMAT, testSpec.getId()));
    }
}
