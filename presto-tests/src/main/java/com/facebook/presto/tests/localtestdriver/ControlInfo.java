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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class ControlInfo
{
    private static final String RESULT_INFO_FILE_NAME = "control-result-info.json";
    private static final String DATA_FILE_NAME = "control-data.tsv";
    private final String name;
    private final ControlSpec controlSpec;
    private final Optional<Path> backupDirectory;
    private final Path directory;
    private final boolean compareFiles;
    private final Optional<String> catalog;
    private final Optional<String> schema;
    private final String sql;

    public ControlInfo(TestSuiteSpec suiteSpec, ControlSpec controlSpec, Path directory)
    {
        requireNonNull(suiteSpec, "suiteSpec is null");
        this.name = suiteSpec.getName();
        this.controlSpec = requireNonNull(controlSpec, "controlSpec is null");
        this.backupDirectory = validateBackupDirectory(controlSpec);
        this.directory = requireNonNull(directory, "directory is null");
        this.compareFiles = suiteSpec.getCompareFiles().orElse(false);
        this.catalog = suiteSpec.getCatalog();
        this.schema = suiteSpec.getSchema();
        this.sql = suiteSpec.getSql();
    }

    private static Optional<Path> validateBackupDirectory(ControlSpec controlSpec)
    {
        Optional<Path> backupDirectory = controlSpec.getBackupDirectory().map(Paths::get);
        if (backupDirectory.isPresent()) {
            checkState(!Files.exists(backupDirectory.get()) || backupDirectory.get().toFile().isDirectory(), "backupDirectory '%s' is not a directory", controlSpec.getBackupDirectory().get());
            checkState(backupDirectory.get().isAbsolute(), "backupDirectory '%s' is not an absolute path", controlSpec.getBackupDirectory().get());
        }
        return backupDirectory;
    }

    public String getName()
    {
        return name;
    }

    public ControlSpec getControlSpec()
    {
        return controlSpec;
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

    public Optional<Path> getBackupDirectory()
    {
        return backupDirectory;
    }

    public Optional<Path> getBackupDataPath()
    {
        return backupDirectory.map(backupPath -> backupPath.resolve(DATA_FILE_NAME));
    }

    public Optional<Path> getBackupResultInfoPath()
    {
        return backupDirectory.map(backupPath -> backupPath.resolve(RESULT_INFO_FILE_NAME));
    }

    public Path getDataPath()
    {
        return directory.resolve(DATA_FILE_NAME);
    }

    public Path getResultInfoPath()
    {
        return directory.resolve(RESULT_INFO_FILE_NAME);
    }
}
