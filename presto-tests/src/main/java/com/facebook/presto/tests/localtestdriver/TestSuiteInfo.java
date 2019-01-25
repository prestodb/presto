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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class TestSuiteInfo
{
    private final List<TestInfo> testInfos;
    private final ControlInfo controlInfo;
    private final Optional<Path> root;
    private final Path directory;

    public TestSuiteInfo(TestSuiteSpec testSuiteSpec, boolean useTempDirectory)
            throws IOException
    {
        requireNonNull(testSuiteSpec, "testSuiteSpec is null");
        checkArgument(Paths.get(testSuiteSpec.getDirectory()).isAbsolute(), "directory '%' is not an absolute path", testSuiteSpec.getDirectory());
        root = getRootDirectory(testSuiteSpec, useTempDirectory);
        directory = getDirectory(testSuiteSpec, root);
        controlInfo = new ControlInfo(testSuiteSpec, generateControlSpec(testSuiteSpec), directory);
        testInfos = generateTestSpecs(testSuiteSpec).stream().map(testSpec -> new TestInfo(testSuiteSpec, testSpec, directory))
                .collect(toImmutableList());
    }

    private static Optional<Path> getRootDirectory(TestSuiteSpec suiteSpec, boolean useTempDirectory)
            throws IOException
    {
        if (useTempDirectory) {
            return Optional.of(Files.createTempDirectory("test-driver"));
        }
        else {
            return Optional.empty();
        }
    }

    private static Path getDirectory(TestSuiteSpec suiteSpec, Optional<Path> root)
    {
        if (root.isPresent()) {
            Path suiteDirectory = Paths.get(suiteSpec.getDirectory()).resolve(suiteSpec.getName());
            Path subDirectory = suiteDirectory.getRoot().relativize(suiteDirectory);
            return root.get().resolve(subDirectory);
        }
        else {
            return Paths.get(suiteSpec.getDirectory()).resolve(suiteSpec.getName());
        }
    }

    public List<TestInfo> getTestInfos()
    {
        return testInfos;
    }

    public ControlInfo getControlInfo()
    {
        return controlInfo;
    }

    private static List<TestSpec> generateTestSpecs(TestSuiteSpec testSuiteSpec)
    {
        requireNonNull(testSuiteSpec);
        ImmutableList.Builder<ImmutableMap.Builder<String, String>> propertiesBuilder = ImmutableList.builder();
        // Use of map builder enforces the rule that properties in the suite spec must all be unique
        // i.e. no property group should contain a property in the property matrix and vice versa
        if (testSuiteSpec.getSessionPropertyGroups().isEmpty()) {
            // Add empty group to start with
            propertiesBuilder.add(ImmutableMap.builder());
        }
        else {
            // Add all property groups
            for (Map<String, String> propertyGroup : testSuiteSpec.getSessionPropertyGroups()) {
                propertiesBuilder.add(ImmutableMap.<String, String>builder().putAll(propertyGroup));
            }
        }
        List<ImmutableMap.Builder<String, String>> properties = propertiesBuilder.build();
        // For each list of property values in the property matrix
        // Copy the current properties and add the new property
        for (Map.Entry<String, List<String>> entry : testSuiteSpec.getSessionPropertyMatrix().entrySet()) {
            String key = entry.getKey();
            ImmutableList.Builder<ImmutableMap.Builder<String, String>> next = ImmutableList.builder();
            for (String value : entry.getValue()) {
                for (ImmutableMap.Builder<String, String> builder : properties) {
                    next.add(ImmutableMap.<String, String>builder()
                            .putAll(builder.build())
                            .put(key, value));
                }
            }
            properties = next.build();
        }
        ImmutableList.Builder<TestSpec> testSpecBuilder = ImmutableList.builder();
        int id = 0;
        for (ImmutableMap.Builder<String, String> sessionProperties : properties) {
            testSpecBuilder.add(new TestSpec(
                    id++,
                    sessionProperties.build()));
        }
        return testSpecBuilder.build();
    }

    private static ControlSpec generateControlSpec(TestSuiteSpec testSuiteSpec)
    {
        return new ControlSpec(
                testSuiteSpec.getControlSessionProperties(), testSuiteSpec.getBackupDirectory());
    }
    public Path getDirectoryToDelete()
    {
        return root.orElse(directory);
    }
}
