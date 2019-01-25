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

import com.facebook.presto.tests.DistributedQueryRunner;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static com.facebook.presto.execution.TestQueryRunnerUtil.createQueryRunner;
import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.Resources.getResource;
import static java.nio.file.Files.readAllBytes;

public class TestSuiteGroups
{
    private static final JsonCodec<TestGroupSpec> TEST_GROUP_SPEC_JSON_CODEC = new JsonCodecFactory(
            () -> new ObjectMapperProvider().get().enable(FAIL_ON_UNKNOWN_PROPERTIES))
            .jsonCodec(TestGroupSpec.class);
    private static final JsonCodec<TestSuiteSpec> TEST_SUITE_SPEC_JSON_CODEC = new JsonCodecFactory(
            () -> new ObjectMapperProvider().get().enable(FAIL_ON_UNKNOWN_PROPERTIES))
            .jsonCodec(TestSuiteSpec.class);

    // Intended for use in the shell or a custom test with parameters via intellij while iterating
    // Example usage:
    // mvn test -Dtest=com.facebook.presto.tests.localtestdriver.TestSuiteGroups#testSingleSuite -DtestSuiteResource=test-driver/suite_spec.json -DuseTempDirectory=false
    @Test
    @Parameters({"testSuiteResource", "force", "useTempDirectory"})
    public void testSingleSuite(@Optional("test-driver/suite_spec.json") String testSuiteResource, @Optional("true") boolean force, @Optional("true") boolean useTempDirectory)
            throws Exception
    {
        Path testSuitePath = Paths.get(getResource(testSuiteResource).getPath());
        TestSuiteSpec testSuiteSpec = TEST_SUITE_SPEC_JSON_CODEC.fromJson(readAllBytes(testSuitePath));
        try (DistributedQueryRunner queryRunner = createQueryRunner()) {
            TestSuiteRunner testSuiteRunner = new TestSuiteRunner(queryRunner.getCoordinator(), testSuiteSpec, force, useTempDirectory);
            // Does not clean up temp directories
            testSuiteRunner.run();
        }
    }

    //Example usage from shell:
    // mvn test -Dtest=com.facebook.presto.tests.localtestdriver.TestSuiteGroups#testExampleSuiteGroup
    @Test(dataProvider = "exampleTestSuiteGroups")
    public void testExampleSuiteGroup(Path testGroupPath)
            throws Exception
    {
        TestGroupSpec testGroupSpec = TEST_GROUP_SPEC_JSON_CODEC.fromJson(readAllBytes(testGroupPath));
        for (TestSuiteSpec testSuiteSpec : testGroupSpec.getTestSuites()) {
            try (DistributedQueryRunner queryRunner = createQueryRunner();
                    TestSuiteRunner testSuiteRunner = new TestSuiteRunner(queryRunner.getCoordinator(), testSuiteSpec, true, true)) {
                testSuiteRunner.run();
            }
        }
    }

    @Test
    @DataProvider(name = "exampleTestSuiteGroups")
    public Object[][] testExampleSuiteGroups()
            throws Exception
    {
        Path testSuiteGroups = Paths.get(getResource("test-driver/example-test-suite-groups").getPath());
        List<Path> testSuitePaths = Files.list(testSuiteGroups).collect(toImmutableList());
        Object[][] result = new Object[testSuitePaths.size()][1];
        for (int i = 0; i < testSuitePaths.size(); i++) {
            result[i][0] = testSuitePaths.get(i);
        }
        return result;
    }
}
