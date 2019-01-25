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

import com.facebook.presto.Session.SessionBuilder;
import com.facebook.presto.server.testing.TestingPrestoServer;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

public class TestRunner
{
    private static final Logger log = Logger.get(TestRunner.class);
    private static final JsonCodec<ResultInfo> RESULT_INFO_JSON_CODEC = new JsonCodecFactory(
            () -> new ObjectMapperProvider().get().enable(FAIL_ON_UNKNOWN_PROPERTIES))
            .jsonCodec(ResultInfo.class);
    private static final JsonCodec<TestSpec> TEST_SPEC_JSON_CODEC = new JsonCodecFactory(
            () -> new ObjectMapperProvider().get().enable(FAIL_ON_UNKNOWN_PROPERTIES))
            .jsonCodec(TestSpec.class);

    private final TestInfo testInfo;
    private final ControlInfo controlInfo;

    public TestRunner(TestInfo testInfo, ControlInfo controlInfo)
    {
        this.testInfo = requireNonNull(testInfo, "testInfo is null");
        this.controlInfo = requireNonNull(controlInfo, "controlInfo is null");
    }

    public void run(TestingPrestoServer server)
            throws IOException
    {
        verifyControlInfo();
        SessionBuilder sessionBuilder = testSessionBuilder();
        testInfo.getCatalog().ifPresent(sessionBuilder::setCatalog);
        testInfo.getSchema().ifPresent(sessionBuilder::setSchema);
        for (Map.Entry<String, String> entry : testInfo.getTestSpec().getSessionProperties().entrySet()) {
            sessionBuilder.setSystemProperty(entry.getKey(), entry.getValue());
        }
        verify(Files.exists(testInfo.getDirectory()), "Test directory does not exist");
        // Necessary to correlate tests to session properties
        createTestSpecFile();
        LocalTestDriverClient client = new LocalTestDriverClient(server);
        log.info("Running test [%s] in '%s' with sql: %s", testInfo.getTestSpec().getSessionProperties(), testInfo.getDirectory(), testInfo.getSql());

        ResultInfo resultInfo = client.execute(sessionBuilder.build(), testInfo.getSql(), getDataPath()).getResult();
        createResultInfoFile(resultInfo);
        assertTestData(resultInfo);
    }

    private Optional<Path> getDataPath()
    {
        return testInfo.getCompareFiles() ? Optional.of(testInfo.getDataPath()) : Optional.empty();
    }

    private void verifyControlInfo()
    {
        verify(Files.exists(controlInfo.getResultInfoPath()), "Control result '%s' does not exist", controlInfo.getResultInfoPath());
    }

    private void createTestSpecFile()
            throws IOException
    {
        Files.write(testInfo.getSpecPath(), TEST_SPEC_JSON_CODEC.toJsonBytes(testInfo.getTestSpec()));
    }

    private void createResultInfoFile(ResultInfo resultInfo)
            throws IOException
    {
        Files.write(testInfo.getResultInfoPath(), RESULT_INFO_JSON_CODEC.toJsonBytes(resultInfo));
    }

    private void assertTestData(ResultInfo resultInfo)
            throws IOException
    {
        assertChecksums(resultInfo);
        if (!Files.exists(controlInfo.getDataPath()) || !Files.exists(testInfo.getDataPath())) {
            return;
        }

        try (BufferedReader controlReader = Files.newBufferedReader(controlInfo.getDataPath());
                BufferedReader testReader = Files.newBufferedReader(testInfo.getDataPath())) {
            String controlLine;
            String testLine;
            while (true) {
                controlLine = controlReader.readLine();
                testLine = testReader.readLine();
                if (controlLine == null || testLine == null) {
                    break;
                }
                assertEquals(controlLine, testLine);
            }
            checkState(controlLine == null && testLine == null, format("Files '%s' and '%s' have differing lengths", controlInfo.getDataPath(), testInfo.getDataPath()));
        }
    }

    private void assertChecksums(ResultInfo resultInfo)
            throws IOException
    {
        ResultInfo controlResultInfo = RESULT_INFO_JSON_CODEC.fromJson(Files.readAllBytes(controlInfo.getResultInfoPath()));
        checkState(controlResultInfo.equals(resultInfo), format("Result info differs for '%s' and '%s' differ", controlInfo.getResultInfoPath(), testInfo.getResultInfoPath()));
    }
}
