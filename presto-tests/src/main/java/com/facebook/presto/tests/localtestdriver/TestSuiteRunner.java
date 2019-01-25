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

import com.facebook.presto.server.testing.TestingPrestoServer;
import com.google.common.io.RecursiveDeleteOption;

import java.io.Closeable;
import java.io.IOException;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static java.util.Objects.requireNonNull;

public class TestSuiteRunner
        implements Closeable
{
    private final TestingPrestoServer server;
    private final boolean force;
    private final TestSuiteInfo testSuiteInfo;

    public TestSuiteRunner(TestingPrestoServer server, TestSuiteSpec testSuiteSpec, boolean force, boolean useTempDirectory)
            throws IOException
    {
        this.server = requireNonNull(server, "server is null");
        this.force = force;
        this.testSuiteInfo = new TestSuiteInfo(testSuiteSpec, useTempDirectory);
    }

    public void run()
            throws Exception
    {
        ControlRunner controlRunner = new ControlRunner(testSuiteInfo.getControlInfo());
        controlRunner.run(server, force);
        for (TestInfo testInfo : testSuiteInfo.getTestInfos()) {
            TestRunner testRunner = new TestRunner(testInfo, testSuiteInfo.getControlInfo());
            testRunner.run(server);
        }
    }

    @Override
    public void close()
            throws IOException
    {
        // Cleans directory when declared in try-with-resources to reduce disk space while many tests run
        deleteRecursively(testSuiteInfo.getDirectoryToDelete(), RecursiveDeleteOption.ALLOW_INSECURE);
    }
}
