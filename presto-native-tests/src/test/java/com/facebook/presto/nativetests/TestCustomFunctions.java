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
package com.facebook.presto.nativetests;

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.facebook.presto.nativetests.NativeTestsUtils.getCustomFunctionsPluginDirectory;
import static java.lang.Boolean.parseBoolean;

public class TestCustomFunctions
        extends AbstractTestQueryFramework
{
    private String storageFormat;
    private boolean sidecarEnabled;

    @BeforeSuite
    public void buildNativeLibrary()
            throws IOException, InterruptedException
    {
        // If we built with examples on, do not try to build.
        // This usually happens during the github pipeline.
        try {
            getCustomFunctionsPluginDirectory();
        }
        catch (Exception e) {
            Path prestoRoot = Paths.get(System.getProperty("user.dir")).toAbsolutePath();
            while (prestoRoot != null && !Files.exists(prestoRoot.resolve("presto-native-tests"))) {
                prestoRoot = prestoRoot.getParent();
            }
            if (prestoRoot == null) {
                throw new IllegalStateException("Could not locate presto root directory.");
            }
            String workingDir = prestoRoot
                    .resolve("presto-native-tests").toAbsolutePath().toString();
            ProcessBuilder builder = new ProcessBuilder("make", "debug"); // TODO "release"
            builder.directory(new File(workingDir));
            builder.redirectErrorStream(true);
            Process process = builder.start();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    System.out.println("[BUILD OUTPUT] " + line);
                }
            }
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new IllegalStateException("C++ build failed with exit code " + exitCode);
            }
        }
    }

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        storageFormat = System.getProperty("storageFormat", "PARQUET");
        sidecarEnabled = parseBoolean(System.getProperty("sidecarEnabled", "true"));
        super.init();
    }

    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        return NativeTestsUtils.createNativeQueryRunner(storageFormat, sidecarEnabled);
    }

    @Override
    protected void createTables()
    {
        NativeTestsUtils.createTables(storageFormat);
    }

    /// Sidecar is needed to support custom functions in Presto C++.
    @Test
    public void testCustomAdd()
    {
        if (sidecarEnabled) {
            assertQuery(
                    "SELECT custom_add(orderkey, custkey) FROM orders",
                    "SELECT orderkey + custkey FROM orders");
        }
    }
}
