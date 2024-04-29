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
package com.facebook.presto.nativeworker;

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.hive.HiveTestUtils.getProperty;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createLineitem;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.REMOTE_FUNCTION_CATALOG_NAME;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.REMOTE_FUNCTION_JSON_SIGNATURES;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.setupJsonFunctionNamespaceManager;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.startRemoteFunctionServer;

@Test(groups = "remote-function")
public abstract class AbstractTestNativeRemoteFunctions
        extends AbstractTestQueryFramework
{
    // The unix domain socket (UDS) path to communicate with the remote fuction server.
    protected String remoteFunctionServerUds;

    // The path to the compiled remote function server binary.
    private String remoteFunctionServerBinaryPath;

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        preInitQueryRunners();
        super.init();
        postInitQueryRunners();
    }

    protected void preInitQueryRunners()
    {
        // Initialize the remote function thrift server process.
        String propertyName = "REMOTE_FUNCTION_SERVER";
        remoteFunctionServerBinaryPath = getProperty(propertyName).orElseThrow(() -> new NullPointerException("Remote function server path missing. Add -DREMOTE_FUNCTION_SERVER=<path/to/binary>" +
                " to your JVM arguments, or create an environment variable REMOTE_FUNCTION_SERVER with value <path/to/binary>."));
        remoteFunctionServerUds = startRemoteFunctionServer(remoteFunctionServerBinaryPath);
    }

    protected void postInitQueryRunners()
    {
        // Install json function registration namespace manager.
        setupJsonFunctionNamespaceManager(getQueryRunner(), REMOTE_FUNCTION_JSON_SIGNATURES, REMOTE_FUNCTION_CATALOG_NAME);
    }

    @Override
    protected void createTables()
    {
        // Create some tpch tables.
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();
        createLineitem(queryRunner);
    }

    @Test
    public void testRemoteFunction()
    {
        assertQuery("SELECT remote.schema.ceil(linenumber) FROM lineitem", "SELECT ceil(linenumber) FROM lineitem");
        assertQuery("SELECT remote.schema.ceil(discount) FROM lineitem", "SELECT ceil(discount) FROM lineitem");
        assertQuery("SELECT remote.schema.ceil(discount * 7) FROM lineitem", "SELECT ceil(discount * 7) FROM lineitem");
        assertQuery("SELECT remote.schema.ceil(ceil(discount)) FROM lineitem", "SELECT ceil(ceil(discount * 7)) FROM lineitem");

        assertQuery("SELECT remote.schema.concat(returnflag, linestatus) FROM lineitem", "SELECT concat(returnflag, linestatus) FROM lineitem");
        assertQuery("SELECT remote.schema.concat('asd', linestatus) FROM lineitem", "SELECT concat('asd', linestatus) FROM lineitem");

        // TODO: `throwOnError` needs to be implemented in the server so we can have remote functions as function parameters.
        // https://github.com/facebookincubator/velox/issues/5288
        //
        // "(?s)" enables DOTALL mode so that "." can match newlines.
        assertQueryFails("SELECT remote.schema.ceil(remote.schema.ceil(discount)) FROM lineitem", "(?s).*Error.*");
    }
}
