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
import com.facebook.presto.tests.AbstractTestFnServerAuth;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tests.utils.FnServerAuthTestUtils;

/**
 * Runs all {@link AbstractTestFnServerAuth} test cases against a
 * Java coordinator + C++ (native) worker cluster.
 *
 * Lives in presto-native-tests so it is gated behind the same CI pipeline
 * that builds and provides the native presto_server binary.
 */
public class TestFnServerAuthOnNativeCluster
        extends AbstractTestFnServerAuth
{
    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        return FnServerAuthTestUtils.createNativeRunnerWithValidHttpsFnServer();
    }

    @Override
    protected QueryRunner createRunnerWithOnlyMtls()
            throws Exception
    {
        return FnServerAuthTestUtils.createNativeRunnerWithOnlyMtls();
    }

    @Override
    protected QueryRunner createRunnerWithInvalidCertInFnServer()
            throws Exception
    {
        return FnServerAuthTestUtils.createNativeRunnerWithInvalidCertInFnServer();
    }

    @Override
    protected QueryRunner createRunnerWithWrongJwtSecretOnFnServer()
            throws Exception
    {
        return FnServerAuthTestUtils.createNativeRunnerWithWrongJwtSecretOnFnServer();
    }

    @Override
    protected QueryRunner createRunnerWithNoJwtOnFnServer()
            throws Exception
    {
        return FnServerAuthTestUtils.createNativeRunnerWithNoJwtOnFnServer();
    }
}
