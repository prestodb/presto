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

import com.facebook.presto.tests.AbstractTestFnServerAuth;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tests.utils.FnServerAuthTestUtils;
import org.testng.annotations.BeforeClass;

import static java.lang.Boolean.parseBoolean;

/**
 * Runs all {@link com.facebook.presto.tests.AbstractTestFnServerAuth} test cases against a
 * Java coordinator + C++ (native) worker cluster with mTLS only (no JWT).
 *
 * <p>The sidecar plugin behaviour is controlled via the {@code sidecarEnabled} system property
 * (default: {@code true}).  Run the suite twice — once with the default and once with
 * {@code -DsidecarEnabled=false} — to cover both paths.
 */
public class TestFnServerAuthWithMtlsOnlyOnNativeCluster
        extends AbstractTestFnServerAuth
{
    private boolean sidecarEnabled;

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        sidecarEnabled = parseBoolean(System.getProperty("sidecarEnabled", "true"));
        super.init();
    }

    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        return FnServerAuthTestUtils.createNativeRunnerWithOnlyMtls(sidecarEnabled);
    }
}
