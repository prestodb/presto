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

/**
 * Runs all {@link com.facebook.presto.tests.AbstractTestFnServerAuth} test cases against a
 * Java coordinator + C++ (native) worker cluster with mTLS + JWT.
 */
public class TestFnServerAuthWithMtlsAndJwtOnNativeCluster
        extends AbstractTestFnServerAuth
{
    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        return FnServerAuthTestUtils.createNativeRunnerWithMtlsAndJwt();
    }
}
