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
package com.facebook.presto.verifier.prestoaction;

import com.facebook.presto.verifier.annotation.ForControl;
import com.facebook.presto.verifier.annotation.ForTest;
import com.facebook.presto.verifier.framework.SourceQuery;
import com.facebook.presto.verifier.framework.VerificationContext;
import com.facebook.presto.verifier.retry.ForClusterConnection;
import com.facebook.presto.verifier.retry.ForPresto;
import com.facebook.presto.verifier.retry.RetryConfig;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class RoutingPrestoActionFactory
        implements PrestoActionFactory
{
    private final SqlExceptionClassifier exceptionClassifier;
    private final PrestoClusterConfig controlClusterConfig;
    private final PrestoClusterConfig testClusterConfig;
    private final RetryConfig networkRetryConfig;
    private final RetryConfig prestoRetryConfig;

    @Inject
    public RoutingPrestoActionFactory(
            SqlExceptionClassifier exceptionClassifier,
            @ForControl PrestoClusterConfig controlClusterConfig,
            @ForTest PrestoClusterConfig testClusterConfig,
            @ForClusterConnection RetryConfig networkRetryConfig,
            @ForPresto RetryConfig prestoRetryConfig)
    {
        this.exceptionClassifier = requireNonNull(exceptionClassifier, "exceptionClassifier is null");
        this.controlClusterConfig = requireNonNull(controlClusterConfig, "controlClusterConfig is null");
        this.testClusterConfig = requireNonNull(testClusterConfig, "testClusterConfig is null");
        this.networkRetryConfig = requireNonNull(networkRetryConfig, "networkRetryConfig is null");
        this.prestoRetryConfig = requireNonNull(prestoRetryConfig, "verifierConfig is null");
    }

    @Override
    public RoutingPrestoAction create(SourceQuery sourceQuery, VerificationContext verificationContext)
    {
        return new RoutingPrestoAction(
                new JdbcPrestoAction(
                        exceptionClassifier,
                        sourceQuery.getControlConfiguration(),
                        verificationContext,
                        controlClusterConfig,
                        networkRetryConfig,
                        prestoRetryConfig),
                new JdbcPrestoAction(
                        exceptionClassifier,
                        sourceQuery.getTestConfiguration(),
                        verificationContext,
                        testClusterConfig,
                        networkRetryConfig,
                        prestoRetryConfig));
    }
}
