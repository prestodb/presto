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
package com.facebook.presto.verifier.framework;

import com.facebook.presto.verifier.retry.ForClusterConnection;
import com.facebook.presto.verifier.retry.ForPresto;
import com.facebook.presto.verifier.retry.RetryConfig;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class JdbcPrestoActionFactory
        implements PrestoActionFactory
{
    private final SqlExceptionClassifier exceptionClassifier;
    private final VerifierConfig verifierConfig;
    private final RetryConfig networkRetryConfig;
    private final RetryConfig prestoRetryConfig;

    @Inject
    public JdbcPrestoActionFactory(
            SqlExceptionClassifier exceptionClassifier,
            VerifierConfig verifierConfig,
            @ForClusterConnection RetryConfig networkRetryConfig,
            @ForPresto RetryConfig prestoRetryConfig)
    {
        this.exceptionClassifier = requireNonNull(exceptionClassifier, "exceptionClassifier is null");
        this.verifierConfig = requireNonNull(verifierConfig, "verifierConfig is null");
        this.networkRetryConfig = requireNonNull(networkRetryConfig, "verifierConfig is null");
        this.prestoRetryConfig = requireNonNull(prestoRetryConfig, "verifierConfig is null");
    }

    @Override
    public JdbcPrestoAction create(
            QueryConfiguration controlConfiguration,
            QueryConfiguration testConfiguration,
            VerificationContext verificationContext)
    {
        return new JdbcPrestoAction(
                exceptionClassifier,
                controlConfiguration,
                testConfiguration,
                verificationContext,
                verifierConfig,
                networkRetryConfig,
                prestoRetryConfig);
    }
}
