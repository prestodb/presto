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
package com.facebook.presto.server;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.presto.common.ErrorCode;
import com.facebook.presto.spi.StandardErrorCode;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import jakarta.validation.constraints.NotNull;

import java.util.Set;

import static com.facebook.presto.spi.StandardErrorCode.REMOTE_TASK_ERROR;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

public class RetryConfig
{
    private boolean retryEnabled = true;
    private Set<String> allowedRetryDomains = ImmutableSet.of();
    private boolean requireHttps;
    private Set<Integer> crossClusterRetryErrorCodes = ImmutableSet.of(
            REMOTE_TASK_ERROR.toErrorCode().getCode());

    public boolean isRetryEnabled()
    {
        return retryEnabled;
    }

    @Config("retry.enabled")
    @ConfigDescription("Enable cross-cluster retry functionality")
    public RetryConfig setRetryEnabled(boolean retryEnabled)
    {
        this.retryEnabled = retryEnabled;
        return this;
    }

    @NotNull
    public Set<String> getAllowedRetryDomains()
    {
        return allowedRetryDomains;
    }

    @Config("retry.allowed-domains")
    @ConfigDescription("Comma-separated list of allowed domains for retry URLs " +
            "(supports wildcards like *.example.com)")
    public RetryConfig setAllowedRetryDomains(String domains)
    {
        if (domains == null || domains.trim().isEmpty()) {
            this.allowedRetryDomains = ImmutableSet.of();
        }
        else {
            this.allowedRetryDomains = Splitter.on(',')
                    .trimResults()
                    .omitEmptyStrings()
                    .splitToList(domains)
                    .stream()
                    .map(String::toLowerCase)
                    .collect(toImmutableSet());
        }
        return this;
    }

    public boolean isRequireHttps()
    {
        return requireHttps;
    }

    @Config("retry.require-https")
    @ConfigDescription("Require HTTPS for retry URLs")
    public RetryConfig setRequireHttps(boolean requireHttps)
    {
        this.requireHttps = requireHttps;
        return this;
    }

    @NotNull
    public Set<Integer> getCrossClusterRetryErrorCodes()
    {
        return crossClusterRetryErrorCodes;
    }

    @Config("retry.cross-cluster-error-codes")
    @ConfigDescription("Comma-separated list of error codes that allow cross-cluster retry")
    public RetryConfig setCrossClusterRetryErrorCodes(String errorCodes)
    {
        if (errorCodes == null || errorCodes.trim().isEmpty()) {
            // Keep the default error codes
            return this;
        }
        else {
            this.crossClusterRetryErrorCodes = Splitter.on(',')
                    .trimResults()
                    .omitEmptyStrings()
                    .splitToList(errorCodes)
                    .stream()
                    .map(StandardErrorCode::valueOf)
                    .map(StandardErrorCode::toErrorCode)
                    .map(ErrorCode::getCode)
                    .collect(toImmutableSet());
        }
        return this;
    }
}
