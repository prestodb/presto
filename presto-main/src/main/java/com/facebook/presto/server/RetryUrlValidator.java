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

import com.facebook.airlift.log.Logger;

import javax.inject.Inject;

import java.net.URI;
import java.util.Set;

import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class RetryUrlValidator
{
    private static final Logger log = Logger.get(RetryUrlValidator.class);
    public static final String RETRY_PATH = "/v1/statement/queued/retry";

    private final RetryConfig retryConfig;

    @Inject
    public RetryUrlValidator(RetryConfig retryConfig)
    {
        this.retryConfig = requireNonNull(retryConfig, "retryConfig is null");
    }

    public boolean isValidRetryUrl(URI retryUrl, String currentServerHost)
    {
        requireNonNull(retryUrl, "retryUrl is null");
        if (!retryConfig.isRetryEnabled()) {
            return false;
        }

        try {
            // Check protocol
            if (retryConfig.isRequireHttps() && !"https".equalsIgnoreCase(retryUrl.getScheme())) {
                log.debug("Retry URL rejected - not HTTPS: %s", retryUrl);
                return false;
            }

            // Check path
            if (!retryUrl.getPath().startsWith(RETRY_PATH)) {
                log.debug("Retry URL rejected - invalid path: %s", retryUrl);
                return false;
            }

            if (retryUrl.getRawQuery() != null) {
                log.debug("Retry URL rejected - parameters present: %s", retryUrl);
                return false;
            }

            // Check domain allowlist
            if (!isDomainAllowed(retryUrl.getHost(), currentServerHost)) {
                log.debug("Retry URL rejected - domain not allowed: %s", retryUrl.getHost());
                return false;
            }

            return true;
        }
        catch (Exception e) {
            log.debug(e, "Invalid retry URL: %s", retryUrl);
            return false;
        }
    }

    private boolean isDomainAllowed(String host, String currentServerHost)
    {
        Set<String> allowedDomains = retryConfig.getAllowedRetryDomains();
        String lowerHost = host.toLowerCase(ENGLISH);

        // If no domains are configured, only allow same domain as current server
        if (allowedDomains.isEmpty()) {
            if (currentServerHost == null) {
                // Fallback to original behavior if current host not provided
                log.warn("Current server host not provided, cannot restrict to same domain");
                return false;
            }
            return lowerHost.equals(currentServerHost.toLowerCase(ENGLISH));
        }

        for (String allowedDomain : allowedDomains) {
            if (allowedDomain.startsWith("*.")) {
                // Wildcard domain
                String suffix = allowedDomain.substring(1);
                if (lowerHost.endsWith(suffix)) {
                    return true;
                }
            }
            else if (lowerHost.equals(allowedDomain)) {
                // Exact match
                return true;
            }
        }

        return false;
    }
}
