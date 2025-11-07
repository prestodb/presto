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
package com.facebook.presto.plugin.clp.split;

import com.facebook.presto.plugin.clp.ClpConfig;

import javax.inject.Inject;

import java.net.MalformedURLException;
import java.net.URL;

/**
 * Uber-specific implementation of CLP Pinot split provider.
 * <p>
 * At Uber, Pinot is accessed through Neutrino, a cross-region routing and aggregation service
 * that provides a unified interface for querying distributed Pinot clusters. This implementation
 * customizes the SQL query endpoint URL to use Neutrino's global statements API instead of
 * the standard Pinot query endpoint.
 * </p>
 */
public class UberClpPinotSplitProvider
        extends ClpPinotSplitProvider
{
    /**
     * Constructs an Uber CLP Pinot split provider with the given configuration.
     *
     * @param config the CLP configuration
     */
    @Inject
    public UberClpPinotSplitProvider(ClpConfig config)
    {
        super(config);
    }

    /**
     * Constructs the Neutrino SQL query endpoint URL for Uber's Pinot infrastructure.
     * <p>
     * Instead of using Pinot's standard {@code /query/sql} endpoint, this method constructs
     * a URL pointing to Neutrino's {@code /v1/globalStatements} endpoint, which provides
     * cross-region query routing and aggregation capabilities.
     * </p>
     *
     * @param config the CLP configuration containing the base Neutrino service URL
     * @return the Neutrino global statements endpoint URL
     * @throws MalformedURLException if the constructed URL is invalid
     */
    @Override
    protected URL buildPinotSqlQueryEndpointUrl(ClpConfig config) throws MalformedURLException
    {
        return new URL(config.getMetadataDbUrl() + "/v1/globalStatements");
    }
}
