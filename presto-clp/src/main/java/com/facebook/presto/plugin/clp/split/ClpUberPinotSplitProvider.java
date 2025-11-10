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
import com.facebook.presto.plugin.clp.ClpTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.annotations.VisibleForTesting;

import javax.inject.Inject;

import java.net.MalformedURLException;
import java.net.URL;

import static java.util.Objects.requireNonNull;

/**
 * Uber-specific implementation of CLP Pinot split provider.
 * <p>
 * At Uber, Pinot is accessed through Neutrino, a cross-region routing and aggregation service
 * that provides a unified interface for querying distributed Pinot clusters. This implementation
 * customizes the SQL query endpoint URL to use Neutrino's global statements API instead of
 * the standard Pinot query endpoint.
 * </p>
 */
public class ClpUberPinotSplitProvider
        extends ClpPinotSplitProvider
{
    /**
     * Constructs an Uber CLP Pinot split provider with the given configuration.
     *
     * @param config the CLP configuration
     */
    @Inject
    public ClpUberPinotSplitProvider(ClpConfig config)
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

    /**
     * Infers the Uber-specific Pinot metadata table name from the CLP table handle.
     * <p>
     * At Uber, Pinot tables are organized under a specific namespace hierarchy.
     * All logging-related metadata tables are prefixed with {@code "rta.logging."}
     * to identify them within Uber's multi-tenant Pinot infrastructure. This prefix
     * represents:
     * <ul>
     *   <li><b>rta</b>: Real-Time Analytics platform namespace</li>
     *   <li><b>logging</b>: The logging subsystem within RTA</li>
     * </ul>
     * </p>
     * <p>
     * Unlike the standard Pinot implementation where schemas can affect table naming,
     * Uber's approach uses a flat namespace where all logging tables share the same
     * prefix regardless of the schema being queried.
     * </p>
     * <p>
     * Examples:
     * <ul>
     *   <li>Schema: "default", Table: "logs" → Pinot table: "rta.logging.logs"</li>
     *   <li>Schema: "production", Table: "events" → Pinot table: "rta.logging.events"</li>
     *   <li>Schema: "staging", Table: "metrics" → Pinot table: "rta.logging.metrics"</li>
     * </ul>
     * </p>
     *
     * @param tableHandle the CLP table handle containing schema and table information
     * @return the fully-qualified Pinot metadata table name with Uber's namespace prefix
     * @throws NullPointerException if tableHandle is null
     */
    @Override
    protected String inferMetadataTableName(ClpTableHandle tableHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        SchemaTableName schemaTableName = tableHandle.getSchemaTableName();

        // Uber's Pinot tables use a fixed namespace prefix for all logging tables
        // Format: rta.logging.<table_name>
        String tableName = schemaTableName.getTableName();
        return buildUberTableName(tableName);
    }

    /**
     * Factory method for building Uber-specific table names.
     * Exposed for testing purposes.
     *
     * @param tableName the base table name
     * @return the fully-qualified Uber Pinot table name
     */
    @VisibleForTesting
    protected String buildUberTableName(String tableName)
    {
        return String.format("rta.logging.%s", tableName);
    }
}
