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
package com.facebook.presto.plugin.jdbc;

/**
 * Provides table location information for JDBC connectors.
 * Different implementations can provide location information based on
 * different sources (e.g., connection URL, service discovery, etc.)
 */
public interface TableLocationProvider
{
    /**
     * Returns the location/URL for the table.
     * This could be a connection URL, service endpoint, or any other
     * location identifier relevant to the specific connector.
     */
    String getTableLocation();
}
