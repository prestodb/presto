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
package com.datastax.driver.core;

import com.datastax.oss.driver.api.core.cql.Row;

/**
 * Utility class for creating test Row objects.
 * Note: This is a stub implementation for test compatibility.
 * The new driver doesn't expose internal row construction APIs.
 */
public final class RowUtil
{
    private RowUtil()
    {
    }

    /**
     * Creates a single string row for testing.
     * Note: This method is deprecated and should not be used with the new driver.
     * Tests should use actual Cassandra queries to get Row objects.
     */
    @Deprecated
    public static Row createSingleStringRow(String value, int protocolVersion)
    {
        throw new UnsupportedOperationException(
                "Row creation is not supported with the new Cassandra driver. " +
                "Tests should use actual Cassandra queries to obtain Row objects.");
    }
}

// Made with Bob
