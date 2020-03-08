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

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.plugin.jdbc.StandardReadMappings.bigintReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.integerReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.smallintReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.tinyintReadMapping;
import static com.facebook.presto.plugin.jdbc.TestingJdbcTypeHandle.JDBC_BIGINT;
import static com.facebook.presto.plugin.jdbc.TestingJdbcTypeHandle.JDBC_INTEGER;
import static com.facebook.presto.plugin.jdbc.TestingJdbcTypeHandle.JDBC_INTEGER_UNSIGNED;
import static com.facebook.presto.plugin.jdbc.TestingJdbcTypeHandle.JDBC_SMALLINT;
import static com.facebook.presto.plugin.jdbc.TestingJdbcTypeHandle.JDBC_SMALLINT_UNSIGNED;
import static com.facebook.presto.plugin.jdbc.TestingJdbcTypeHandle.JDBC_TINYINT;
import static com.facebook.presto.plugin.jdbc.TestingJdbcTypeHandle.JDBC_TINYINT_UNSIGNED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestJdbcTypeToPrestoType
{
    @Test
    public void testUnsignedNumberType()
    {
        ImmutableMap<JdbcTypeHandle, ReadMapping> jdbcTypeHandleTypeAndPrestoType = ImmutableMap.<JdbcTypeHandle, ReadMapping>builder()
                .put(JDBC_TINYINT, tinyintReadMapping())
                .put(JDBC_TINYINT_UNSIGNED, smallintReadMapping())
                .put(JDBC_SMALLINT, smallintReadMapping())
                .put(JDBC_SMALLINT_UNSIGNED, integerReadMapping())
                .put(JDBC_INTEGER, integerReadMapping())
                .put(JDBC_INTEGER_UNSIGNED, bigintReadMapping())
                .put(JDBC_BIGINT, bigintReadMapping())
                .build();
        jdbcTypeHandleTypeAndPrestoType.forEach((jdbcTypeHandle, expectReadMapping) -> {
            Optional<ReadMapping> actualReadMapping = StandardReadMappings.jdbcTypeToPrestoType(jdbcTypeHandle);
            assertTrue(actualReadMapping.isPresent());
            assertEquals(actualReadMapping.get().getType(), expectReadMapping.getType());
        });
    }
}
