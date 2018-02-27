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
package com.facebook.presto.connector.thrift.integration;

import com.facebook.presto.connector.thrift.TestingThriftService;
import com.facebook.presto.connector.thrift.api.PrestoThriftService;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.function.Supplier;

import static com.facebook.presto.connector.thrift.integration.ThriftQueryRunner.createThriftQueryRunner;
import static com.facebook.presto.spi.session.PropertyMetadata.booleanSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.doubleSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.integerSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.longSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.stringSessionProperty;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;

public class TestThriftSessionPropertiesRegistration
{
    private QueryRunner queryRunner;

    @BeforeClass
    void setup()
            throws Exception
    {
        Supplier<PrestoThriftService> servers = new Supplier<PrestoThriftService>()
        {
            @Override
            public PrestoThriftService get()
            {
                return new TestingThriftService(ImmutableList.of(
                        booleanSessionProperty("test_bool_prop", "boolean session property", true, false),
                        booleanSessionProperty("test_bool_prop_nullable", "nullable boolean session property", null, false),
                        integerSessionProperty("test_int_prop", "integer session property", 100, false),
                        integerSessionProperty("test_int_prop_nullable", "nullable integer session property", null, false),
                        longSessionProperty("test_long_prop", "long session property", 1000L, false),
                        longSessionProperty("test_long_prop_nullable", "nullable long session property", null, false),
                        doubleSessionProperty("test_double_prop", "double session property", 1.9, false),
                        doubleSessionProperty("test_double_prop_nullable", "nullable double session property", null, false),
                        stringSessionProperty("test_string_prop", "string session property", "This is a string", false),
                        stringSessionProperty("test_string_prop_nullable", "nullable string session property", null, false)));
            }
        };

        queryRunner = createThriftQueryRunner(servers, 3, 2);
    }

    @AfterClass
    void shutdown()
    {
        queryRunner.close();
    }

    @Test
    void testRegisterSessionProperties()
    {
        MaterializedResult result = queryRunner.execute(testSessionBuilder()
                        .setCatalogSessionProperty("thrift", "test_bool_prop", "false")
                        .setCatalogSessionProperty("thrift", "test_bool_prop_nullable", "false")
                        .setCatalogSessionProperty("thrift", "test_int_prop", "10")
                        .setCatalogSessionProperty("thrift", "test_int_prop_nullable", "10")
                        .setCatalogSessionProperty("thrift", "test_long_prop", "2000")
                        .setCatalogSessionProperty("thrift", "test_long_prop_nullable", "2000")
                        .setCatalogSessionProperty("thrift", "test_double_prop", "77.81")
                        .setCatalogSessionProperty("thrift", "test_double_prop_nullable", "2000")
                        .setCatalogSessionProperty("thrift", "test_string_prop", "Another string")
                        .setCatalogSessionProperty("thrift", "test_string_prop_nullable", "Another string")
                        .build(),
                "show session");
        assertEquals(getSessionRowFromResult(result, "thrift", "test_bool_prop"), ImmutableList.of("thrift.test_bool_prop", "false", "true", "boolean", "boolean session property"));
        assertEquals(getSessionRowFromResult(result, "thrift", "test_bool_prop_nullable"), ImmutableList.of("thrift.test_bool_prop_nullable", "false", "", "boolean", "nullable boolean session property"));
        assertEquals(getSessionRowFromResult(result, "thrift", "test_int_prop"), ImmutableList.of("thrift.test_int_prop", "10", "100", "integer", "integer session property"));
        assertEquals(getSessionRowFromResult(result, "thrift", "test_int_prop_nullable"), ImmutableList.of("thrift.test_int_prop_nullable", "10", "", "integer", "nullable integer session property"));
        assertEquals(getSessionRowFromResult(result, "thrift", "test_long_prop"), ImmutableList.of("thrift.test_long_prop", "2000", "1000", "bigint", "long session property"));
        assertEquals(getSessionRowFromResult(result, "thrift", "test_long_prop_nullable"), ImmutableList.of("thrift.test_long_prop_nullable", "2000", "", "bigint", "nullable long session property"));
        assertEquals(getSessionRowFromResult(result, "thrift", "test_double_prop"), ImmutableList.of("thrift.test_double_prop", "77.81", "1.9", "double", "double session property"));
        assertEquals(getSessionRowFromResult(result, "thrift", "test_double_prop_nullable"), ImmutableList.of("thrift.test_double_prop_nullable", "77.81", "", "double", "nullable double session property"));
        assertEquals(getSessionRowFromResult(result, "thrift", "test_string_prop"), ImmutableList.of("thrift.test_string_prop", "Another string", "This is a string", "varchar", "string session property"));
        assertEquals(getSessionRowFromResult(result, "thrift", "test_string_prop_nullable"), ImmutableList.of("thrift.test_string_prop_nullable", "Another string", "", "varchar", "nullable string session property"));
    }

    private List<Object> getSessionRowFromResult(MaterializedResult result, String catalogName, String propertyName)
    {
        return result.getMaterializedRows().stream()
                .filter(row -> row.getField(0).equals(catalogName + "." + propertyName))
                .findAny()
                .map(MaterializedRow::getFields)
                .orElse(null);
    }
}
