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
package com.facebook.presto.plugin.clp.optimization;

import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.plugin.clp.ClpColumnHandle;
import com.facebook.presto.plugin.clp.TestClpQueryBase;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slices;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Basic tests for ClpFilterToKqlConverter focusing on metadata SQL generation
 * with string and numeric literals.
 */
@Test(singleThreaded = true)
public class TestClpFilterToKqlConverter
        extends TestClpQueryBase
{
    private ClpFilterToKqlConverter converter;
    private Map<VariableReferenceExpression, ColumnHandle> assignments;
    private Set<String> metadataFilterColumns;

    @BeforeMethod
    public void setUp()
    {
        assignments = new HashMap<>();
        metadataFilterColumns = ImmutableSet.of("hostname", "status_code");
        converter = new ClpFilterToKqlConverter(
                standardFunctionResolution,
                functionAndTypeManager,
                assignments,
                metadataFilterColumns);
    }

    /**
     * Test string literal equality with metadata SQL generation.
     * This is the main fix - ensuring string literals are handled correctly.
     */
    @Test
    public void testStringLiteralWithMetadataSql()
    {
        // Setup
        VariableReferenceExpression hostnameVar = new VariableReferenceExpression(
                Optional.empty(), "hostname", VARCHAR);
        ClpColumnHandle hostnameColumn = new ClpColumnHandle("hostname", "hostname", VARCHAR);
        assignments.put(hostnameVar, hostnameColumn);

        // Test: hostname = 'abc'
        ConstantExpression stringLiteral = new ConstantExpression(Slices.utf8Slice("abc"), VARCHAR);
        CallExpression equalCall = new CallExpression(
                Optional.empty(),
                "equal",
                standardFunctionResolution.comparisonFunction(OperatorType.EQUAL, VARCHAR, VARCHAR),
                BOOLEAN,
                ImmutableList.of(hostnameVar, stringLiteral));

        ClpExpression result = equalCall.accept(converter, null);

        // Verify
        assertTrue(result.getPushDownExpression().isPresent());
        assertEquals(result.getPushDownExpression().get(), "hostname: \"abc\"");
        assertTrue(result.getMetadataSqlQuery().isPresent());
        assertEquals(result.getMetadataSqlQuery().get(), "\"hostname\" = 'abc'");
    }

    /**
     * Test numeric literal equality with metadata SQL generation.
     */
    @Test
    public void testNumericLiteralWithMetadataSql()
    {
        // Setup
        VariableReferenceExpression statusCodeVar = new VariableReferenceExpression(
                Optional.empty(), "status_code", INTEGER);
        ClpColumnHandle statusCodeColumn = new ClpColumnHandle("status_code", "status_code", INTEGER);
        assignments.put(statusCodeVar, statusCodeColumn);

        // Test: status_code = 200
        ConstantExpression numericLiteral = new ConstantExpression(200L, INTEGER);
        CallExpression equalCall = new CallExpression(
                Optional.empty(),
                "equal",
                standardFunctionResolution.comparisonFunction(OperatorType.EQUAL, INTEGER, INTEGER),
                BOOLEAN,
                ImmutableList.of(statusCodeVar, numericLiteral));

        ClpExpression result = equalCall.accept(converter, null);

        // Verify
        assertTrue(result.getPushDownExpression().isPresent());
        assertEquals(result.getPushDownExpression().get(), "status_code: 200");
        assertTrue(result.getMetadataSqlQuery().isPresent());
        assertEquals(result.getMetadataSqlQuery().get(), "\"status_code\" = 200");
    }

    /**
     * Test escaping special characters in KQL string values.
     */
    @Test
    public void testEscapeKqlSpecialChars()
    {
        assertEquals(
                ClpFilterToKqlConverter.escapeKqlSpecialCharsForStringValue("path\\to\\file"),
                "path\\\\to\\\\file");
        assertEquals(
                ClpFilterToKqlConverter.escapeKqlSpecialCharsForStringValue("file*.txt"),
                "file\\*.txt");
        assertEquals(
                ClpFilterToKqlConverter.escapeKqlSpecialCharsForStringValue("normal_string"),
                "normal_string");
    }
}
