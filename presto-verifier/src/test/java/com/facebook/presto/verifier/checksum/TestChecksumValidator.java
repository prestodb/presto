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
package com.facebook.presto.verifier.checksum;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.SqlVarbinary;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.verifier.framework.Column;
import com.facebook.presto.verifier.framework.VerifierConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.sql.SqlFormatter.formatSql;
import static com.facebook.presto.sql.parser.IdentifierSymbol.AT_SIGN;
import static com.facebook.presto.sql.parser.IdentifierSymbol.COLON;
import static com.facebook.presto.verifier.VerifierTestUtil.createChecksumValidator;
import static com.facebook.presto.verifier.framework.VerifierUtil.PARSING_OPTIONS;
import static com.facebook.presto.verifier.framework.VerifierUtil.delimitedIdentifier;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestChecksumValidator
{
    private static final FunctionAndTypeManager FUNCTION_AND_TYPE_MANAGER = createTestFunctionAndTypeManager();

    private static final Column BIGINT_COLUMN = createColumn("bigint", BIGINT);
    private static final Column VARCHAR_COLUMN = createColumn("varchar", VARCHAR);
    private static final Column DOUBLE_COLUMN = createColumn("double", DOUBLE);
    private static final Column REAL_COLUMN = createColumn("real", REAL);
    private static final Column DOUBLE_ARRAY_COLUMN = createColumn("double_array", new ArrayType(DOUBLE));
    private static final Column REAL_ARRAY_COLUMN = createColumn("real_array", new ArrayType(REAL));
    private static final Column INT_ARRAY_COLUMN = createColumn("int_array", new ArrayType(INTEGER));
    private static final Column ROW_ARRAY_COLUMN = createColumn("row_array", FUNCTION_AND_TYPE_MANAGER.getType(parseTypeSignature("array(row(a int,b varchar))")));
    private static final Column MAP_ARRAY_COLUMN = createColumn("map_array", FUNCTION_AND_TYPE_MANAGER.getType(parseTypeSignature("array(map(int,varchar))")));
    private static final Column MAP_COLUMN = createColumn("map", FUNCTION_AND_TYPE_MANAGER.getType(parseTypeSignature("map(int,array(varchar))")));
    private static final Column MAP_FLOAT_NON_FLOAT_COLUMN = createColumn("map_float_non_float", FUNCTION_AND_TYPE_MANAGER.getType(parseTypeSignature("map(real,int)")));
    private static final Column MAP_NON_ORDERABLE_COLUMN = createColumn("map_non_orderable", FUNCTION_AND_TYPE_MANAGER.getType(parseTypeSignature("map(map(int,varchar),map(int,varchar))")));
    private static final Column ROW_COLUMN = createColumn("row", FUNCTION_AND_TYPE_MANAGER.getType(parseTypeSignature("row(i int, varchar, d double, a array(int), r row(double, b bigint))")));

    private static final double RELATIVE_ERROR_MARGIN = 1e-4;
    private static final double ABSOLUTE_ERROR_MARGIN = 1e-12;
    private static final Map<String, Object> FLOATING_POINT_COUNTS = ImmutableMap.<String, Object>builder()
            .put("double$nan_count", 2L)
            .put("double$pos_inf_count", 3L)
            .put("double$neg_inf_count", 4L)
            .put("real$nan_count", 2L)
            .put("real$pos_inf_count", 3L)
            .put("real$neg_inf_count", 4L)
            .build();
    private static final Map<String, Object> ROW_COLUMN_CHECKSUMS = ImmutableMap.<String, Object>builder()
            .put("row.i$checksum", new SqlVarbinary(new byte[] {0xa}))
            .put("row._col2$checksum", new SqlVarbinary(new byte[] {0xb}))
            .put("row.d$nan_count", 2L)
            .put("row.d$pos_inf_count", 3L)
            .put("row.d$neg_inf_count", 4L)
            .put("row.d$sum", 0.0)
            .put("row.a$checksum", new SqlVarbinary(new byte[] {0xc}))
            .put("row.a$cardinality_checksum", new SqlVarbinary(new byte[] {0xd}))
            .put("row.a$cardinality_sum", 2L)
            .put("row.r._col1$nan_count", 2L)
            .put("row.r._col1$pos_inf_count", 3L)
            .put("row.r._col1$neg_inf_count", 4L)
            .put("row.r._col1$sum", 0.0)
            .put("row.r.b$checksum", new SqlVarbinary(new byte[] {0xe}))
            .build();
    private static final SqlParser sqlParser = new SqlParser(new SqlParserOptions().allowIdentifierSymbol(COLON, AT_SIGN));

    private final ChecksumValidator checksumValidator = createChecksumValidator(new VerifierConfig()
            .setRelativeErrorMargin(RELATIVE_ERROR_MARGIN)
            .setAbsoluteErrorMargin(ABSOLUTE_ERROR_MARGIN));

    @Test
    public void testChecksumQuery()
    {
        Query checksumQuery = checksumValidator.generateChecksumQuery(
                QualifiedName.of("test:di"),
                ImmutableList.of(
                        BIGINT_COLUMN,
                        VARCHAR_COLUMN,
                        DOUBLE_COLUMN,
                        REAL_COLUMN,
                        DOUBLE_ARRAY_COLUMN,
                        REAL_ARRAY_COLUMN,
                        INT_ARRAY_COLUMN,
                        ROW_ARRAY_COLUMN,
                        MAP_ARRAY_COLUMN,
                        MAP_COLUMN,
                        MAP_FLOAT_NON_FLOAT_COLUMN,
                        MAP_NON_ORDERABLE_COLUMN,
                        ROW_COLUMN));
        Statement expectedChecksumQuery = sqlParser.createStatement(
                "SELECT\n" +
                        "  \"count\"(*)\n" +
                        ", \"checksum\"(\"bigint\") \"bigint$checksum\"\n" +
                        ", \"checksum\"(\"varchar\") \"varchar$checksum\"\n" +
                        ", \"sum\"(\"double\") FILTER (WHERE \"is_finite\"(\"double\")) \"double$sum\"\n" +
                        ", \"count\"(\"double\") FILTER (WHERE \"is_nan\"(\"double\")) \"double$nan_count\"\n" +
                        ", \"count\"(\"double\") FILTER (WHERE (\"double\" = \"infinity\"())) \"double$pos_inf_count\"\n" +
                        ", \"count\"(\"double\") FILTER (WHERE (\"double\" = -\"infinity\"())) \"double$neg_inf_count\"\n" +
                        ", \"sum\"(CAST(\"real\" AS double)) FILTER (WHERE \"is_finite\"(\"real\")) \"real$sum\"\n" +
                        ", \"count\"(\"real\") FILTER (WHERE \"is_nan\"(\"real\")) \"real$nan_count\"\n" +
                        ", \"count\"(\"real\") FILTER (WHERE (\"real\" = \"infinity\"())) \"real$pos_inf_count\"\n" +
                        ", \"count\"(\"real\") FILTER (WHERE (\"real\" = -\"infinity\"())) \"real$neg_inf_count\"\n" +
                        ", \"sum\"(\"array_sum\"(\"filter\"(\"double_array\", (x) -> \"is_finite\"(x)))) \"double_array$sum\"\n" +
                        ", \"sum\"(\"cardinality\"(\"filter\"(\"double_array\", (x) -> \"is_nan\"(x)))) \"double_array$nan_count\"\n" +
                        ", \"sum\"(\"cardinality\"(\"filter\"(\"double_array\", (x) -> (x = +\"infinity\"())))) \"double_array$pos_inf_count\"\n" +
                        ", \"sum\"(\"cardinality\"(\"filter\"(\"double_array\", (x) -> (x = -\"infinity\"())))) \"double_array$neg_inf_count\"\n" +
                        ", \"checksum\"(\"cardinality\"(\"double_array\")) \"double_array$cardinality_checksum\"\n" +
                        ", COALESCE(\"sum\"(\"cardinality\"(\"double_array\")), 0) \"double_array$cardinality_sum\"\n" +
                        ", \"sum\"(\"array_sum\"(\"filter\"(CAST(\"real_array\" AS array(double)), (x) -> \"is_finite\"(x)))) \"real_array$sum\"\n" +
                        ", \"sum\"(\"cardinality\"(\"filter\"(CAST(\"real_array\" AS array(double)), (x) -> \"is_nan\"(x)))) \"real_array$nan_count\"\n" +
                        ", \"sum\"(\"cardinality\"(\"filter\"(CAST(\"real_array\" AS array(double)), (x) -> (x = +\"infinity\"())))) \"real_array$pos_inf_count\"\n" +
                        ", \"sum\"(\"cardinality\"(\"filter\"(CAST(\"real_array\" AS array(double)), (x) -> (x = -\"infinity\"())))) \"real_array$neg_inf_count\"\n" +
                        ", \"checksum\"(\"cardinality\"(CAST(\"real_array\" AS array(double)))) \"real_array$cardinality_checksum\"\n" +
                        ", COALESCE(\"sum\"(\"cardinality\"(CAST(\"real_array\" AS array(double)))), 0) \"real_array$cardinality_sum\"\n" +
                        ", \"checksum\"(\"array_sort\"(\"int_array\")) \"int_array$checksum\"\n" +
                        ", \"checksum\"(\"cardinality\"(\"int_array\")) \"int_array$cardinality_checksum\"\n" +
                        ", COALESCE(\"sum\"(\"cardinality\"(\"int_array\")), 0) \"int_array$cardinality_sum\"\n" +
                        ", COALESCE(\"checksum\"(TRY(\"array_sort\"(\"row_array\"))), \"checksum\"(\"row_array\")) \"row_array$checksum\"\n" +
                        ", \"checksum\"(\"cardinality\"(\"row_array\")) \"row_array$cardinality_checksum\"\n" +
                        ", COALESCE(\"sum\"(\"cardinality\"(\"row_array\")), 0) \"row_array$cardinality_sum\"\n" +
                        ", \"checksum\"(\"map_array\") \"map_array$checksum\"\n" +
                        ", \"checksum\"(\"cardinality\"(\"map_array\")) \"map_array$cardinality_checksum\"\n" +
                        ", COALESCE(\"sum\"(\"cardinality\"(\"map_array\")), 0) \"map_array$cardinality_sum\"\n" +
                        ", \"checksum\"(\"map\") \"map$checksum\"\n" +
                        ", \"checksum\"(\"array_sort\"(\"map_keys\"(\"map\"))) \"map$keys_checksum\"\n" +
                        ", \"checksum\"(\"cardinality\"(\"map\")) \"map$cardinality_checksum\"\n" +
                        ", COALESCE(\"sum\"(\"cardinality\"(\"map\")), 0) \"map$cardinality_sum\"\n" +
                        ", \"checksum\"(\"map_float_non_float\") \"map_float_non_float$checksum\"\n" +
                        ", \"checksum\"(\"array_sort\"(\"map_keys\"(\"map_float_non_float\"))) \"map_float_non_float$keys_checksum\"\n" +
                        ", \"checksum\"(\"array_sort\"(\"map_values\"(\"map_float_non_float\"))) \"map_float_non_float$values_checksum\"\n" +
                        ", \"checksum\"(\"cardinality\"(\"map_float_non_float\")) \"map_float_non_float$cardinality_checksum\"\n" +
                        ", COALESCE(\"sum\"(\"cardinality\"(\"map_float_non_float\")), 0) \"map_float_non_float$cardinality_sum\"\n" +
                        ", \"checksum\"(\"map_non_orderable\") \"map_non_orderable$checksum\"\n" +
                        ", \"checksum\"(\"map_keys\"(\"map_non_orderable\")) \"map_non_orderable$keys_checksum\"\n" +
                        ", \"checksum\"(\"cardinality\"(\"map_non_orderable\")) \"map_non_orderable$cardinality_checksum\"\n" +
                        ", COALESCE(\"sum\"(\"cardinality\"(\"map_non_orderable\")), 0) \"map_non_orderable$cardinality_sum\"\n" +
                        ", \"checksum\"(\"row\".\"i\") \"row.i$checksum\"\n" +
                        ", \"checksum\"(\"row\"[2]) \"row._col2$checksum\"\n" +
                        ", \"sum\"(\"row\".\"d\") FILTER (WHERE \"is_finite\"(\"row\".\"d\")) \"row.d$sum\"\n" +
                        ", \"count\"(\"row\".\"d\") FILTER (WHERE \"is_nan\"(\"row\".\"d\")) \"row.d$nan_count\"\n" +
                        ", \"count\"(\"row\".\"d\") FILTER (WHERE (\"row\".\"d\" = \"infinity\"())) \"row.d$pos_inf_count\"\n" +
                        ", \"count\"(\"row\".\"d\") FILTER (WHERE (\"row\".\"d\" = -\"infinity\"())) \"row.d$neg_inf_count\"\n" +
                        ", \"checksum\"(\"array_sort\"(\"row\".\"a\")) \"row.a$checksum\"\n" +
                        ", \"checksum\"(\"cardinality\"(\"row\".\"a\")) \"row.a$cardinality_checksum\"\n" +
                        ", COALESCE(\"sum\"(\"cardinality\"(\"row\".\"a\")), 0) \"row.a$cardinality_sum\"\n" +
                        ", \"sum\"(\"row\".\"r\"[1]) FILTER (WHERE \"is_finite\"(\"row\".\"r\"[1])) \"row.r._col1$sum\"\n" +
                        ", \"count\"(\"row\".\"r\"[1]) FILTER (WHERE \"is_nan\"(\"row\".\"r\"[1])) \"row.r._col1$nan_count\"\n" +
                        ", \"count\"(\"row\".\"r\"[1]) FILTER (WHERE (\"row\".\"r\"[1] = \"infinity\"())) \"row.r._col1$pos_inf_count\"\n" +
                        ", \"count\"(\"row\".\"r\"[1]) FILTER (WHERE (\"row\".\"r\"[1] = -\"infinity\"())) \"row.r._col1$neg_inf_count\"\n" +
                        ", \"checksum\"(\"row\".\"r\".\"b\") \"row.r.b$checksum\"\n" +
                        "FROM\n" +
                        "  test:di\n",
                PARSING_OPTIONS);

        String[] arrayExpected = formatSql(expectedChecksumQuery, Optional.empty()).split("\n");
        String[] arrayActual = formatSql(checksumQuery, Optional.empty()).split("\n");
        Arrays.sort(arrayExpected);
        Arrays.sort(arrayActual);
        assertEquals(arrayActual, arrayExpected);
    }

    @Test
    public void testSimple()
    {
        List<Column> columns = ImmutableList.of(BIGINT_COLUMN, VARCHAR_COLUMN);
        ChecksumResult controlChecksum = new ChecksumResult(
                5,
                ImmutableMap.<String, Object>builder()
                        .put("bigint$checksum", new SqlVarbinary(new byte[] {0xa}))
                        .put("varchar$checksum", new SqlVarbinary(new byte[] {0xb}))
                        .build());

        // Matched
        assertTrue(checksumValidator.getMismatchedColumns(columns, controlChecksum, controlChecksum).isEmpty());

        // Mismatched
        ChecksumResult testChecksum = new ChecksumResult(
                5,
                ImmutableMap.<String, Object>builder()
                        .put("bigint$checksum", new SqlVarbinary(new byte[] {0x1a}))
                        .put("varchar$checksum", new SqlVarbinary(new byte[] {0x1b}))
                        .build());
        assertMismatchedColumns(columns, controlChecksum, testChecksum, BIGINT_COLUMN, VARCHAR_COLUMN);
    }

    @Test
    public void testFloatingPoint()
    {
        List<Column> columns = ImmutableList.of(DOUBLE_COLUMN, REAL_COLUMN);
        ChecksumResult controlChecksum = new ChecksumResult(
                5,
                ImmutableMap.<String, Object>builder()
                        .putAll(FLOATING_POINT_COUNTS)
                        .put("double$sum", 1.0)
                        .put("real$sum", 1.0)
                        .build());

        // Matched
        ChecksumResult testChecksum = new ChecksumResult(
                5,
                ImmutableMap.<String, Object>builder()
                        .putAll(FLOATING_POINT_COUNTS)
                        .put("double$sum", 1 + RELATIVE_ERROR_MARGIN)
                        .put("real$sum", 1 - RELATIVE_ERROR_MARGIN + RELATIVE_ERROR_MARGIN * RELATIVE_ERROR_MARGIN)
                        .build());
        assertTrue(checksumValidator.getMismatchedColumns(columns, controlChecksum, testChecksum).isEmpty());

        // Mismatched
        testChecksum = new ChecksumResult(
                5,
                ImmutableMap.<String, Object>builder()
                        .put("double$sum", 1.0)
                        .put("double$nan_count", 0L)
                        .put("double$pos_inf_count", 3L)
                        .put("double$neg_inf_count", 4L)
                        .put("real$sum", 1.0)
                        .put("real$nan_count", 2L)
                        .put("real$pos_inf_count", 0L)
                        .put("real$neg_inf_count", 4L)
                        .build());
        assertMismatchedColumns(columns, controlChecksum, testChecksum, DOUBLE_COLUMN, REAL_COLUMN);

        testChecksum = new ChecksumResult(
                5,
                ImmutableMap.<String, Object>builder()
                        .put("double$sum", 1.0)
                        .put("double$nan_count", 2L)
                        .put("double$pos_inf_count", 3L)
                        .put("double$neg_inf_count", 0L)
                        .put("real$sum", 1 - RELATIVE_ERROR_MARGIN)
                        .put("real$nan_count", 2L)
                        .put("real$pos_inf_count", 3L)
                        .put("real$neg_inf_count", 4L)
                        .build());
        List<ColumnMatchResult<?>> mismatchedColumns = assertMismatchedColumns(columns, controlChecksum, testChecksum, DOUBLE_COLUMN, REAL_COLUMN);
        assertEquals(mismatchedColumns.get(1).getMessage(), Optional.of("relative error: 1.0000500025000149E-4"));
    }

    @Test
    public void testFloatingPointArray()
    {
        List<Column> columns = ImmutableList.of(DOUBLE_ARRAY_COLUMN, REAL_ARRAY_COLUMN);

        Map<String, Object> stableCounts = new HashMap<>(10);
        stableCounts.put("double_array$nan_count", 2L);
        stableCounts.put("double_array$pos_inf_count", 3L);
        stableCounts.put("double_array$neg_inf_count", 4L);
        stableCounts.put("double_array$cardinality_checksum", new SqlVarbinary(new byte[] {0xb}));
        stableCounts.put("double_array$cardinality_sum", 10L);
        stableCounts.put("real_array$nan_count", 2L);
        stableCounts.put("real_array$pos_inf_count", 3L);
        stableCounts.put("real_array$neg_inf_count", 4L);
        stableCounts.put("real_array$cardinality_checksum", new SqlVarbinary(new byte[] {0xd}));
        stableCounts.put("real_array$cardinality_sum", 10L);

        Map<String, Object> emptyTableCounts = new HashMap<>(12);
        emptyTableCounts.put("double_array$nan_count", null);
        emptyTableCounts.put("double_array$pos_inf_count", null);
        emptyTableCounts.put("double_array$neg_inf_count", null);
        emptyTableCounts.put("double_array$cardinality_checksum", null);
        emptyTableCounts.put("double_array$cardinality_sum", null);
        emptyTableCounts.put("double_array$sum", null);
        emptyTableCounts.put("real_array$nan_count", null);
        emptyTableCounts.put("real_array$pos_inf_count", null);
        emptyTableCounts.put("real_array$neg_inf_count", null);
        emptyTableCounts.put("real_array$cardinality_checksum", null);
        emptyTableCounts.put("real_array$cardinality_sum", null);
        emptyTableCounts.put("real_array$sum", null);

        final ChecksumResult controlChecksum = new ChecksumResult(
                5,
                ImmutableMap.<String, Object>builder()
                        .putAll(stableCounts)
                        .put("double_array$sum", 1.0)
                        .put("real_array$sum", 1.0)
                        .build());

        // Matched
        ChecksumResult testChecksum = new ChecksumResult(
                5,
                ImmutableMap.<String, Object>builder()
                        .putAll(stableCounts)
                        .put("double_array$sum", 1 + RELATIVE_ERROR_MARGIN)
                        .put("real_array$sum", 1 - RELATIVE_ERROR_MARGIN + RELATIVE_ERROR_MARGIN * RELATIVE_ERROR_MARGIN)
                        .build());
        assertTrue(checksumValidator.getMismatchedColumns(columns, controlChecksum, testChecksum).isEmpty());

        // Matched two empty tables
        final ChecksumResult testEmptyChecksum = new ChecksumResult(0, emptyTableCounts);
        assertTrue(checksumValidator.getMismatchedColumns(columns, testEmptyChecksum, testEmptyChecksum).isEmpty());

        // Mismatched empty and non-empty (number of rows differs).
        assertThatThrownBy(() -> checksumValidator.getMismatchedColumns(columns, controlChecksum, testEmptyChecksum))
                .isInstanceOf(IllegalArgumentException.class);

        // Mismatched
        testChecksum = new ChecksumResult(
                5,
                ImmutableMap.<String, Object>builder()
                        .put("double_array$sum", 1.0)
                        .put("double_array$nan_count", 0L)
                        .put("double_array$pos_inf_count", 3L)
                        .put("double_array$neg_inf_count", 4L)
                        .put("double_array$cardinality_checksum", new SqlVarbinary(new byte[] {0xb}))
                        .put("double_array$cardinality_sum", 10L)
                        .put("real_array$sum", 1.0)
                        .put("real_array$nan_count", 2L)
                        .put("real_array$pos_inf_count", 0L)
                        .put("real_array$neg_inf_count", 4L)
                        .put("real_array$cardinality_checksum", new SqlVarbinary(new byte[] {0xd}))
                        .put("real_array$cardinality_sum", 10L)
                        .build());
        assertMismatchedColumns(columns, controlChecksum, testChecksum, DOUBLE_ARRAY_COLUMN, REAL_ARRAY_COLUMN);

        testChecksum = new ChecksumResult(
                5,
                ImmutableMap.<String, Object>builder()
                        .put("double_array$sum", 1.0)
                        .put("double_array$nan_count", 2L)
                        .put("double_array$pos_inf_count", 3L)
                        .put("double_array$neg_inf_count", 0L)
                        .put("double_array$cardinality_checksum", new SqlVarbinary(new byte[] {0xb}))
                        .put("double_array$cardinality_sum", 10L)
                        .put("real_array$sum", 1 - RELATIVE_ERROR_MARGIN)
                        .put("real_array$nan_count", 2L)
                        .put("real_array$pos_inf_count", 3L)
                        .put("real_array$neg_inf_count", 4L)
                        .put("real_array$cardinality_checksum", new SqlVarbinary(new byte[] {0xd}))
                        .put("real_array$cardinality_sum", 10L)
                        .build());
        List<ColumnMatchResult<?>> mismatchedColumns = assertMismatchedColumns(columns, controlChecksum, testChecksum, DOUBLE_ARRAY_COLUMN, REAL_ARRAY_COLUMN);
        assertEquals(mismatchedColumns.get(1).getMessage(), Optional.of("relative error: 1.0000500025000149E-4"));
    }

    @Test
    public void testFloatingPointWithNull()
    {
        List<Column> columns = ImmutableList.of(DOUBLE_COLUMN, REAL_COLUMN);
        Map<String, Object> controlResult = new HashMap<>(FLOATING_POINT_COUNTS);
        controlResult.put("double$sum", 1.0);
        controlResult.put("real$sum", null);
        ChecksumResult controlChecksum = new ChecksumResult(5, controlResult);

        Map<String, Object> testResult = new HashMap<>(FLOATING_POINT_COUNTS);
        testResult.put("double$sum", null);
        testResult.put("real$sum", 1.0);
        ChecksumResult testChecksum = new ChecksumResult(5, testResult);

        assertMismatchedColumns(columns, controlChecksum, testChecksum, DOUBLE_COLUMN, REAL_COLUMN);
    }

    @Test
    public void testFloatingPointCloseToZero()
    {
        List<Column> columns = ImmutableList.of(DOUBLE_COLUMN, REAL_COLUMN);

        // Matched
        ChecksumResult controlChecksum = new ChecksumResult(
                5,
                ImmutableMap.<String, Object>builder()
                        .putAll(FLOATING_POINT_COUNTS)
                        .put("double$sum", -4.9e-12)
                        .put("real$sum", 4.9e-12)
                        .build());
        ChecksumResult testChecksum = new ChecksumResult(
                5,
                ImmutableMap.<String, Object>builder()
                        .putAll(FLOATING_POINT_COUNTS)
                        .put("double$sum", 4.9e-12)
                        .put("real$sum", 0.0)
                        .build());
        assertTrue(checksumValidator.getMismatchedColumns(columns, controlChecksum, testChecksum).isEmpty());

        // Mismatched
        controlChecksum = new ChecksumResult(
                5,
                ImmutableMap.<String, Object>builder()
                        .putAll(FLOATING_POINT_COUNTS)
                        .put("double$sum", 0.0)
                        .put("real$sum", 5.1e-12)
                        .build());
        testChecksum = new ChecksumResult(
                5,
                ImmutableMap.<String, Object>builder()
                        .putAll(FLOATING_POINT_COUNTS)
                        .put("double$sum", 5.1e-12)
                        .put("real$sum", 0.0)
                        .build());
        assertMismatchedColumns(columns, controlChecksum, testChecksum, DOUBLE_COLUMN, REAL_COLUMN);
    }

    @Test
    public void testArray()
    {
        List<Column> columns = ImmutableList.of(INT_ARRAY_COLUMN, MAP_ARRAY_COLUMN);
        ChecksumResult controlChecksum = new ChecksumResult(
                5,
                ImmutableMap.<String, Object>builder()
                        .put("int_array$checksum", new SqlVarbinary(new byte[] {0xa}))
                        .put("int_array$cardinality_checksum", new SqlVarbinary(new byte[] {0xb}))
                        .put("int_array$cardinality_sum", 1L)
                        .put("map_array$checksum", new SqlVarbinary(new byte[] {0xc}))
                        .put("map_array$cardinality_checksum", new SqlVarbinary(new byte[] {0xd}))
                        .put("map_array$cardinality_sum", 2L)
                        .build());

        // Matched
        assertTrue(checksumValidator.getMismatchedColumns(columns, controlChecksum, controlChecksum).isEmpty());

        // Mismatched different elements
        ChecksumResult testChecksum = new ChecksumResult(
                5,
                ImmutableMap.<String, Object>builder()
                        .put("int_array$checksum", new SqlVarbinary(new byte[] {0x1a}))
                        .put("int_array$cardinality_checksum", new SqlVarbinary(new byte[] {0xb}))
                        .put("int_array$cardinality_sum", 1L)
                        .put("map_array$checksum", new SqlVarbinary(new byte[] {0x1c}))
                        .put("map_array$cardinality_checksum", new SqlVarbinary(new byte[] {0xd}))
                        .put("map_array$cardinality_sum", 2L)
                        .build());
        assertMismatchedColumns(columns, controlChecksum, testChecksum, INT_ARRAY_COLUMN, MAP_ARRAY_COLUMN);

        // Mismatched different cardinality checksum
        testChecksum = new ChecksumResult(
                5,
                ImmutableMap.<String, Object>builder()
                        .put("int_array$checksum", new SqlVarbinary(new byte[] {0xa}))
                        .put("int_array$cardinality_checksum", new SqlVarbinary(new byte[] {0x1b}))
                        .put("int_array$cardinality_sum", 1L)
                        .put("map_array$checksum", new SqlVarbinary(new byte[] {0xc}))
                        .put("map_array$cardinality_checksum", new SqlVarbinary(new byte[] {0x1d}))
                        .put("map_array$cardinality_sum", 2L)
                        .build());
        assertMismatchedColumns(columns, controlChecksum, testChecksum, INT_ARRAY_COLUMN, MAP_ARRAY_COLUMN);

        // Mismatched different cardinality sum
        testChecksum = new ChecksumResult(
                5,
                ImmutableMap.<String, Object>builder()
                        .put("int_array$checksum", new SqlVarbinary(new byte[] {0xa}))
                        .put("int_array$cardinality_checksum", new SqlVarbinary(new byte[] {0xb}))
                        .put("int_array$cardinality_sum", 3L)
                        .put("map_array$checksum", new SqlVarbinary(new byte[] {0xc}))
                        .put("map_array$cardinality_checksum", new SqlVarbinary(new byte[] {0xd}))
                        .put("map_array$cardinality_sum", 4L)
                        .build());
        assertMismatchedColumns(columns, controlChecksum, testChecksum, INT_ARRAY_COLUMN, MAP_ARRAY_COLUMN);
    }

    @Test
    public void testRow()
    {
        List<Column> columns = ImmutableList.of(ROW_COLUMN);
        ChecksumResult controlChecksum = new ChecksumResult(ROW_COLUMN_CHECKSUMS.size(), ROW_COLUMN_CHECKSUMS);

        assertTrue(checksumValidator.getMismatchedColumns(columns, controlChecksum, controlChecksum).isEmpty());

        // Mismatched different elements
        ChecksumResult testChecksum = new ChecksumResult(
                ROW_COLUMN_CHECKSUMS.size(),
                merge(ROW_COLUMN_CHECKSUMS, ImmutableMap.<String, Object>builder()
                        .put("row.i$checksum", new SqlVarbinary(new byte[] {0x1a}))
                        .put("row.r.b$checksum", new SqlVarbinary(new byte[] {0x1d}))
                        .build()));

        Column aFieldColumn = Column.create("row.i", new DereferenceExpression(ROW_COLUMN.getExpression(), new Identifier("i")), INTEGER);
        Column rbFieldColumn = Column.create("row.r.b", new DereferenceExpression(new DereferenceExpression(ROW_COLUMN.getExpression(), new Identifier("r")), new Identifier("b")), BIGINT);
        assertMismatchedColumns(columns, controlChecksum, testChecksum, aFieldColumn, rbFieldColumn);
    }

    @Test
    public void testMapNoValuesCheckSum()
    {
        List<Column> columns = ImmutableList.of(MAP_COLUMN);

        // Match.
        Map<String, Object> countsBase = new HashMap<>(4);
        countsBase.put("map$checksum", new SqlVarbinary(new byte[] {0xa}));
        countsBase.put("map$keys_checksum", new SqlVarbinary(new byte[] {0xb}));
        countsBase.put("map$cardinality_sum", 3L);
        countsBase.put("map$cardinality_checksum", new SqlVarbinary(new byte[] {0xd}));
        ChecksumResult controlChecksumBase = new ChecksumResult(5, countsBase);
        assertTrue(checksumValidator.getMismatchedColumns(columns, controlChecksumBase, controlChecksumBase).isEmpty());

        // Mismatch in checksum.
        Map<String, Object> countsA = new HashMap<>(5);
        countsA.put("map$checksum", new SqlVarbinary(new byte[] {0xb}));
        countsA.put("map$keys_checksum", new SqlVarbinary(new byte[] {0xb}));
        countsA.put("map$values_checksum", null);
        countsA.put("map$cardinality_sum", 3L);
        countsA.put("map$cardinality_checksum", new SqlVarbinary(new byte[] {0xd}));
        ChecksumResult controlChecksumA = new ChecksumResult(5, countsA);
        assertMismatchedColumns(columns, controlChecksumBase, controlChecksumA, MAP_COLUMN);

        // Mismatch in cardinality sum.
        Map<String, Object> countsB = new HashMap<>(4);
        countsB.put("map$checksum", new SqlVarbinary(new byte[] {0xa}));
        countsB.put("map$keys_checksum", new SqlVarbinary(new byte[] {0xb}));
        countsB.put("map$cardinality_sum", 4L);
        countsB.put("map$cardinality_checksum", new SqlVarbinary(new byte[] {0xd}));
        ChecksumResult controlChecksumB = new ChecksumResult(5, countsB);
        assertMismatchedColumns(columns, controlChecksumBase, controlChecksumB, MAP_COLUMN);
    }

    @Test
    public void testMap()
    {
        List<Column> columns = ImmutableList.of(MAP_COLUMN);

        ChecksumResult controlChecksum = new ChecksumResult(
                5,
                ImmutableMap.<String, Object>builder()
                        .put("map$checksum", new SqlVarbinary(new byte[] {0xa}))
                        .put("map$keys_checksum", new SqlVarbinary(new byte[] {0xb}))
                        .put("map$values_checksum", new SqlVarbinary(new byte[] {0xc}))
                        .put("map$cardinality_sum", 3L)
                        .put("map$cardinality_checksum", new SqlVarbinary(new byte[] {0xd}))
                        .build());

        // Matched
        assertTrue(checksumValidator.getMismatchedColumns(columns, controlChecksum, controlChecksum).isEmpty());

        // Mismatched map checksum
        ChecksumResult testChecksum = new ChecksumResult(
                5,
                ImmutableMap.<String, Object>builder()
                        .put("map$checksum", new SqlVarbinary(new byte[] {0x1a}))
                        .put("map$keys_checksum", new SqlVarbinary(new byte[] {0xb}))
                        .put("map$values_checksum", new SqlVarbinary(new byte[] {0xc}))
                        .put("map$cardinality_sum", 3L)
                        .put("map$cardinality_checksum", new SqlVarbinary(new byte[] {0xd}))
                        .build());
        assertMismatchedColumns(columns, controlChecksum, testChecksum, MAP_COLUMN);

        // Mismatched keys checksum
        testChecksum = new ChecksumResult(
                5,
                ImmutableMap.<String, Object>builder()
                        .put("map$checksum", new SqlVarbinary(new byte[] {0xa}))
                        .put("map$keys_checksum", new SqlVarbinary(new byte[] {0x1b}))
                        .put("map$values_checksum", new SqlVarbinary(new byte[] {0xc}))
                        .put("map$cardinality_checksum", new SqlVarbinary(new byte[] {0xd}))
                        .put("map$cardinality_sum", 3L)
                        .build());
        assertMismatchedColumns(columns, controlChecksum, testChecksum, MAP_COLUMN);

        // Mismatched values checksum
        testChecksum = new ChecksumResult(
                5,
                ImmutableMap.<String, Object>builder()
                        .put("map$checksum", new SqlVarbinary(new byte[] {0xa}))
                        .put("map$keys_checksum", new SqlVarbinary(new byte[] {0xb}))
                        .put("map$values_checksum", new SqlVarbinary(new byte[] {0x1c}))
                        .put("map$cardinality_checksum", new SqlVarbinary(new byte[] {0xd}))
                        .put("map$cardinality_sum", 3L)
                        .build());
        assertMismatchedColumns(columns, controlChecksum, testChecksum, MAP_COLUMN);

        // Mismatched cardinality checksum
        testChecksum = new ChecksumResult(
                5,
                ImmutableMap.<String, Object>builder()
                        .put("map$checksum", new SqlVarbinary(new byte[] {0xa}))
                        .put("map$keys_checksum", new SqlVarbinary(new byte[] {0xb}))
                        .put("map$values_checksum", new SqlVarbinary(new byte[] {0xc}))
                        .put("map$cardinality_checksum", new SqlVarbinary(new byte[] {0x1d}))
                        .put("map$cardinality_sum", 3L)
                        .build());
        assertMismatchedColumns(columns, controlChecksum, testChecksum, MAP_COLUMN);

        // Mismatched cardinality sum
        testChecksum = new ChecksumResult(
                5,
                ImmutableMap.<String, Object>builder()
                        .put("map$checksum", new SqlVarbinary(new byte[] {0xa}))
                        .put("map$keys_checksum", new SqlVarbinary(new byte[] {0xb}))
                        .put("map$values_checksum", new SqlVarbinary(new byte[] {0xc}))
                        .put("map$cardinality_checksum", new SqlVarbinary(new byte[] {0xd}))
                        .put("map$cardinality_sum", 4L)
                        .build());
        assertMismatchedColumns(columns, controlChecksum, testChecksum, MAP_COLUMN);
    }

    private List<ColumnMatchResult<?>> assertMismatchedColumns(List<Column> columns, ChecksumResult controlChecksum, ChecksumResult testChecksum, Column... expected)
    {
        List<ColumnMatchResult<?>> mismatchedColumns = ImmutableList.copyOf(checksumValidator.getMismatchedColumns(columns, controlChecksum, testChecksum));
        List<Column> actual = mismatchedColumns.stream()
                .map(ColumnMatchResult::getColumn)
                .collect(toImmutableList());
        assertEquals(actual, asList(expected));
        return mismatchedColumns;
    }

    //ImmutableMap.builder() does not allow overlapping keys
    private Map<String, Object> merge(Map<String, Object> origin, Map<String, Object> layer)
    {
        HashMap<String, Object> result = new HashMap<>(origin);
        result.putAll(layer);
        return result;
    }

    private static Column createColumn(String name, Type type)
    {
        return Column.create(name, delimitedIdentifier(name), type);
    }
}
