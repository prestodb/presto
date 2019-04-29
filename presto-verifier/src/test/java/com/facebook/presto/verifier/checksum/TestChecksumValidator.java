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

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.SqlVarbinary;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.type.TypeRegistry;
import com.facebook.presto.verifier.framework.Column;
import com.facebook.presto.verifier.framework.VerifierConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.SqlFormatter.formatSql;
import static com.facebook.presto.sql.parser.IdentifierSymbol.AT_SIGN;
import static com.facebook.presto.sql.parser.IdentifierSymbol.COLON;
import static com.facebook.presto.verifier.framework.Column.Category.FLOATING_POINT;
import static com.facebook.presto.verifier.framework.Column.Category.ORDERABLE_ARRAY;
import static com.facebook.presto.verifier.framework.Column.Category.SIMPLE;
import static com.facebook.presto.verifier.framework.VerifierUtil.PARSING_OPTIONS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestChecksumValidator
{
    private static final TypeRegistry typeRegistry = new TypeRegistry();

    static {
        new FunctionManager(typeRegistry, new BlockEncodingManager(typeRegistry), new FeaturesConfig());
    }

    private static final Column BIGINT_COLUMN = new Column("bigint", SIMPLE, BIGINT);
    private static final Column VARCHAR_COLUMN = new Column("varchar", SIMPLE, VARCHAR);
    private static final Column DOUBLE_COLUMN = new Column("double", FLOATING_POINT, DOUBLE);
    private static final Column REAL_COLUMN = new Column("real", FLOATING_POINT, REAL);
    private static final Column INT_ARRAY_COLUMN = new Column("int_array", ORDERABLE_ARRAY, new ArrayType(INTEGER));
    private static final Column ROW_ARRAY_COLUMN = new Column("row_array", ORDERABLE_ARRAY, typeRegistry.getType(parseTypeSignature("array(row(a int,b varchar))")));
    private static final Column MAP_ARRAY_COLUMN = new Column("map_array", SIMPLE, typeRegistry.getType(parseTypeSignature("array(map(int,varchar))")));

    private static final double RELATIVE_ERROR_MARGIN = 1e-4;
    private static final double ABSOLUTE_ERROR_MARGIN = 1e-12;
    private static final Map<String, Object> FLOATING_POINT_COUNTS = ImmutableMap.<String, Object>builder()
            .put("double_nan_count", 2L)
            .put("double_pos_inf_count", 3L)
            .put("double_neg_inf_count", 4L)
            .put("real_nan_count", 2L)
            .put("real_pos_inf_count", 3L)
            .put("real_neg_inf_count", 4L)
            .build();
    private static final SqlParser sqlParser = new SqlParser(new SqlParserOptions().allowIdentifierSymbol(COLON, AT_SIGN));

    private final ChecksumValidator checksumValidator = new ChecksumValidator(
            new SimpleColumnValidator(),
            new FloatingPointColumnValidator(new VerifierConfig().setRelativeErrorMargin(RELATIVE_ERROR_MARGIN).setAbsoluteErrorMargin(ABSOLUTE_ERROR_MARGIN)),
            new OrderableArrayColumnValidator());

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
                        INT_ARRAY_COLUMN,
                        ROW_ARRAY_COLUMN,
                        MAP_ARRAY_COLUMN));
        Statement expectedChecksumQuery = sqlParser.createStatement(
                "SELECT\n" +
                        "  \"count\"(*)\n" +
                        ", \"checksum\"(\"bigint\") \"bigint_checksum\"\n" +
                        ", \"checksum\"(\"varchar\") \"varchar_checksum\"\n" +
                        ", \"sum\"(\"double\") FILTER (WHERE \"is_finite\"(\"double\")) \"double_sum\"\n" +
                        ", \"count\"(\"double\") FILTER (WHERE \"is_nan\"(\"double\")) \"double_nan_count\"\n" +
                        ", \"count\"(\"double\") FILTER (WHERE (\"double\" = \"infinity\"())) \"double_pos_inf_count\"\n" +
                        ", \"count\"(\"double\") FILTER (WHERE (\"double\" = -\"infinity\"())) \"double_neg_inf_count\"\n" +
                        ", \"sum\"(CAST(\"real\" AS double)) FILTER (WHERE \"is_finite\"(\"real\")) \"real_sum\"\n" +
                        ", \"count\"(\"real\") FILTER (WHERE \"is_nan\"(\"real\")) \"real_nan_count\"\n" +
                        ", \"count\"(\"real\") FILTER (WHERE (\"real\" = \"infinity\"())) \"real_pos_inf_count\"\n" +
                        ", \"count\"(\"real\") FILTER (WHERE (\"real\" = -\"infinity\"())) \"real_neg_inf_count\"\n" +
                        ", \"checksum\"(\"array_sort\"(\"int_array\")) int_array_checksum\n" +
                        ", COALESCE(\"checksum\"(TRY(\"array_sort\"(\"row_array\"))), \"checksum\"(\"row_array\")) \"row_array_checksum\"" +
                        ", \"checksum\"(\"map_array\") \"map_array_checksum\"\n" +
                        "FROM\n" +
                        "  test:di",
                PARSING_OPTIONS);
        assertEquals(checksumQuery, expectedChecksumQuery, "Actual: " + formatSql(checksumQuery, Optional.empty()));
    }

    @Test
    public void testSimple()
    {
        List<Column> columns = ImmutableList.of(BIGINT_COLUMN, VARCHAR_COLUMN);
        ChecksumResult controlChecksum = new ChecksumResult(
                5,
                ImmutableMap.<String, Object>builder()
                        .put("bigint_checksum", new SqlVarbinary(new byte[] {0xa}))
                        .put("varchar_checksum", new SqlVarbinary(new byte[] {0xb}))
                        .build());

        // Matched
        assertTrue(checksumValidator.getMismatchedColumns(columns, controlChecksum, controlChecksum).isEmpty());

        // Mismatched
        ChecksumResult testChecksum = new ChecksumResult(
                5,
                ImmutableMap.<String, Object>builder()
                        .put("bigint_checksum", new SqlVarbinary(new byte[] {0x1a}))
                        .put("varchar_checksum", new SqlVarbinary(new byte[] {0x1b}))
                        .build());
        assertEquals(
                checksumValidator.getMismatchedColumns(columns, controlChecksum, testChecksum),
                ImmutableMap.builder()
                        .put(BIGINT_COLUMN, new ColumnMatchResult(false, "control(checksum: 0a) test(checksum: 1a)"))
                        .put(VARCHAR_COLUMN, new ColumnMatchResult(false, "control(checksum: 0b) test(checksum: 1b)"))
                        .build());
    }

    @Test
    public void testFloatingPoint()
    {
        List<Column> columns = ImmutableList.of(DOUBLE_COLUMN, REAL_COLUMN);
        ChecksumResult controlChecksum = new ChecksumResult(
                5,
                ImmutableMap.<String, Object>builder()
                        .putAll(FLOATING_POINT_COUNTS)
                        .put("double_sum", 1.0)
                        .put("real_sum", 1.0)
                        .build());

        // Matched
        ChecksumResult testChecksum = new ChecksumResult(
                5,
                ImmutableMap.<String, Object>builder()
                        .putAll(FLOATING_POINT_COUNTS)
                        .put("double_sum", 1 + RELATIVE_ERROR_MARGIN)
                        .put("real_sum", 1 - RELATIVE_ERROR_MARGIN + RELATIVE_ERROR_MARGIN * RELATIVE_ERROR_MARGIN)
                        .build());
        assertTrue(checksumValidator.getMismatchedColumns(columns, controlChecksum, testChecksum).isEmpty());

        // Mismatched
        testChecksum = new ChecksumResult(
                5,
                ImmutableMap.<String, Object>builder()
                        .put("double_sum", 1.0)
                        .put("double_nan_count", 0L)
                        .put("double_pos_inf_count", 3L)
                        .put("double_neg_inf_count", 4L)
                        .put("real_sum", 1.0)
                        .put("real_nan_count", 2L)
                        .put("real_pos_inf_count", 0L)
                        .put("real_neg_inf_count", 4L)
                        .build());
        assertEquals(
                checksumValidator.getMismatchedColumns(columns, controlChecksum, testChecksum),
                ImmutableMap.builder()
                        .put(DOUBLE_COLUMN, new ColumnMatchResult(false, "control(NaN: 2, +infinity: 3, -infinity: 4) test(NaN: 0, +infinity: 3, -infinity: 4)"))
                        .put(REAL_COLUMN, new ColumnMatchResult(false, "control(NaN: 2, +infinity: 3, -infinity: 4) test(NaN: 2, +infinity: 0, -infinity: 4)"))
                        .build());

        testChecksum = new ChecksumResult(
                5,
                ImmutableMap.<String, Object>builder()
                        .put("double_sum", 1.0)
                        .put("double_nan_count", 2L)
                        .put("double_pos_inf_count", 3L)
                        .put("double_neg_inf_count", 0L)
                        .put("real_sum", 1 - RELATIVE_ERROR_MARGIN)
                        .put("real_nan_count", 2L)
                        .put("real_pos_inf_count", 3L)
                        .put("real_neg_inf_count", 4L)
                        .build());
        assertEquals(
                checksumValidator.getMismatchedColumns(columns, controlChecksum, testChecksum),
                ImmutableMap.builder()
                        .put(DOUBLE_COLUMN, new ColumnMatchResult(false, "control(NaN: 2, +infinity: 3, -infinity: 4) test(NaN: 2, +infinity: 3, -infinity: 0)"))
                        .put(REAL_COLUMN, new ColumnMatchResult(false, "control(sum: 1.0) test(sum: 0.9999) relative error: 1.0000500025000149E-4"))
                        .build());
    }

    @Test
    public void testFloatingPointWithNull()
    {
        List<Column> columns = ImmutableList.of(DOUBLE_COLUMN, REAL_COLUMN);
        Map<String, Object> controlResult = new HashMap<>(FLOATING_POINT_COUNTS);
        controlResult.put("double_sum", 1.0);
        controlResult.put("real_sum", null);
        ChecksumResult controlChecksum = new ChecksumResult(5, controlResult);

        Map<String, Object> testResult = new HashMap<>(FLOATING_POINT_COUNTS);
        testResult.put("double_sum", null);
        testResult.put("real_sum", 1.0);
        ChecksumResult testChecksum = new ChecksumResult(5, testResult);

        assertEquals(
                checksumValidator.getMismatchedColumns(columns, controlChecksum, testChecksum),
                ImmutableMap.builder()
                        .put(DOUBLE_COLUMN, new ColumnMatchResult(false, "control(sum: 1.0) test(sum: null)"))
                        .put(REAL_COLUMN, new ColumnMatchResult(false, "control(sum: null) test(sum: 1.0)"))
                        .build());
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
                        .put("double_sum", 0.0)
                        .put("real_sum", 4.9e-12)
                        .build());
        ChecksumResult testChecksum = new ChecksumResult(
                5,
                ImmutableMap.<String, Object>builder()
                        .putAll(FLOATING_POINT_COUNTS)
                        .put("double_sum", 4.9e-12)
                        .put("real_sum", 0.0)
                        .build());
        assertTrue(checksumValidator.getMismatchedColumns(columns, controlChecksum, testChecksum).isEmpty());

        // Mismatched
        controlChecksum = new ChecksumResult(
                5,
                ImmutableMap.<String, Object>builder()
                        .putAll(FLOATING_POINT_COUNTS)
                        .put("double_sum", 0.0)
                        .put("real_sum", 5.1e-12)
                        .build());
        testChecksum = new ChecksumResult(
                5,
                ImmutableMap.<String, Object>builder()
                        .putAll(FLOATING_POINT_COUNTS)
                        .put("double_sum", 5.1e-12)
                        .put("real_sum", 0.0)
                        .build());
        assertEquals(
                checksumValidator.getMismatchedColumns(columns, controlChecksum, testChecksum),
                ImmutableMap.builder()
                        .put(DOUBLE_COLUMN, new ColumnMatchResult(false, "control(mean: 0.0) test(mean: 1.0199999999999999E-12) difference: 1.0199999999999999E-12"))
                        .put(REAL_COLUMN, new ColumnMatchResult(false, "control(mean: 1.0199999999999999E-12) test(mean: 0.0) difference: 1.0199999999999999E-12"))
                        .build());
    }

    @Test
    public void testArray()
    {
        List<Column> columns = ImmutableList.of(INT_ARRAY_COLUMN, MAP_ARRAY_COLUMN);
        ChecksumResult controlChecksum = new ChecksumResult(
                5,
                ImmutableMap.<String, Object>builder()
                        .put("int_array_checksum", new SqlVarbinary(new byte[] {0xa}))
                        .put("map_array_checksum", new SqlVarbinary(new byte[] {0xb}))
                        .build());

        // Matched
        assertTrue(checksumValidator.getMismatchedColumns(columns, controlChecksum, controlChecksum).isEmpty());

        // Mismatched
        ChecksumResult testChecksum = new ChecksumResult(
                5,
                ImmutableMap.<String, Object>builder()
                        .put("int_array_checksum", new SqlVarbinary(new byte[] {0x1a}))
                        .put("map_array_checksum", new SqlVarbinary(new byte[] {0x1b}))
                        .build());
        assertEquals(
                checksumValidator.getMismatchedColumns(columns, controlChecksum, testChecksum),
                ImmutableMap.builder()
                        .put(INT_ARRAY_COLUMN, new ColumnMatchResult(false, "control(checksum: 0a) test(checksum: 1a)"))
                        .put(MAP_ARRAY_COLUMN, new ColumnMatchResult(false, "control(checksum: 0b) test(checksum: 1b)"))
                        .build());
    }
}
