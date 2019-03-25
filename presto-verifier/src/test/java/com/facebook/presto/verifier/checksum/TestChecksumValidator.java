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
import com.facebook.presto.sql.parser.ParsingOptions;
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

import java.util.List;
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
import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE;
import static com.facebook.presto.verifier.framework.Column.Category.FLOATING_POINT;
import static com.facebook.presto.verifier.framework.Column.Category.ORDERABLE_ARRAY;
import static com.facebook.presto.verifier.framework.Column.Category.SIMPLE;
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
    private static final Column MAP_ARRAY_COLUMN = new Column("map_array", SIMPLE, typeRegistry.getType(parseTypeSignature("array(map(int,varchar))")));

    private static final List<Column> COLUMNS = ImmutableList.of(
            BIGINT_COLUMN,
            VARCHAR_COLUMN,
            DOUBLE_COLUMN,
            REAL_COLUMN,
            INT_ARRAY_COLUMN,
            MAP_ARRAY_COLUMN);
    private static final ChecksumResult CONTROL_CHECKSUM = new ChecksumResult(
            5,
            ImmutableMap.<String, Object>builder()
                    .put("bigint_checksum", new SqlVarbinary(new byte[] {0xa}))
                    .put("varchar_checksum", new SqlVarbinary(new byte[] {0xb}))
                    .put("double_sum", 1.0)
                    .put("double_nan_count", 52L)
                    .put("double_pos_inf_count", 53L)
                    .put("double_neg_inf_count", 54L)
                    .put("real_sum", 1.0)
                    .put("real_nan_count", 55L)
                    .put("real_pos_inf_count", 56L)
                    .put("real_neg_inf_count", 57L)
                    .put("int_array_sorted_checksum", new SqlVarbinary(new byte[] {0xd}))
                    .put("map_array_checksum", new SqlVarbinary(new byte[] {0xe}))
                    .build());
    private static final double RELATIVE_ERROR_MARGIN = 1e-4;
    private static final SqlParser sqlParser = new SqlParser(new SqlParserOptions().allowIdentifierSymbol(COLON, AT_SIGN));

    private final ChecksumValidator checksumValidator = new ChecksumValidator(
            new SimpleColumnValidator(),
            new FloatingPointColumnValidator(new VerifierConfig().setRelativeErrorMargin(RELATIVE_ERROR_MARGIN)),
            new OrderableArrayColumnValidator());

    @Test
    public void testChecksumQuery()
    {
        Query checksumQuery = checksumValidator.generateChecksumQuery(QualifiedName.of("test:di"), COLUMNS);
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
                        ", \"checksum\"(\"array_sort\"(\"int_array\")) int_array_sorted_checksum\n" +
                        ", \"checksum\"(\"map_array\") \"map_array_checksum\"\n" +
                        "FROM\n" +
                        "  test:di",
                new ParsingOptions(AS_DOUBLE));
        assertEquals(checksumQuery, expectedChecksumQuery, "Actual: " + formatSql(checksumQuery, Optional.empty()));
    }

    @Test
    public void testColumnsMatched()
    {
        ChecksumResult testChecksum = new ChecksumResult(
                5,
                ImmutableMap.<String, Object>builder()
                        .put("bigint_checksum", new SqlVarbinary(new byte[] {0xa}))
                        .put("varchar_checksum", new SqlVarbinary(new byte[] {0xb}))
                        .put("double_sum", 1 + RELATIVE_ERROR_MARGIN)
                        .put("double_nan_count", 52L)
                        .put("double_pos_inf_count", 53L)
                        .put("double_neg_inf_count", 54L)
                        .put("real_sum", 1 - RELATIVE_ERROR_MARGIN + RELATIVE_ERROR_MARGIN * RELATIVE_ERROR_MARGIN)
                        .put("real_nan_count", 55L)
                        .put("real_pos_inf_count", 56L)
                        .put("real_neg_inf_count", 57L)
                        .put("int_array_sorted_checksum", new SqlVarbinary(new byte[] {0xd}))
                        .put("map_array_checksum", new SqlVarbinary(new byte[] {0xe}))
                        .build());
        assertTrue(checksumValidator.getMismatchedColumns(COLUMNS, CONTROL_CHECKSUM, testChecksum).isEmpty());
    }

    @Test
    public void testColumnsMismatched()
    {
        ChecksumResult testChecksum = new ChecksumResult(
                5,
                ImmutableMap.<String, Object>builder()
                        .put("bigint_checksum", new SqlVarbinary(new byte[] {0x1a}))
                        .put("varchar_checksum", new SqlVarbinary(new byte[] {0x1b}))
                        .put("double_sum", 1.0)
                        .put("double_nan_count", 0L)
                        .put("double_pos_inf_count", 53L)
                        .put("double_neg_inf_count", 54L)
                        .put("real_sum", 1 - RELATIVE_ERROR_MARGIN)
                        .put("real_nan_count", 55L)
                        .put("real_pos_inf_count", 56L)
                        .put("real_neg_inf_count", 57L)
                        .put("int_array_sorted_checksum", new SqlVarbinary(new byte[] {0x1d}))
                        .put("map_array_checksum", new SqlVarbinary(new byte[] {0x1e}))
                        .build());
        assertEquals(
                checksumValidator.getMismatchedColumns(COLUMNS, CONTROL_CHECKSUM, testChecksum),
                ImmutableMap.builder()
                        .put(BIGINT_COLUMN, new ColumnMatchResult(false, "control(checksum: 0a) test(checksum: 1a)"))
                        .put(VARCHAR_COLUMN, new ColumnMatchResult(false, "control(checksum: 0b) test(checksum: 1b)"))
                        .put(DOUBLE_COLUMN, new ColumnMatchResult(false, "control(NaN: 52, +infinity: 0, -infinity: 53) test(NaN: 53, +infinity: 54, -infinity: 0)"))
                        .put(REAL_COLUMN, new ColumnMatchResult(false, "control(sum: 1.0) test(sum: 0.9999) relative error: 1.0000500025000149E-4"))
                        .put(INT_ARRAY_COLUMN, new ColumnMatchResult(false, "control(sorted_checksum: 0d) test(sorted_checksum: 1d)"))
                        .put(MAP_ARRAY_COLUMN, new ColumnMatchResult(false, "control(checksum: 0e) test(checksum: 1e)"))
                        .build());
    }
}
