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
package com.facebook.presto.delta;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DateTimeEncoding;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import io.airlift.slice.Slices;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Predicate;
import org.testng.annotations.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestScanFilter
{
    @Test
    public void testWithDomainIsAll()
    {
        Domain domain = Domain.all(IntegerType.INTEGER);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "int_value",
                TypeSignature.parseTypeSignature(StandardTypes.INTEGER),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);
        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertFalse(scanFilter.getPredicate().isPresent());
    }

    @Test
    public void testWithDomainIsNone()
    {
        Domain domain = Domain.none(IntegerType.INTEGER);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "int_value",
                TypeSignature.parseTypeSignature(StandardTypes.INTEGER),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);
        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "ALWAYS_FALSE(column(`int_value`))");
    }

    @Test
    public void testWithDomainIntegerEquals()
    {
        Type type = IntegerType.INTEGER;
        Domain domain = Domain.create(ValueSet.ofRanges(Range.equal(type, 1L)), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "int_value",
                TypeSignature.parseTypeSignature(StandardTypes.INTEGER),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`int_value`) = 1)");
    }

    @Test
    public void testWithDomainIntegerGreaterThan()
    {
        Type type = IntegerType.INTEGER;
        Domain domain = Domain.create(ValueSet.ofRanges(Range.greaterThan(type, 1L)), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "int_value",
                TypeSignature.parseTypeSignature(StandardTypes.INTEGER),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`int_value`) > 1)");
    }

    @Test
    public void testWithDomainIntegerGreaterOrEqualThan()
    {
        Type type = IntegerType.INTEGER;
        Domain domain = Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(type, 1L)), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "int_value",
                TypeSignature.parseTypeSignature(StandardTypes.INTEGER),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`int_value`) >= 1)");
    }

    @Test
    public void testWithDomainIntegerLessThan()
    {
        Type type = IntegerType.INTEGER;
        Domain domain = Domain.create(ValueSet.ofRanges(Range.lessThan(type, 1L)), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "int_value",
                TypeSignature.parseTypeSignature(StandardTypes.INTEGER),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`int_value`) < 1)");
    }

    @Test
    public void testWithDomainIntegerLessOrEqualThan()
    {
        Type type = IntegerType.INTEGER;
        Domain domain = Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(type, 1L)), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "int_value",
                TypeSignature.parseTypeSignature(StandardTypes.INTEGER),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`int_value`) <= 1)");
    }

    @Test
    public void testWithDomainIntegerIsNull()
    {
        Domain domain = Domain.onlyNull(IntegerType.INTEGER);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "int_value",
                TypeSignature.parseTypeSignature(StandardTypes.INTEGER),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "IS_NULL(column(`int_value`))");
    }

    @Test
    public void testWithDomainIntegerIsNotNull()
    {
        Domain domain = Domain.notNull(IntegerType.INTEGER);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "int_value",
                TypeSignature.parseTypeSignature(StandardTypes.INTEGER),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "IS_NOT_NULL(column(`int_value`))");
    }

    @Test
    public void testWithDomainBooleanEquals()
    {
        Type type = BooleanType.BOOLEAN;
        Domain domain = Domain.create(ValueSet.ofRanges(Range.equal(type, true)), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "bool_value",
                TypeSignature.parseTypeSignature(StandardTypes.BOOLEAN),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`bool_value`) = true)");
    }

    @Test
    public void testWithDomainBooleanIsNull()
    {
        Domain domain = Domain.onlyNull(BooleanType.BOOLEAN);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "bool_value",
                TypeSignature.parseTypeSignature(StandardTypes.BOOLEAN),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "IS_NULL(column(`bool_value`))");
    }

    @Test
    public void testWithDomainBooleanIsNotNull()
    {
        Domain domain = Domain.notNull(BooleanType.BOOLEAN);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "bool_value",
                TypeSignature.parseTypeSignature(StandardTypes.BOOLEAN),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "IS_NOT_NULL(column(`bool_value`))");
    }

    @Test
    public void testWithDomainTinyintEquals()
    {
        Type type = TinyintType.TINYINT;
        Domain domain = Domain.create(ValueSet.ofRanges(Range.equal(type, 1L)), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "tinyint_value",
                TypeSignature.parseTypeSignature(StandardTypes.TINYINT),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`tinyint_value`) = 1)");
    }

    @Test
    public void testWithDomainTinyintGreaterThan()
    {
        Type type = TinyintType.TINYINT;
        Domain domain = Domain.create(ValueSet.ofRanges(Range.greaterThan(type, 1L)), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "tinyint_value",
                TypeSignature.parseTypeSignature(StandardTypes.TINYINT),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`tinyint_value`) > 1)");
    }

    @Test
    public void testWithDomainTinyintGreaterOrEqualsThan()
    {
        Type type = TinyintType.TINYINT;
        Domain domain = Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(type, 1L)), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "tinyint_value",
                TypeSignature.parseTypeSignature(StandardTypes.TINYINT),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`tinyint_value`) >= 1)");
    }

    @Test
    public void testWithDomainTinyintLesserThan()
    {
        Type type = TinyintType.TINYINT;
        Domain domain = Domain.create(ValueSet.ofRanges(Range.lessThan(type, 1L)), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "tinyint_value",
                TypeSignature.parseTypeSignature(StandardTypes.TINYINT),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`tinyint_value`) < 1)");
    }

    @Test
    public void testWithDomainTinyintLesserOrEqualsThan()
    {
        Type type = TinyintType.TINYINT;
        Domain domain = Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(type, 1L)), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "tinyint_value",
                TypeSignature.parseTypeSignature(StandardTypes.TINYINT),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`tinyint_value`) <= 1)");
    }

    @Test
    public void testWithDomainSmallintEquals()
    {
        Type type = SmallintType.SMALLINT;
        Domain domain = Domain.create(ValueSet.ofRanges(Range.equal(type, 1L)), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "smallint_value",
                TypeSignature.parseTypeSignature(StandardTypes.SMALLINT),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`smallint_value`) = 1)");
    }

    @Test
    public void testWithDomainSmallintGreaterThan()
    {
        Type type = SmallintType.SMALLINT;
        Domain domain = Domain.create(ValueSet.ofRanges(Range.greaterThan(type, 1L)), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "smallint_value",
                TypeSignature.parseTypeSignature(StandardTypes.SMALLINT),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`smallint_value`) > 1)");
    }

    @Test
    public void testWithDomainSmallintGreaterOrEqualThan()
    {
        Type type = SmallintType.SMALLINT;
        Domain domain = Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(type, 1L)), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "smallint_value",
                TypeSignature.parseTypeSignature(StandardTypes.SMALLINT),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`smallint_value`) >= 1)");
    }

    @Test
    public void testWithDomainSmallintLessThan()
    {
        Type type = SmallintType.SMALLINT;
        Domain domain = Domain.create(ValueSet.ofRanges(Range.lessThan(type, 1L)), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "smallint_value",
                TypeSignature.parseTypeSignature(StandardTypes.SMALLINT),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`smallint_value`) < 1)");
    }

    @Test
    public void testWithDomainSmallintLessOrEqualThan()
    {
        Type type = SmallintType.SMALLINT;
        Domain domain = Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(type, 1L)), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "smallint_value",
                TypeSignature.parseTypeSignature(StandardTypes.SMALLINT),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`smallint_value`) <= 1)");
    }

    @Test
    public void testWithDomainBigintEquals()
    {
        Type type = BigintType.BIGINT;
        Domain domain = Domain.create(ValueSet.ofRanges(Range.equal(type, 1L)), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "bigint_value",
                TypeSignature.parseTypeSignature(StandardTypes.BIGINT),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`bigint_value`) = 1)");
    }

    @Test
    public void testWithDomainBigintGreaterThan()
    {
        Type type = BigintType.BIGINT;
        Domain domain = Domain.create(ValueSet.ofRanges(Range.greaterThan(type, 1L)), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "bigint_value",
                TypeSignature.parseTypeSignature(StandardTypes.BIGINT),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`bigint_value`) > 1)");
    }

    @Test
    public void testWithDomainBigintGreaterOrEqualThan()
    {
        Type type = BigintType.BIGINT;
        Domain domain = Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(type, 1L)), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "bigint_value",
                TypeSignature.parseTypeSignature(StandardTypes.BIGINT),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`bigint_value`) >= 1)");
    }

    @Test
    public void testWithDomainBigintLesserThan()
    {
        Type type = BigintType.BIGINT;
        Domain domain = Domain.create(ValueSet.ofRanges(Range.lessThan(type, 1L)), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "bigint_value",
                TypeSignature.parseTypeSignature(StandardTypes.BIGINT),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`bigint_value`) < 1)");
    }

    @Test
    public void testWithDomainBigintLesserOrEqualThan()
    {
        Type type = BigintType.BIGINT;
        Domain domain = Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(type, 1L)), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "bigint_value",
                TypeSignature.parseTypeSignature(StandardTypes.BIGINT),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`bigint_value`) <= 1)");
    }

    @Test
    public void testWithDomainRealEquals()
    {
        Type type = RealType.REAL;
        Domain domain = Domain.create(ValueSet.ofRanges(Range.equal(type, Integer.valueOf(Float.floatToRawIntBits(1.5f)).longValue())), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "real_value",
                TypeSignature.parseTypeSignature(StandardTypes.REAL),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`real_value`) = 1.5)");
    }

    @Test
    public void testWithDomainRealGreaterThan()
    {
        Type type = RealType.REAL;
        Domain domain = Domain.create(ValueSet.ofRanges(Range.greaterThan(type, Integer.valueOf(Float.floatToRawIntBits(1.5f)).longValue())), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "real_value",
                TypeSignature.parseTypeSignature(StandardTypes.REAL),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`real_value`) > 1.5)");
    }

    @Test
    public void testWithDomainRealGreaterThanOrEquals()
    {
        Type type = RealType.REAL;
        Domain domain = Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(type, Integer.valueOf(Float.floatToRawIntBits(1.5f)).longValue())), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "real_value",
                TypeSignature.parseTypeSignature(StandardTypes.REAL),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`real_value`) >= 1.5)");
    }

    @Test
    public void testWithDomainRealLessThan()
    {
        Type type = RealType.REAL;
        Domain domain = Domain.create(ValueSet.ofRanges(Range.lessThan(type, Integer.valueOf(Float.floatToRawIntBits(1.5f)).longValue())), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "real_value",
                TypeSignature.parseTypeSignature(StandardTypes.REAL),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`real_value`) < 1.5)");
    }

    @Test
    public void testWithDomainRealLessOrEqualThan()
    {
        Type type = RealType.REAL;
        Domain domain = Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(type, Integer.valueOf(Float.floatToRawIntBits(1.5f)).longValue())), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "real_value",
                TypeSignature.parseTypeSignature(StandardTypes.REAL),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`real_value`) <= 1.5)");
    }

    @Test
    public void testWithDomainDoubleEquals()
    {
        Type type = DoubleType.DOUBLE;
        Domain domain = Domain.create(ValueSet.ofRanges(Range.equal(type, 1.5)), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "double_value",
                TypeSignature.parseTypeSignature(StandardTypes.DOUBLE),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`double_value`) = 1.5)");
    }

    @Test
    public void testWithDomainDoubleGreaterThan()
    {
        Type type = DoubleType.DOUBLE;
        Domain domain = Domain.create(ValueSet.ofRanges(Range.greaterThan(type, 1.5)), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "double_value",
                TypeSignature.parseTypeSignature(StandardTypes.DOUBLE),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`double_value`) > 1.5)");
    }

    @Test
    public void testWithDomainDoubleGreaterOrEqualThan()
    {
        Type type = DoubleType.DOUBLE;
        Domain domain = Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(type, 1.5)), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "double_value",
                TypeSignature.parseTypeSignature(StandardTypes.DOUBLE),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`double_value`) >= 1.5)");
    }

    @Test
    public void testWithDomainDoubleLessThan()
    {
        Type type = DoubleType.DOUBLE;
        Domain domain = Domain.create(ValueSet.ofRanges(Range.lessThan(type, 1.5)), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "double_value",
                TypeSignature.parseTypeSignature(StandardTypes.DOUBLE),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`double_value`) < 1.5)");
    }

    @Test
    public void testWithDomainDoubleLessOrEqualThan()
    {
        Type type = DoubleType.DOUBLE;
        Domain domain = Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(type, 1.5)), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "double_value",
                TypeSignature.parseTypeSignature(StandardTypes.DOUBLE),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`double_value`) <= 1.5)");
    }

    @Test
    public void testWithDomainDecimalEquals()
    {
        Domain domain = Domain.create(ValueSet.ofRanges(Range.equal(DecimalType.createDecimalType(38, 18),
                Decimals.encodeScaledValue(new BigDecimal("1.5"), 18))), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "double_value",
                TypeSignature.parseTypeSignature("decimal(38,18)"),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`double_value`) = 1.500000000000000000)");
    }

    @Test
    public void testWithDomainVarcharEquals()
    {
        Type type = VarcharType.VARCHAR;
        Domain domain = Domain.create(ValueSet.ofRanges(Range.equal(type, Slices.utf8Slice("test"))), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "varchar_value",
                TypeSignature.parseTypeSignature(StandardTypes.VARCHAR),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`varchar_value`) = test)");
    }

    @Test
    public void testWithDomainVarcharEqualsLongTextWithSpacesAndSpecialCharacters()
    {
        Type type = VarcharType.VARCHAR;
        Domain domain = Domain.create(ValueSet.ofRanges(Range.equal(type, Slices.utf8Slice("test with spaces and |@#% 'a'aápapaááá characters"))), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "varchar_value",
                TypeSignature.parseTypeSignature(StandardTypes.VARCHAR),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`varchar_value`) = test with spaces and |@#% 'a'aápapaááá characters)");
    }

    @Test
    public void testWithDomainVarcharLessThan()
    {
        Type type = VarcharType.VARCHAR;
        Domain domain = Domain.create(ValueSet.ofRanges(Range.lessThan(type, Slices.utf8Slice("test"))), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "varchar_value",
                TypeSignature.parseTypeSignature(StandardTypes.VARCHAR),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`varchar_value`) < test)");
    }

    @Test
    public void testWithDomainVarcharLessThanOrEquals()
    {
        Type type = VarcharType.VARCHAR;
        Domain domain = Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(type, Slices.utf8Slice("test"))), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "varchar_value",
                TypeSignature.parseTypeSignature(StandardTypes.VARCHAR),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`varchar_value`) <= test)");
    }

    @Test
    public void testWithDomainVarcharGreaterThan()
    {
        Type type = VarcharType.VARCHAR;
        Domain domain = Domain.create(ValueSet.ofRanges(Range.greaterThan(type, Slices.utf8Slice("test"))), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "varchar_value",
                TypeSignature.parseTypeSignature(StandardTypes.VARCHAR),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`varchar_value`) > test)");
    }

    @Test
    public void testWithDomainVarcharGreaterThanOrEquals()
    {
        Type type = VarcharType.VARCHAR;
        Domain domain = Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(type, Slices.utf8Slice("test"))), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "varchar_value",
                TypeSignature.parseTypeSignature(StandardTypes.VARCHAR),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`varchar_value`) >= test)");
    }

    @Test
    public void testWithDomainVarbinaryEquals()
    {
        Type type = VarbinaryType.VARBINARY;
        Domain domain = Domain.create(ValueSet.ofRanges(Range.equal(type, Slices.wrappedBuffer(new byte[] {1, 2, 3}))), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "varbinary_value",
                TypeSignature.parseTypeSignature(StandardTypes.VARBINARY),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertTrue(scanFilter.getPredicate().get().toString().startsWith("(column(`varbinary_value`) = [B@"));
    }

    @Test
    public void testWithDomainVarbinaryIsNull()
    {
        Domain domain = Domain.onlyNull(VarbinaryType.VARBINARY);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "varbinary_value",
                TypeSignature.parseTypeSignature(StandardTypes.VARBINARY),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "IS_NULL(column(`varbinary_value`))");
    }

    @Test
    public void testWithDomainVarbinaryIsNotNull()
    {
        Domain domain = Domain.notNull(VarbinaryType.VARBINARY);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "varbinary_value",
                TypeSignature.parseTypeSignature(StandardTypes.VARBINARY),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "IS_NOT_NULL(column(`varbinary_value`))");
    }

    @Test
    public void testWithDomainDateEquals()
    {
        Type type = DateType.DATE;
        LocalDate date = LocalDate.of(2026, 9, 18);
        long epochDay = date.toEpochDay();
        assertEquals(epochDay, 20714L);
        Domain domain = Domain.create(ValueSet.ofRanges(Range.equal(type, date.toEpochDay())), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "date_value",
                TypeSignature.parseTypeSignature(StandardTypes.DATE),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`date_value`) = 20714)");
    }

    @Test
    public void testWithDomainDateLessThan()
    {
        Type type = DateType.DATE;
        LocalDate date = LocalDate.of(2026, 9, 19);
        long epochDay = date.toEpochDay();
        assertEquals(epochDay, 20715L);

        Domain domain = Domain.create(ValueSet.ofRanges(Range.lessThan(type, date.toEpochDay())), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "date_value",
                TypeSignature.parseTypeSignature(StandardTypes.DATE),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`date_value`) < 20715)");
    }

    @Test
    public void testWithDomainDateLessOrEqualThan()
    {
        Type type = DateType.DATE;
        LocalDate date = LocalDate.of(2026, 9, 19);
        long epochDay = date.toEpochDay();
        assertEquals(epochDay, 20715L);

        Domain domain = Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(type, date.toEpochDay())), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "date_value",
                TypeSignature.parseTypeSignature(StandardTypes.DATE),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`date_value`) <= 20715)");
    }

    @Test
    public void testWithDomainDateGreaterThan()
    {
        Type type = DateType.DATE;
        LocalDate date = LocalDate.of(2026, 9, 19);
        long epochDay = date.toEpochDay();
        assertEquals(epochDay, 20715L);

        Domain domain = Domain.create(ValueSet.ofRanges(Range.greaterThan(type, date.toEpochDay())), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "date_value",
                TypeSignature.parseTypeSignature(StandardTypes.DATE),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`date_value`) > 20715)");
    }

    @Test
    public void testWithDomainDateGreaterOrEqualThan()
    {
        Type type = DateType.DATE;
        LocalDate date = LocalDate.of(2026, 9, 19);
        long epochDay = date.toEpochDay();
        assertEquals(epochDay, 20715L);

        Domain domain = Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(type, date.toEpochDay())), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "date_value",
                TypeSignature.parseTypeSignature(StandardTypes.DATE),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`date_value`) >= 20715)");
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testWithDomainTimestampEquals()
    {
        long millisUtc = 392934375000L;
        Type type = TimestampType.TIMESTAMP;
        Domain domain = Domain.create(ValueSet.ofRanges(
                Range.equal(type, millisUtc)), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "timestamp_value",
                TypeSignature.parseTypeSignature(StandardTypes.TIMESTAMP),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        // equals millisUTC * 1000 as delta kernel expects microseconds
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`timestamp_value`) = 392934375000000)");
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testWithDomainTimestampLessThan()
    {
        long millisUtc = 392934375000L;
        Type type = TimestampType.TIMESTAMP;
        Domain domain = Domain.create(ValueSet.ofRanges(
                Range.lessThan(type, millisUtc)), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "timestamp_value",
                TypeSignature.parseTypeSignature(StandardTypes.TIMESTAMP),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        // equals millisUTC * 1000 as delta kernel expects microseconds
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`timestamp_value`) < 392934375000000)");
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testWithDomainTimestampLessThanOrEquals()
    {
        long millisUtc = 392934375000L;
        Type type = TimestampType.TIMESTAMP;
        Domain domain = Domain.create(ValueSet.ofRanges(
                Range.lessThanOrEqual(type, millisUtc)), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "timestamp_value",
                TypeSignature.parseTypeSignature(StandardTypes.TIMESTAMP),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        // equals millisUTC * 1000 as delta kernel expects microseconds
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`timestamp_value`) <= 392934375000000)");
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testWithDomainTimestampGreaterThan()
    {
        long millisUtc = 392934375000L;
        Type type = TimestampType.TIMESTAMP;
        Domain domain = Domain.create(ValueSet.ofRanges(
                Range.greaterThan(type, millisUtc)), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "timestamp_value",
                TypeSignature.parseTypeSignature(StandardTypes.TIMESTAMP),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        // equals millisUTC * 1000 as delta kernel expects microseconds
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`timestamp_value`) > 392934375000000)");
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testWithDomainTimestampGreaterThanOrEquals()
    {
        long millisUtc = 392934375000L;
        Type type = TimestampType.TIMESTAMP;
        Domain domain = Domain.create(ValueSet.ofRanges(
                Range.greaterThanOrEqual(type, millisUtc)), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "timestamp_value",
                TypeSignature.parseTypeSignature(StandardTypes.TIMESTAMP),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        // equals millisUTC * 1000 as delta kernel expects microseconds
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`timestamp_value`) >= 392934375000000)");
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testWithDomainTimestampWithTimezoneEquals()
    {
        long millisUtc = 392934375000L;
        Type type = TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
        Domain domain = Domain.create(ValueSet.ofRanges(Range.equal(type,
                DateTimeEncoding.packDateTimeWithZone(millisUtc, TimeZoneKey.UTC_KEY))), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "timestamp_value",
                TypeSignature.parseTypeSignature(StandardTypes.TIMESTAMP_WITH_TIME_ZONE),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`timestamp_value`) = 392934375000000)");
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testWithDomainTimestampWithTimezoneLessThan()
    {
        long millisUtc = 392934375000L;
        Type type = TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
        Domain domain = Domain.create(ValueSet.ofRanges(Range.lessThan(type,
                DateTimeEncoding.packDateTimeWithZone(millisUtc, TimeZoneKey.UTC_KEY))), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "timestamp_value",
                TypeSignature.parseTypeSignature(StandardTypes.TIMESTAMP_WITH_TIME_ZONE),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`timestamp_value`) < 392934375000000)");
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testWithDomainTimestampWithTimezoneLessOrEqualThan()
    {
        long millisUtc = 392934375000L;
        Type type = TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
        Domain domain = Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(type,
                DateTimeEncoding.packDateTimeWithZone(millisUtc, TimeZoneKey.UTC_KEY))), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "timestamp_value",
                TypeSignature.parseTypeSignature(StandardTypes.TIMESTAMP_WITH_TIME_ZONE),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`timestamp_value`) <= 392934375000000)");
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testWithDomainTimestampWithTimezoneGreaterThan()
    {
        long millisUtc = 392934375000L;
        Type type = TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
        Domain domain = Domain.create(ValueSet.ofRanges(Range.greaterThan(type,
                DateTimeEncoding.packDateTimeWithZone(millisUtc, TimeZoneKey.UTC_KEY))), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "timestamp_value",
                TypeSignature.parseTypeSignature(StandardTypes.TIMESTAMP_WITH_TIME_ZONE),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`timestamp_value`) > 392934375000000)");
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testWithDomainTimestampWithTimezoneGreaterOrEqualThan()
    {
        long millisUtc = 392934375000L;
        Type type = TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
        Domain domain = Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(type,
                DateTimeEncoding.packDateTimeWithZone(millisUtc, TimeZoneKey.UTC_KEY))), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "timestamp_value",
                TypeSignature.parseTypeSignature(StandardTypes.TIMESTAMP_WITH_TIME_ZONE),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`timestamp_value`) >= 392934375000000)");
    }

    @Test
    public void testConvertRangeToPredicateSingleValue() throws Exception
    {
        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        Column column = new Column("int_value");
        Range range = Range.equal(IntegerType.INTEGER, 100L);
        TypeSignature typeSignature = TypeSignature.parseTypeSignature(StandardTypes.INTEGER);

        Method method = ScanFilter.Builder.class.getDeclaredMethod("convertRangeToPredicate", Range.class, Column.class, TypeSignature.class);
        method.setAccessible(true);
        Predicate result = (Predicate) method.invoke(builder, range, column, typeSignature);

        assertEquals(result.toString(), "(column(`int_value`) = 100)");
    }

    @Test
    public void testConvertRangeToPredicateBoundedRange() throws Exception
    {
        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        Column column = new Column("int_value");
        Range range = Range.range(IntegerType.INTEGER, 10L, true, 20L, true);
        TypeSignature typeSignature = TypeSignature.parseTypeSignature(StandardTypes.INTEGER);

        Method method = ScanFilter.Builder.class.getDeclaredMethod("convertRangeToPredicate", Range.class, Column.class, TypeSignature.class);
        method.setAccessible(true);
        Predicate result = (Predicate) method.invoke(builder, range, column, typeSignature);

        assertEquals(result.toString(), "((column(`int_value`) >= 10) AND (column(`int_value`) <= 20))");
    }

    @Test
    public void testConvertRangeToPredicateUnboundedLow() throws Exception
    {
        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        Column column = new Column("int_value");
        Range range = Range.lessThanOrEqual(IntegerType.INTEGER, 50L);
        TypeSignature typeSignature = TypeSignature.parseTypeSignature(StandardTypes.INTEGER);

        Method method = ScanFilter.Builder.class.getDeclaredMethod("convertRangeToPredicate", Range.class, Column.class, TypeSignature.class);
        method.setAccessible(true);
        Predicate result = (Predicate) method.invoke(builder, range, column, typeSignature);

        assertEquals(result.toString(), "(column(`int_value`) <= 50)");
    }

    @Test
    public void testConvertRangeToPredicateUnboundedHigh() throws Exception
    {
        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        Column column = new Column("int_value");
        Range range = Range.greaterThan(IntegerType.INTEGER, 30L);
        TypeSignature typeSignature = TypeSignature.parseTypeSignature(StandardTypes.INTEGER);

        Method method = ScanFilter.Builder.class.getDeclaredMethod("convertRangeToPredicate", Range.class, Column.class, TypeSignature.class);
        method.setAccessible(true);
        Predicate result = (Predicate) method.invoke(builder, range, column, typeSignature);

        assertEquals(result.toString(), "(column(`int_value`) > 30)");
    }

    @Test
    public void testAddPredicateWithNullHandlingNullsAllowed()
    {
        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "int_value",
                TypeSignature.parseTypeSignature(StandardTypes.INTEGER),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());

        Domain domain = Domain.create(ValueSet.ofRanges(Range.equal(IntegerType.INTEGER, 5L)), true);
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "((column(`int_value`) = 5) OR IS_NULL(column(`int_value`)))");
    }

    @Test
    public void testCombinePredicatesWithOrMultiplePredicates()
    {
        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "int_value",
                TypeSignature.parseTypeSignature(StandardTypes.INTEGER),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());

        Domain domain = Domain.create(ValueSet.ofRanges(
                Range.equal(IntegerType.INTEGER, 10L),
                Range.equal(IntegerType.INTEGER, 20L),
                Range.equal(IntegerType.INTEGER, 30L)), false);
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(((column(`int_value`) = 10) OR (column(`int_value`) = 20)) OR (column(`int_value`) = 30))");
    }

    @Test
    public void testCreatePredicateEmpty()
    {
        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.build();
        assertFalse(scanFilter.getPredicate().isPresent());
    }

    @Test
    public void testCreatePredicateMultipleDomains()
    {
        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        DeltaColumnHandle columnHandle1 = new DeltaColumnHandle(
                0L,
                null,
                "int_value1",
                TypeSignature.parseTypeSignature(StandardTypes.INTEGER),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());

        DeltaColumnHandle columnHandle2 = new DeltaColumnHandle(
                1L,
                null,
                "int_value2",
                TypeSignature.parseTypeSignature(StandardTypes.INTEGER),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());

        Domain domain1 = Domain.create(ValueSet.ofRanges(Range.equal(IntegerType.INTEGER, 10L)), false);
        Domain domain2 = Domain.create(ValueSet.ofRanges(Range.equal(IntegerType.INTEGER, 20L)), false);

        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain1 = new TupleDomain.ColumnDomain<>(columnHandle1, domain1);
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain2 = new TupleDomain.ColumnDomain<>(columnHandle2, domain2);

        ScanFilter scanFilter = builder.withDomain(columnDomain1).withDomain(columnDomain2).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "((column(`int_value1`) = 10) AND (column(`int_value2`) = 20))");
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testArrayTypeUnsupported() throws Throwable
    {
        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        Method method = ScanFilter.Builder.class.getDeclaredMethod("convertToLiteral", Object.class, TypeSignature.class);
        method.setAccessible(true);
        try {
            method.invoke(builder, null, TypeSignature.parseTypeSignature(StandardTypes.ARRAY));
        }
        catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testStructTypeUnsupported() throws Throwable
    {
        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        Method method = ScanFilter.Builder.class.getDeclaredMethod("convertToLiteral", Object.class, TypeSignature.class);
        method.setAccessible(true);
        try {
            method.invoke(builder, null, TypeSignature.parseTypeSignature(StandardTypes.ROW));
        }
        catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testMapTypeUnsupported() throws Throwable
    {
        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        Method method = ScanFilter.Builder.class.getDeclaredMethod("convertToLiteral", Object.class, TypeSignature.class);
        method.setAccessible(true);
        try {
            method.invoke(builder, null, TypeSignature.parseTypeSignature(StandardTypes.MAP));
        }
        catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    @Test
    public void testWithDomainColumnMappingSpecialCharactersInColumnName()
    {
        Type type = VarcharType.VARCHAR;
        Domain domain = Domain.create(ValueSet.ofRanges(Range.equal(type, Slices.utf8Slice("eva.martinez@example.com"))), false);
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                "e-mail (address)",
                TypeSignature.parseTypeSignature(StandardTypes.VARCHAR),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);

        ScanFilter.Builder builder = new ScanFilter.Builder(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        ScanFilter scanFilter = builder.withDomain(columnDomain).build();
        assertTrue(scanFilter.getPredicate().isPresent());
        assertEquals(scanFilter.getPredicate().get().toString(), "(column(`e-mail (address)`) = eva.martinez@example.com)");
    }
}
