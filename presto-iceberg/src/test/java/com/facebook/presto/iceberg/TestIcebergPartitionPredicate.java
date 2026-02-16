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
package com.facebook.presto.iceberg;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.HivePartitionKey;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.BaseFileScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.types.Types;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.iceberg.IcebergColumnHandle.primitiveIcebergColumnHandle;
import static com.facebook.presto.iceberg.IcebergUtil.deserializePartitionValue;
import static org.apache.iceberg.expressions.Expressions.alwaysTrue;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestIcebergPartitionPredicate
{
    private static final int YEAR_COLUMN_ID = 1;
    private static final int MONTH_COLUMN_ID = 2;

    @Test
    public void testFilterConstraintIsAll()
    {
        Map<Integer, HivePartitionKey> partitionKeys = ImmutableMap.of(
                YEAR_COLUMN_ID, new HivePartitionKey("year", Optional.of("2024")));

        assertTrue(partitionMatchesPredicate(TupleDomain.all(), partitionKeys));
    }

    @Test
    public void testFilterConstraintIsNone()
    {
        Map<Integer, HivePartitionKey> partitionKeys = ImmutableMap.of(
                YEAR_COLUMN_ID, new HivePartitionKey("year", Optional.of("2024")));

        assertFalse(partitionMatchesPredicate(TupleDomain.none(), partitionKeys));
    }

    @Test
    public void testEmptyPartitionKeys()
    {
        IcebergColumnHandle yearColumn = primitiveIcebergColumnHandle(YEAR_COLUMN_ID, "year", INTEGER, Optional.empty());
        TupleDomain<IcebergColumnHandle> filterConstraint = TupleDomain.withColumnDomains(
                ImmutableMap.of(yearColumn, Domain.singleValue(INTEGER, 2024L)));

        assertTrue(partitionMatchesPredicate(filterConstraint, ImmutableMap.of()));
    }

    @Test
    public void testPartitionValueMatchesDomain()
    {
        IcebergColumnHandle yearColumn = primitiveIcebergColumnHandle(YEAR_COLUMN_ID, "year", INTEGER, Optional.empty());
        TupleDomain<IcebergColumnHandle> filterConstraint = TupleDomain.withColumnDomains(
                ImmutableMap.of(yearColumn, Domain.singleValue(INTEGER, 2024L)));

        Map<Integer, HivePartitionKey> partitionKeys = ImmutableMap.of(
                YEAR_COLUMN_ID, new HivePartitionKey("year", Optional.of("2024")));

        assertTrue(partitionMatchesPredicate(filterConstraint, partitionKeys));
    }

    @Test
    public void testPartitionValueNotInDomain()
    {
        IcebergColumnHandle yearColumn = primitiveIcebergColumnHandle(YEAR_COLUMN_ID, "year", INTEGER, Optional.empty());
        TupleDomain<IcebergColumnHandle> filterConstraint = TupleDomain.withColumnDomains(
                ImmutableMap.of(yearColumn, Domain.multipleValues(INTEGER, ImmutableList.of(2023L, 2025L))));

        Map<Integer, HivePartitionKey> partitionKeys = ImmutableMap.of(
                YEAR_COLUMN_ID, new HivePartitionKey("year", Optional.of("2024")));

        assertFalse(partitionMatchesPredicate(filterConstraint, partitionKeys));
    }

    @Test
    public void testNullPartitionValueWithNullAllowed()
    {
        IcebergColumnHandle yearColumn = primitiveIcebergColumnHandle(YEAR_COLUMN_ID, "year", INTEGER, Optional.empty());
        TupleDomain<IcebergColumnHandle> filterConstraint = TupleDomain.withColumnDomains(
                ImmutableMap.of(yearColumn, Domain.onlyNull(INTEGER)));

        Map<Integer, HivePartitionKey> partitionKeys = ImmutableMap.of(
                YEAR_COLUMN_ID, new HivePartitionKey("year", Optional.empty()));

        assertTrue(partitionMatchesPredicate(filterConstraint, partitionKeys));
    }

    @Test
    public void testNullPartitionValueWithNullNotAllowed()
    {
        IcebergColumnHandle yearColumn = primitiveIcebergColumnHandle(YEAR_COLUMN_ID, "year", INTEGER, Optional.empty());
        TupleDomain<IcebergColumnHandle> filterConstraint = TupleDomain.withColumnDomains(
                ImmutableMap.of(yearColumn, Domain.singleValue(INTEGER, 2024L)));

        Map<Integer, HivePartitionKey> partitionKeys = ImmutableMap.of(
                YEAR_COLUMN_ID, new HivePartitionKey("year", Optional.empty()));

        assertFalse(partitionMatchesPredicate(filterConstraint, partitionKeys));
    }

    @Test
    public void testNonPartitionColumnInFilter()
    {
        IcebergColumnHandle nonPartitionColumn = primitiveIcebergColumnHandle(99, "non_partition_col", INTEGER, Optional.empty());
        TupleDomain<IcebergColumnHandle> filterConstraint = TupleDomain.withColumnDomains(
                ImmutableMap.of(nonPartitionColumn, Domain.singleValue(INTEGER, 2024L)));

        Map<Integer, HivePartitionKey> partitionKeys = ImmutableMap.of(
                YEAR_COLUMN_ID, new HivePartitionKey("year", Optional.of("2020")));

        assertTrue(partitionMatchesPredicate(filterConstraint, partitionKeys));
    }

    @Test
    public void testMultiplePartitionColumns()
    {
        IcebergColumnHandle yearColumn = primitiveIcebergColumnHandle(YEAR_COLUMN_ID, "year", INTEGER, Optional.empty());
        IcebergColumnHandle monthColumn = primitiveIcebergColumnHandle(MONTH_COLUMN_ID, "month", INTEGER, Optional.empty());

        TupleDomain<IcebergColumnHandle> filterConstraint = TupleDomain.withColumnDomains(
                ImmutableMap.of(
                        yearColumn, Domain.singleValue(INTEGER, 2024L),
                        monthColumn, Domain.multipleValues(INTEGER, ImmutableList.of(1L, 6L, 12L))));

        Map<Integer, HivePartitionKey> matchingPartition = ImmutableMap.of(
                YEAR_COLUMN_ID, new HivePartitionKey("year", Optional.of("2024")),
                MONTH_COLUMN_ID, new HivePartitionKey("month", Optional.of("6")));
        assertTrue(partitionMatchesPredicate(filterConstraint, matchingPartition));

        Map<Integer, HivePartitionKey> nonMatchingPartition = ImmutableMap.of(
                YEAR_COLUMN_ID, new HivePartitionKey("year", Optional.of("2024")),
                MONTH_COLUMN_ID, new HivePartitionKey("month", Optional.of("3")));
        assertFalse(partitionMatchesPredicate(filterConstraint, nonMatchingPartition));

        Map<Integer, HivePartitionKey> wrongYearPartition = ImmutableMap.of(
                YEAR_COLUMN_ID, new HivePartitionKey("year", Optional.of("2023")),
                MONTH_COLUMN_ID, new HivePartitionKey("month", Optional.of("6")));
        assertFalse(partitionMatchesPredicate(filterConstraint, wrongYearPartition));
    }

    @Test
    public void testPartitionMatchesDynamicFilterWithYearTransform()
    {
        Schema schema = new Schema(
                Types.NestedField.required(1, "order_date", Types.DateType.get()),
                Types.NestedField.optional(2, "amount", Types.DoubleType.get()));

        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .year("order_date")
                .build();

        // year transform value = 54 (2024 - 1970)
        DataFile dataFile2024 = DataFiles.builder(spec)
                .withPath("/data/order_date_year=2024/file1.parquet")
                .withFileSizeInBytes(1024)
                .withRecordCount(100)
                .withPartitionPath("order_date_year=54")
                .build();

        DataFile dataFile2023 = DataFiles.builder(spec)
                .withPath("/data/order_date_year=2023/file2.parquet")
                .withFileSizeInBytes(1024)
                .withRecordCount(100)
                .withPartitionPath("order_date_year=53")
                .build();

        String schemaString = SchemaParser.toJson(schema);
        String specString = PartitionSpecParser.toJson(spec);
        ResidualEvaluator residuals = ResidualEvaluator.of(spec, alwaysTrue(), false);

        FileScanTask task2024 = new BaseFileScanTask(dataFile2024, new DeleteFile[0], schemaString, specString, residuals);
        FileScanTask task2023 = new BaseFileScanTask(dataFile2023, new DeleteFile[0], schemaString, specString, residuals);

        // 2024-01-01 = 19723, 2024-12-31 = 20088 (days since epoch)
        IcebergColumnHandle orderDateColumn = primitiveIcebergColumnHandle(1, "order_date", DATE, Optional.empty());
        TupleDomain<IcebergColumnHandle> filterFor2024 = TupleDomain.withColumnDomains(
                ImmutableMap.of(orderDateColumn, Domain.create(
                        ValueSet.ofRanges(
                                Range.range(DATE, 19723L, true, 20088L, true)),
                        false)));

        assertTrue(partitionMatchesDynamicFilter(filterFor2024, task2024),
                "Year 2024 partition should match filter for dates in 2024");

        assertFalse(partitionMatchesDynamicFilter(filterFor2024, task2023),
                "Year 2023 partition should not match filter for dates in 2024");
    }

    @Test
    public void testPartitionMatchesDynamicFilterWithMonthTransform()
    {
        Schema schema = new Schema(
                Types.NestedField.required(1, "order_date", Types.DateType.get()),
                Types.NestedField.optional(2, "amount", Types.DoubleType.get()));

        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .month("order_date")
                .build();

        // Month transform: months since epoch (2024-06 = 653, 2024-01 = 648)
        DataFile dataFileJun2024 = DataFiles.builder(spec)
                .withPath("/data/order_date_month=2024-06/file1.parquet")
                .withFileSizeInBytes(1024)
                .withRecordCount(100)
                .withPartitionPath("order_date_month=653")
                .build();

        DataFile dataFileJan2024 = DataFiles.builder(spec)
                .withPath("/data/order_date_month=2024-01/file2.parquet")
                .withFileSizeInBytes(1024)
                .withRecordCount(100)
                .withPartitionPath("order_date_month=648")
                .build();

        String schemaString = SchemaParser.toJson(schema);
        String specString = PartitionSpecParser.toJson(spec);
        ResidualEvaluator residuals = ResidualEvaluator.of(spec, alwaysTrue(), false);

        FileScanTask taskJun = new BaseFileScanTask(dataFileJun2024, new DeleteFile[0], schemaString, specString, residuals);
        FileScanTask taskJan = new BaseFileScanTask(dataFileJan2024, new DeleteFile[0], schemaString, specString, residuals);

        // 2024-06-01 = 19875, 2024-06-30 = 19904 (days since epoch)
        IcebergColumnHandle orderDateColumn = primitiveIcebergColumnHandle(1, "order_date", DATE, Optional.empty());
        TupleDomain<IcebergColumnHandle> filterForJun2024 = TupleDomain.withColumnDomains(
                ImmutableMap.of(orderDateColumn, Domain.create(
                        ValueSet.ofRanges(
                                Range.range(DATE, 19875L, true, 19904L, true)),
                        false)));

        assertTrue(partitionMatchesDynamicFilter(filterForJun2024, taskJun),
                "June 2024 partition should match filter for June 2024 dates");

        assertFalse(partitionMatchesDynamicFilter(filterForJun2024, taskJan),
                "January 2024 partition should not match filter for June 2024 dates");
    }

    @Test
    public void testPartitionMatchesDynamicFilterWithIdentityTransform()
    {
        Schema schema = new Schema(
                Types.NestedField.required(1, "customer_id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "amount", Types.DoubleType.get()));

        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .identity("customer_id")
                .build();

        DataFile dataFileCustomer1 = DataFiles.builder(spec)
                .withPath("/data/customer_id=1/file1.parquet")
                .withFileSizeInBytes(1024)
                .withRecordCount(100)
                .withPartitionPath("customer_id=1")
                .build();

        DataFile dataFileCustomer2 = DataFiles.builder(spec)
                .withPath("/data/customer_id=2/file2.parquet")
                .withFileSizeInBytes(1024)
                .withRecordCount(100)
                .withPartitionPath("customer_id=2")
                .build();

        String schemaString = SchemaParser.toJson(schema);
        String specString = PartitionSpecParser.toJson(spec);
        ResidualEvaluator residuals = ResidualEvaluator.of(spec, alwaysTrue(), false);

        FileScanTask task1 = new BaseFileScanTask(dataFileCustomer1, new DeleteFile[0], schemaString, specString, residuals);
        FileScanTask task2 = new BaseFileScanTask(dataFileCustomer2, new DeleteFile[0], schemaString, specString, residuals);

        IcebergColumnHandle customerIdColumn = primitiveIcebergColumnHandle(1, "customer_id", INTEGER, Optional.empty());
        TupleDomain<IcebergColumnHandle> filterForCustomer1 = TupleDomain.withColumnDomains(
                ImmutableMap.of(customerIdColumn, Domain.singleValue(INTEGER, 1L)));

        assertTrue(partitionMatchesDynamicFilter(filterForCustomer1, task1),
                "customer_id=1 partition should match filter for customer_id=1");

        assertFalse(partitionMatchesDynamicFilter(filterForCustomer1, task2),
                "customer_id=2 partition should not match filter for customer_id=1");
    }

    @Test
    public void testPartitionMatchesDynamicFilterAllConstraint()
    {
        Schema schema = new Schema(
                Types.NestedField.required(1, "order_date", Types.DateType.get()));

        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .year("order_date")
                .build();

        DataFile dataFile = DataFiles.builder(spec)
                .withPath("/data/file.parquet")
                .withFileSizeInBytes(1024)
                .withRecordCount(100)
                .withPartitionPath("order_date_year=54")
                .build();

        String schemaString = SchemaParser.toJson(schema);
        String specString = PartitionSpecParser.toJson(spec);
        ResidualEvaluator residuals = ResidualEvaluator.of(spec, alwaysTrue(), false);
        FileScanTask task = new BaseFileScanTask(dataFile, new DeleteFile[0], schemaString, specString, residuals);

        assertTrue(partitionMatchesDynamicFilter(TupleDomain.<IcebergColumnHandle>all(), task));
    }

    @Test
    public void testPartitionMatchesDynamicFilterNoneConstraint()
    {
        Schema schema = new Schema(
                Types.NestedField.required(1, "order_date", Types.DateType.get()));

        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .year("order_date")
                .build();

        DataFile dataFile = DataFiles.builder(spec)
                .withPath("/data/file.parquet")
                .withFileSizeInBytes(1024)
                .withRecordCount(100)
                .withPartitionPath("order_date_year=54")
                .build();

        String schemaString = SchemaParser.toJson(schema);
        String specString = PartitionSpecParser.toJson(spec);
        ResidualEvaluator residuals = ResidualEvaluator.of(spec, alwaysTrue(), false);
        FileScanTask task = new BaseFileScanTask(dataFile, new DeleteFile[0], schemaString, specString, residuals);

        assertFalse(partitionMatchesDynamicFilter(TupleDomain.<IcebergColumnHandle>none(), task));
    }

    private static boolean partitionMatchesPredicate(
            TupleDomain<IcebergColumnHandle> filterConstraint,
            Map<Integer, HivePartitionKey> partitionKeys)
    {
        if (filterConstraint.isAll()) {
            return true;
        }
        if (filterConstraint.isNone()) {
            return false;
        }
        if (partitionKeys.isEmpty()) {
            return true;
        }
        if (!filterConstraint.getDomains().isPresent()) {
            return true;
        }

        for (Map.Entry<IcebergColumnHandle, Domain> entry : filterConstraint.getDomains().get().entrySet()) {
            IcebergColumnHandle columnHandle = entry.getKey();
            Domain domain = entry.getValue();
            int columnId = columnHandle.getId();

            HivePartitionKey partitionKey = partitionKeys.get(columnId);
            if (partitionKey == null) {
                continue;
            }

            Optional<String> valueOpt = partitionKey.getValue();
            if (!valueOpt.isPresent()) {
                if (!domain.isNullAllowed()) {
                    return false;
                }
            }
            else {
                Object convertedValue = convertPartitionValue(columnHandle.getType(), valueOpt.get());
                if (convertedValue == null) {
                    continue;
                }
                if (!domain.includesNullableValue(convertedValue)) {
                    return false;
                }
            }
        }

        return true;
    }

    private static boolean partitionMatchesDynamicFilter(
            TupleDomain<IcebergColumnHandle> filterConstraint,
            FileScanTask task)
    {
        if (filterConstraint.isAll()) {
            return true;
        }
        if (filterConstraint.isNone()) {
            return false;
        }
        if (!task.spec().isPartitioned()) {
            return true;
        }

        Expression icebergExpression = ExpressionConverter.toIcebergExpression(filterConstraint);
        Expression projected = Projections.inclusive(task.spec()).project(icebergExpression);
        return new Evaluator(task.spec().partitionType(), projected, false).eval(task.file().partition());
    }

    private static Object convertPartitionValue(Type type, String valueString)
    {
        if (valueString == null) {
            return null;
        }
        try {
            return deserializePartitionValue(type, valueString, "dynamic_filter");
        }
        catch (Exception e) {
            return null;
        }
    }
}
