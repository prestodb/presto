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
package com.facebook.presto.hive.s3select;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.SortedRangeSet;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveFileSplit;
import com.facebook.presto.hive.TestBackgroundHiveSplitLoader;
import com.facebook.presto.hive.s3.PrestoS3ClientFactory;
import com.facebook.presto.spi.RecordCursor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.common.predicate.TupleDomain.withColumnDomains;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.hive.HiveTestUtils.FUNCTION_AND_TYPE_MANAGER;
import static com.facebook.presto.hive.HiveTestUtils.SESSION;
import static com.facebook.presto.hive.s3select.TestS3SelectRecordCursor.ARTICLE_COLUMN;
import static com.facebook.presto.hive.s3select.TestS3SelectRecordCursor.AUTHOR_COLUMN;
import static com.facebook.presto.hive.s3select.TestS3SelectRecordCursor.DATE_ARTICLE_COLUMN;
import static com.facebook.presto.hive.s3select.TestS3SelectRecordCursor.QUANTITY_COLUMN;
import static org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMNS;
import static org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMN_TYPES;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestS3SelectRecordCursorProvider
{
    @Test
    public void shouldReturnSelectRecordCursor()
    {
        List<HiveColumnHandle> columns = new ArrayList<>();
        TupleDomain<HiveColumnHandle> effectivePredicate = TupleDomain.all();
        Optional<RecordCursor> recordCursor =
                getRecordCursor(effectivePredicate, columns, true);
        assertTrue(recordCursor.isPresent());
    }

    @Test
    public void shouldReturnSelectRecordCursorWhenEffectivePredicateExists()
    {
        TupleDomain<HiveColumnHandle> effectivePredicate = withColumnDomains(ImmutableMap.of(QUANTITY_COLUMN,
                Domain.create(SortedRangeSet.copyOf(BIGINT, ImmutableList.of(Range.equal(BIGINT, 3L))), false)));
        Optional<RecordCursor> recordCursor =
                getRecordCursor(effectivePredicate, getAllColumns(), true);
        assertTrue(recordCursor.isPresent());
    }

    @Test
    public void shouldReturnSelectRecordCursorWhenProjectionExists()
    {
        TupleDomain<HiveColumnHandle> effectivePredicate = TupleDomain.all();
        List<HiveColumnHandle> columns = ImmutableList.of(QUANTITY_COLUMN, AUTHOR_COLUMN, ARTICLE_COLUMN);
        Optional<RecordCursor> recordCursor =
                getRecordCursor(effectivePredicate, columns, true);
        assertTrue(recordCursor.isPresent());
    }

    @Test
    public void shouldNotReturnSelectRecordCursorWhenPushdownIsDisabled()
    {
        List<HiveColumnHandle> columns = new ArrayList<>();
        TupleDomain<HiveColumnHandle> effectivePredicate = TupleDomain.all();
        Optional<RecordCursor> recordCursor =
                getRecordCursor(effectivePredicate, columns, false);
        assertFalse(recordCursor.isPresent());
    }

    @Test
    public void shouldNotReturnSelectRecordCursorWhenQueryIsNotFiltering()
    {
        TupleDomain<HiveColumnHandle> effectivePredicate = TupleDomain.all();
        Optional<RecordCursor> recordCursor =
                getRecordCursor(effectivePredicate, getAllColumns(), true);
        assertFalse(recordCursor.isPresent());
    }

    @Test
    public void shouldNotReturnSelectRecordCursorWhenProjectionOrderIsDifferent()
    {
        TupleDomain<HiveColumnHandle> effectivePredicate = TupleDomain.all();
        List<HiveColumnHandle> columns = ImmutableList.of(DATE_ARTICLE_COLUMN, QUANTITY_COLUMN, ARTICLE_COLUMN, AUTHOR_COLUMN);
        Optional<RecordCursor> recordCursor =
                getRecordCursor(effectivePredicate, columns, true);
        assertFalse(recordCursor.isPresent());
    }

    private static Optional<RecordCursor> getRecordCursor(TupleDomain<HiveColumnHandle> effectivePredicate,
            List<HiveColumnHandle> columns,
            boolean s3SelectPushdownEnabled)
    {
        S3SelectRecordCursorProvider s3SelectRecordCursorProvider = new S3SelectRecordCursorProvider(
                new TestBackgroundHiveSplitLoader.TestingHdfsEnvironment(new ArrayList<>()),
                new HiveClientConfig(),
                new PrestoS3ClientFactory());
        HiveFileSplit fileSplit = new HiveFileSplit(
                "s3://fakeBucket/fakeObject.gz",
                0,
                100,
                100,
                0,
                Optional.empty(),
                ImmutableMap.of());
        return s3SelectRecordCursorProvider.createRecordCursor(
                new Configuration(),
                SESSION,
                fileSplit,
                createTestingSchema(),
                columns,
                effectivePredicate,
                DateTimeZone.forID(SESSION.getSqlFunctionProperties().getTimeZoneKey().getId()),
                FUNCTION_AND_TYPE_MANAGER,
                s3SelectPushdownEnabled);
    }

    private static Properties createTestingSchema()
    {
        List<HiveColumnHandle> schemaColumns = getAllColumns();
        Properties schema = new Properties();
        String columnNames = buildPropertyFromColumns(schemaColumns, HiveColumnHandle::getName);
        String columnTypeNames = buildPropertyFromColumns(schemaColumns, column -> column.getHiveType().getTypeInfo().getTypeName());
        schema.setProperty(LIST_COLUMNS, columnNames);
        schema.setProperty(LIST_COLUMN_TYPES, columnTypeNames);
        String deserializerClassName = LazySimpleSerDe.class.getName();
        schema.setProperty(SERIALIZATION_LIB, deserializerClassName);
        return schema;
    }

    private static String buildPropertyFromColumns(List<HiveColumnHandle> columns, Function<HiveColumnHandle, String> mapper)
    {
        if (columns.isEmpty()) {
            return "";
        }
        return columns.stream()
                .map(mapper)
                .collect(Collectors.joining(","));
    }

    private static List<HiveColumnHandle> getAllColumns()
    {
        return ImmutableList.of(ARTICLE_COLUMN, AUTHOR_COLUMN, DATE_ARTICLE_COLUMN, QUANTITY_COLUMN);
    }
}
