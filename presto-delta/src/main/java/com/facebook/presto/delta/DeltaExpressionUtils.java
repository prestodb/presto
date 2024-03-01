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
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.data.CloseableIterator;

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.delta.DeltaColumnHandle.ColumnType.PARTITION;
import static com.facebook.presto.delta.DeltaErrorCode.DELTA_INVALID_PARTITION_VALUE;
import static com.facebook.presto.delta.DeltaErrorCode.DELTA_UNSUPPORTED_COLUMN_TYPE;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Double.doubleToRawLongBits;
import static java.lang.Double.parseDouble;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.parseFloat;
import static java.lang.Long.parseLong;
import static java.lang.String.format;

public final class DeltaExpressionUtils
{
    private DeltaExpressionUtils()
    {
    }

    /**
     * Split the predicate into partition and regular column predicates
     */
    public static List<TupleDomain<ColumnHandle>> splitPredicate(
            TupleDomain<ColumnHandle> predicate)
    {
        ImmutableMap.Builder<ColumnHandle, Domain> partitionColumnPredicates = ImmutableMap.builder();
        ImmutableMap.Builder<ColumnHandle, Domain> regularColumnPredicates = ImmutableMap.builder();

        Optional<Map<ColumnHandle, Domain>> domains = predicate.getDomains();
        if (domains.isPresent()) {
            domains.get().entrySet().stream()
                    .forEach(domainPair -> {
                        DeltaColumnHandle columnHandle = (DeltaColumnHandle) domainPair.getKey();
                        if (columnHandle.getColumnType() == PARTITION) {
                            partitionColumnPredicates.put(domainPair.getKey(), domainPair.getValue());
                        }
                        else {
                            regularColumnPredicates.put(domainPair.getKey(), domainPair.getValue());
                        }
                    });
        }

        return ImmutableList.of(
                TupleDomain.withColumnDomains(partitionColumnPredicates.build()),
                TupleDomain.withColumnDomains(regularColumnPredicates.build()));
    }

    /**
     * Utility method that takes an iterator of {@link AddFile}s and a predicate and returns an iterator of {@link AddFile}s
     * that satisfy the predicate (predicate evaluates to a deterministic NO)
     */
    public static CloseableIterator<AddFile> iterateWithPartitionPruning(
            CloseableIterator<AddFile> inputIterator,
            TupleDomain<DeltaColumnHandle> predicate,
            TypeManager typeManager)
    {
        TupleDomain<String> partitionPredicate = extractPartitionColumnsPredicate(predicate);
        if (partitionPredicate.isAll()) {
            return inputIterator; // there is no partition filter, return the input iterator as is.
        }

        if (partitionPredicate.isNone()) {
            // nothing passes the partition predicate, return empty iterator
            return new CloseableIterator<AddFile>()
            {
                @Override
                public boolean hasNext()
                {
                    return false;
                }

                @Override
                public AddFile next()
                {
                    throw new NoSuchElementException();
                }

                @Override
                public void close()
                        throws IOException
                {
                    inputIterator.close();
                }
            };
        }

        List<DeltaColumnHandle> partitionColumns =
                predicate.getColumnDomains().get().stream()
                        .filter(entry -> entry.getColumn().getColumnType() == PARTITION)
                        .map(entry -> entry.getColumn())
                        .collect(Collectors.toList());

        return new CloseableIterator<AddFile>()
        {
            private AddFile nextItem;

            @Override
            public boolean hasNext()
            {
                if (nextItem != null) {
                    return true;
                }

                while (inputIterator.hasNext()) {
                    AddFile nextFile = inputIterator.next();
                    if (evaluatePartitionPredicate(partitionPredicate, partitionColumns, typeManager, nextFile)) {
                        nextItem = nextFile;
                        break;
                    }
                }

                return nextItem != null;
            }

            @Override
            public AddFile next()
            {
                if (!hasNext()) {
                    throw new NoSuchElementException("there are no more files");
                }
                AddFile toReturn = nextItem;
                nextItem = null;
                return toReturn;
            }

            @Override
            public void close()
                    throws IOException
            {
                inputIterator.close();
            }
        };
    }

    private static TupleDomain<String> extractPartitionColumnsPredicate(TupleDomain<DeltaColumnHandle> predicate)
    {
        return predicate.transform(
                deltaColumnHandle -> {
                    if (deltaColumnHandle.getColumnType() != PARTITION) {
                        return null;
                    }
                    return deltaColumnHandle.getName();
                });
    }

    private static boolean evaluatePartitionPredicate(
            TupleDomain<String> partitionPredicate,
            List<DeltaColumnHandle> partitionColumns,
            TypeManager typeManager,
            AddFile addFile)
    {
        checkArgument(!partitionPredicate.isNone(), "Expecting a predicate with at least one expression");
        for (DeltaColumnHandle partitionColumn : partitionColumns) {
            String columnName = partitionColumn.getName();
            String partitionValue = addFile.getPartitionValues().get(columnName);
            Domain domain = getDomain(partitionColumn, partitionValue, typeManager, addFile.getPath());
            Domain columnPredicate = partitionPredicate.getDomains().get().get(columnName);

            if (columnPredicate == null) {
                continue; // there is no predicate on this column
            }

            if (columnPredicate.intersect(domain).isNone()) {
                return false;
            }
        }

        return true;
    }

    private static Domain getDomain(DeltaColumnHandle columnHandle, String partitionValue, TypeManager typeManager, String filePath)
    {
        Type type = typeManager.getType(columnHandle.getDataType());
        if (partitionValue == null) {
            return Domain.onlyNull(type);
        }

        String typeBase = columnHandle.getDataType().getBase();
        try {
            switch (typeBase) {
                case StandardTypes.TINYINT:
                case StandardTypes.SMALLINT:
                case StandardTypes.INTEGER:
                case StandardTypes.BIGINT:
                    Long intValue = parseLong(partitionValue);
                    return Domain.create(ValueSet.of(type, intValue), false);
                case StandardTypes.REAL:
                    Long realValue = (long) floatToRawIntBits(parseFloat(partitionValue));
                    return Domain.create(ValueSet.of(type, realValue), false);
                case StandardTypes.DOUBLE:
                    Long doubleValue = doubleToRawLongBits(parseDouble(partitionValue));
                    return Domain.create(ValueSet.of(type, doubleValue), false);
                case StandardTypes.VARCHAR:
                case StandardTypes.VARBINARY:
                    Slice sliceValue = utf8Slice(partitionValue);
                    return Domain.create(ValueSet.of(type, sliceValue), false);
                case StandardTypes.DATE:
                    Long dateValue = Date.valueOf(partitionValue).getTime(); // convert to millis
                    return Domain.create(ValueSet.of(type, dateValue), false);
                case StandardTypes.TIMESTAMP:
                    Long timestampValue = Timestamp.valueOf(partitionValue).getTime(); // convert to millis
                    return Domain.create(ValueSet.of(type, timestampValue), false);
                case StandardTypes.BOOLEAN:
                    Boolean booleanValue = Boolean.valueOf(partitionValue);
                    return Domain.create(ValueSet.of(type, booleanValue), false);
                default:
                    throw new PrestoException(DELTA_UNSUPPORTED_COLUMN_TYPE,
                            format("Unsupported data type '%s' for partition column %s", columnHandle.getDataType(), columnHandle.getName()));
            }
        }
        catch (IllegalArgumentException exception) {
            throw new PrestoException(DELTA_INVALID_PARTITION_VALUE,
                    format("Can not parse partition value '%s' of type '%s' for partition column '%s' in file '%s'",
                            partitionValue, columnHandle.getDataType(), columnHandle.getName(), filePath),
                    exception);
        }
    }
}
