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
package com.facebook.presto.raptor.storage.organization;

import com.facebook.presto.raptor.RaptorColumnHandle;
import com.facebook.presto.raptor.RaptorInsertTableHandle;
import com.facebook.presto.raptor.RaptorOutputTableHandle;
import com.facebook.presto.raptor.metadata.ForMetadata;
import com.facebook.presto.raptor.metadata.MetadataDao;
import com.facebook.presto.raptor.metadata.Table;
import com.facebook.presto.raptor.metadata.TableColumn;
import com.facebook.presto.raptor.storage.StorageManagerConfig;
import com.facebook.presto.spi.type.Type;
import com.google.inject.Inject;
import org.joda.time.DateTimeZone;
import org.skife.jdbi.v2.IDBI;

import java.util.Optional;
import java.util.OptionalLong;

import static com.facebook.presto.raptor.util.DatabaseUtil.onDemandDao;
import static java.util.Objects.requireNonNull;

public class ShardSplitterProvider
{
    final DateTimeZone defaultTimeZone;
    final TemporalColumnMetaFunction temporalColumnMetaFunction;

    @Inject
    public ShardSplitterProvider(@ForMetadata IDBI dbi, StorageManagerConfig config)
    {
        this(createColumnTypeFromMetaData(onDemandDao(dbi, MetadataDao.class)), config.getShardDayBoundaryTimeZone());
    }

    public ShardSplitterProvider(TemporalColumnMetaFunction temporalColumnMetaFunction, DateTimeZone defaultTimeZone)
    {
        this.temporalColumnMetaFunction = requireNonNull(temporalColumnMetaFunction, "temporalColumnMetaFunction is null");
        this.defaultTimeZone = requireNonNull(defaultTimeZone, "defaultTimeZone is null");
    }

    public ShardSplitter forTable(RaptorInsertTableHandle tableHandle)
    {
        return new ShardSplitter(tableHandle.getBucketCount(),
                tableHandle.getTemporalColumnHandle().map(RaptorColumnHandle::getColumnId).map(OptionalLong::of).orElse(OptionalLong.empty()),
                tableHandle.getTemporalColumnHandle().map(RaptorColumnHandle::getColumnType),
                tableHandle.getTemporalColumnHandle().isPresent() ? Optional.of(tableHandle.getTableTimeZone().orElse(defaultTimeZone)) : Optional.empty());
    }

    public ShardSplitter forTable(RaptorOutputTableHandle tableHandle)
    {
        return new ShardSplitter(tableHandle.getBucketCount(),
                tableHandle.getTemporalColumnHandle().map(RaptorColumnHandle::getColumnId).map(OptionalLong::of).orElse(OptionalLong.empty()),
                tableHandle.getTemporalColumnHandle().map(RaptorColumnHandle::getColumnType),
                tableHandle.getTemporalColumnHandle().isPresent() ? Optional.of(tableHandle.getTableTimeZone().orElse(defaultTimeZone)) : Optional.empty());
    }

    public ShardSplitter forTable(Table tableInfo)
    {
        Optional<Type> temporalType = Optional.empty();
        if (tableInfo.getTemporalColumnId().isPresent()) {
            temporalType = temporalColumnMetaFunction.get(tableInfo.getTableId(), tableInfo.getTemporalColumnId().getAsLong());
        }

        return new ShardSplitter(tableInfo.getBucketCount(),
                tableInfo.getTemporalColumnId(),
                temporalType,
                tableInfo.getTemporalColumnId().isPresent() ? Optional.of(tableInfo.getTableTimeZone().orElse(defaultTimeZone)) : Optional.empty());
    }

    public interface TemporalColumnMetaFunction
    {
        Optional<Type> get(long tableId, long columnId);
    }

    private static TemporalColumnMetaFunction createColumnTypeFromMetaData(MetadataDao dao)
    {
        return (tableId, columnId) -> Optional.ofNullable(dao.getTableColumn(tableId, columnId)).map(TableColumn::getDataType);
    }
}
