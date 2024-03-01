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
package com.facebook.presto.iceberg.delete;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.iceberg.IcebergColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.util.StructLikeSet;
import org.apache.iceberg.util.StructProjection;

import java.util.List;

import static com.facebook.presto.iceberg.IcebergUtil.schemaFromHandles;
import static java.util.Objects.requireNonNull;

public final class EqualityDeleteFilter
        implements DeleteFilter
{
    private final Schema schema;
    private final StructLikeSet deleteSet;

    private EqualityDeleteFilter(Schema schema, StructLikeSet deleteSet)
    {
        this.schema = requireNonNull(schema, "schema is null");
        this.deleteSet = requireNonNull(deleteSet, "deleteSet is null");
    }

    @Override
    public RowPredicate createPredicate(List<IcebergColumnHandle> columns)
    {
        Type[] types = columns.stream()
                .map(IcebergColumnHandle::getType)
                .toArray(Type[]::new);

        Schema fileSchema = schemaFromHandles(columns);
        StructProjection projection = StructProjection.create(fileSchema, schema);

        return (page, position) -> {
            StructLike row = new LazyStructLikeRow(types, page, position);
            return !deleteSet.contains(projection.wrap(row));
        };
    }

    public static DeleteFilter readEqualityDeletes(ConnectorPageSource pageSource, List<IcebergColumnHandle> columns, Schema tableSchema)
    {
        Type[] types = columns.stream()
                .map(IcebergColumnHandle::getType)
                .toArray(Type[]::new);

        Schema deleteSchema = schemaFromHandles(columns);
        StructLikeSet deleteSet = StructLikeSet.create(deleteSchema.asStruct());

        while (!pageSource.isFinished()) {
            Page page = pageSource.getNextPage();
            if (page == null) {
                continue;
            }

            for (int position = 0; position < page.getPositionCount(); position++) {
                deleteSet.add(new StructLikeRow(types, page, position));
            }
        }

        return new EqualityDeleteFilter(deleteSchema, deleteSet);
    }
}
