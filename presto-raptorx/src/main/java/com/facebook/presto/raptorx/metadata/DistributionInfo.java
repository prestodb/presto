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
package com.facebook.presto.raptorx.metadata;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.raptorx.util.DatabaseUtil.optionalUtf8String;
import static com.facebook.presto.raptorx.util.DatabaseUtil.verifyMetadata;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class DistributionInfo
{
    private static final JsonCodec<List<String>> LIST_CODEC = listJsonCodec(String.class);

    private final long distributionId;
    private final Optional<String> distributionName;
    private final int bucketCount;
    private final List<Type> columnTypes;

    public DistributionInfo(long distributionId, Optional<String> distributionName, int bucketCount, List<Type> columnTypes)
    {
        this.distributionId = distributionId;
        this.distributionName = requireNonNull(distributionName, "distributionName is null");
        this.bucketCount = bucketCount;
        this.columnTypes = ImmutableList.copyOf(requireNonNull(columnTypes, "columnTypes is null"));
    }

    public long getDistributionId()
    {
        return distributionId;
    }

    public Optional<String> getDistributionName()
    {
        return distributionName;
    }

    public int getBucketCount()
    {
        return bucketCount;
    }

    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    public static class Mapper
            implements RowMapper<DistributionInfo>
    {
        private final TypeManager typeManager;

        public Mapper(TypeManager typeManager)
        {
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
        }

        @Override
        public DistributionInfo map(ResultSet rs, StatementContext context)
                throws SQLException
        {
            return new DistributionInfo(
                    rs.getLong("distribution_id"),
                    optionalUtf8String(rs.getBytes("distribution_name")),
                    rs.getInt("bucket_count"),
                    deserializeColumnTypes(typeManager, rs.getBytes("column_types")));
        }
    }

    public static byte[] serializeColumnTypes(List<Type> columnTypes)
    {
        return LIST_CODEC.toJsonBytes(columnTypes.stream()
                .map(type -> type.getTypeSignature().toString())
                .collect(toList()));
    }

    private static List<Type> deserializeColumnTypes(TypeManager typeManager, byte[] data)
    {
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (String name : LIST_CODEC.fromJson(data)) {
            Type type = typeManager.getType(parseTypeSignature(name));
            verifyMetadata(type != null, "Unknown distribution column type: %s", name);
            types.add(type);
        }
        return types.build();
    }
}
