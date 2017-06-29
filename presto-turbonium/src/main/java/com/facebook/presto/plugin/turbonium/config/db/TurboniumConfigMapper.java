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
package com.facebook.presto.plugin.turbonium.config.db;

import com.facebook.presto.plugin.turbonium.TurboniumConfig;
import io.airlift.units.DataSize;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

import static io.airlift.units.DataSize.Unit.BYTE;

public class TurboniumConfigMapper
    implements ResultSetMapper<TurboniumConfig>
{
    @Override
    public TurboniumConfig map(int index, ResultSet resultSet, StatementContext context)
            throws SQLException
    {
        long maxDataPerNode = resultSet.getLong("max_data_per_node");
        long maxTableSizePerNode = resultSet.getLong("max_table_size_per_node");
        int splitsPerNode = resultSet.getInt("splits_per_node");
        boolean disableEncoding = resultSet.getBoolean("disable_encoding");
        return new TurboniumConfig()
                .setMaxDataPerNode(new DataSize(maxDataPerNode, BYTE))
                .setMaxTableSizePerNode(new DataSize(maxTableSizePerNode, BYTE))
                .setSplitsPerNode(splitsPerNode)
                .setDisableEncoding(disableEncoding);
    }
}
