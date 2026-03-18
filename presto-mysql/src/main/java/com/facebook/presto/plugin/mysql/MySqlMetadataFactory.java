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
package com.facebook.presto.plugin.mysql;

import com.facebook.presto.plugin.jdbc.JdbcMetadata;
import com.facebook.presto.plugin.jdbc.JdbcMetadataCache;
import com.facebook.presto.plugin.jdbc.JdbcMetadataConfig;
import com.facebook.presto.plugin.jdbc.JdbcMetadataFactory;
import com.facebook.presto.plugin.jdbc.TableLocationProvider;
import jakarta.inject.Inject;

public class MySqlMetadataFactory
        extends JdbcMetadataFactory
{
    @Inject
    public MySqlMetadataFactory(JdbcMetadataCache jdbcMetadataCache, MySqlClient jdbcClient, JdbcMetadataConfig config, TableLocationProvider tableLocationProvider)
    {
        super(jdbcMetadataCache, jdbcClient, config, tableLocationProvider);
    }

    @Override
    public JdbcMetadata create()
    {
        return new MySqlMetadata(
                jdbcMetadataCache,
                (MySqlClient) jdbcClient,
                allowDropTable,
                tableLocationProvider);
    }
}
