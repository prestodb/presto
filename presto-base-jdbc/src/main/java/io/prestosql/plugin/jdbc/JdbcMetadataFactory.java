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
package com.facebook.presto.plugin.jdbc;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class JdbcMetadataFactory
{
    private final JdbcClient jdbcClient;
    private final boolean allowDropTable;

    @Inject
    public JdbcMetadataFactory(JdbcClient jdbcClient, JdbcMetadataConfig config)
    {
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
        requireNonNull(config, "config is null");
        this.allowDropTable = config.isAllowDropTable();
    }

    public JdbcMetadata create()
    {
        return new JdbcMetadata(jdbcClient, allowDropTable);
    }
}
