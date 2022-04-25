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
package com.facebook.presto.plugin.clickhouse;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class ClickHouseMetadataFactory
{
    private final ClickHouseClient clickHouseClient;
    private final boolean allowDropTable;

    @Inject
    public ClickHouseMetadataFactory(ClickHouseClient clickHouseClient, ClickHouseConfig config)
    {
        this.clickHouseClient = requireNonNull(clickHouseClient, "clickHouseClient is null");
        requireNonNull(config, "config is null");
        this.allowDropTable = config.isAllowDropTable();
    }

    public ClickHouseMetadata create()
    {
        return new ClickHouseMetadata(clickHouseClient, allowDropTable);
    }
}
