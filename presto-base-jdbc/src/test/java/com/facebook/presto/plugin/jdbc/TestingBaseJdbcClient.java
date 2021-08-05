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

import com.facebook.presto.plugin.jdbc.optimization.JdbcSortItem;

import java.util.List;

public class TestingBaseJdbcClient
        extends BaseJdbcClient
{
    public TestingBaseJdbcClient(JdbcConnectorId connectorId, BaseJdbcConfig config,
            String identifierQuote, ConnectionFactory connectionFactory)
    {
        super(connectorId, config, identifierQuote, connectionFactory);
    }

    @Override
    public boolean supportsLimit()
    {
        return true;
    }

    @Override
    public boolean supportsTopN(List<JdbcSortItem> sortItems)
    {
        return true;
    }
}
