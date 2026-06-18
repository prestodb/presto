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

/**
 * Simple TableLocationProvider implementation that doesn't depend on BaseJdbcConfig.
 * This demonstrates that TableLocationProvider implementations can use their own
 * configuration mechanisms without depending on BaseJdbcConfig.
 */
public class SimpleTestTableLocationProvider
        implements TableLocationProvider
{
    private final String jdbcUrl;

    @Inject
    public SimpleTestTableLocationProvider(SimpleTestJdbcConfig config)
    {
        requireNonNull(config, "config is null");
        this.jdbcUrl = config.getJdbcUrl();
    }

    @Override
    public String getTableLocation()
    {
        return jdbcUrl;
    }
}
