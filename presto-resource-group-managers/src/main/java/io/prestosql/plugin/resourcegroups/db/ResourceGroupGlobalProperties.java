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
package io.prestosql.plugin.resourcegroups.db;

import io.airlift.units.Duration;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ResourceGroupGlobalProperties
{
    private final Optional<Duration> cpuQuotaPeriod;

    public ResourceGroupGlobalProperties(Optional<Duration> cpuQuotaPeriod)
    {
        this.cpuQuotaPeriod = requireNonNull(cpuQuotaPeriod, "Cpu Quota Period is null");
    }

    public Optional<Duration> getCpuQuotaPeriod()
    {
        return cpuQuotaPeriod;
    }

    public static class Mapper
            implements RowMapper<ResourceGroupGlobalProperties>
    {
        @Override
        public ResourceGroupGlobalProperties map(ResultSet resultSet, StatementContext context)
                throws SQLException
        {
            return new ResourceGroupGlobalProperties(Optional.ofNullable(resultSet.getString("value")).map(Duration::valueOf));
        }
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == this) {
            return true;
        }

        if (!(other instanceof ResourceGroupGlobalProperties)) {
            return false;
        }

        ResourceGroupGlobalProperties that = (ResourceGroupGlobalProperties) other;
        return cpuQuotaPeriod.equals(that.cpuQuotaPeriod);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(cpuQuotaPeriod);
    }
}
