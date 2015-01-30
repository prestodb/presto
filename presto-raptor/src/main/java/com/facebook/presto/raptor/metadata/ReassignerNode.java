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
package com.facebook.presto.raptor.metadata;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.units.Duration;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class ReassignerNode
{
    private final String nodeIdentifier;
    private final long millisSinceLastUpdate;

    public ReassignerNode(String nodeIdentifier, long millisSinceLastUpdate)
    {
        this.nodeIdentifier = checkNotNull(nodeIdentifier, "nodeIdentifier is null");
        checkArgument(millisSinceLastUpdate >= 0, "millisSinceLastUpdate must be >= 0");
        this.millisSinceLastUpdate = millisSinceLastUpdate;
    }

    @JsonProperty
    public String getNodeIdentifier()
    {
        return nodeIdentifier;
    }

    @JsonProperty
    public long getMillisSinceLastUpdate()
    {
        return millisSinceLastUpdate;
    }

    public Duration getDurationSinceLastUpdate()
    {
        return new Duration(millisSinceLastUpdate, TimeUnit.MILLISECONDS);
    }

    public static class Mapper
            implements ResultSetMapper<ReassignerNode>
    {
        @Override
        public ReassignerNode map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return new ReassignerNode(
                    r.getString("node_identifier"),
                    r.getLong("millis_since_last_update"));
        }
    }
}
