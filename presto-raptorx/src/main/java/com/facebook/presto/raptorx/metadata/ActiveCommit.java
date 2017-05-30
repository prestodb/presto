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

import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ActiveCommit
{
    private final long commitId;
    private final long startTime;
    private final boolean rollingBack;
    private final Optional<byte[]> rollbackInfo;

    public ActiveCommit(long commitId, long startTime, boolean rollingBack, Optional<byte[]> rollbackInfo)
    {
        this.commitId = commitId;
        this.startTime = startTime;
        this.rollingBack = rollingBack;
        this.rollbackInfo = requireNonNull(rollbackInfo, "rollbackInfo is null");
    }

    public long getCommitId()
    {
        return commitId;
    }

    public long getStartTime()
    {
        return startTime;
    }

    public boolean isRollingBack()
    {
        return rollingBack;
    }

    public RollbackInfo getRollbackInfo()
    {
        return RollbackInfo.fromBytes(rollbackInfo);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("commitId", commitId)
                .add("startTime", startTime)
                .add("rollingBack", rollingBack)
                .toString();
    }

    public static class Mapper
            implements RowMapper<ActiveCommit>
    {
        @Override
        public ActiveCommit map(ResultSet rs, StatementContext context)
                throws SQLException
        {
            return new ActiveCommit(
                    rs.getLong("commit_id"),
                    rs.getLong("start_time"),
                    rs.getBoolean("rolling_back"),
                    Optional.ofNullable(rs.getBytes("rollback_info")));
        }
    }
}
