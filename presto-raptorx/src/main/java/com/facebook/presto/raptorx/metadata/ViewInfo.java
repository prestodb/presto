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

import static com.facebook.presto.raptorx.util.DatabaseUtil.optionalUtf8String;
import static com.facebook.presto.raptorx.util.DatabaseUtil.utf8String;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ViewInfo
{
    private final long viewId;
    private final String viewName;
    private final long schemaId;
    private final long createTime;
    private final long updateTime;
    private final String viewData;
    private final Optional<String> comment;

    public ViewInfo(
            long viewId,
            String viewName,
            long schemaId,
            long createTime,
            long updateTime,
            String viewData,
            Optional<String> comment)
    {
        this.viewId = viewId;
        this.viewName = requireNonNull(viewName, "viewName is null");
        this.schemaId = schemaId;
        this.createTime = createTime;
        this.updateTime = updateTime;
        this.viewData = requireNonNull(viewData, "viewData is null");
        this.comment = requireNonNull(comment, "comment is null");
    }

    public long getViewId()
    {
        return viewId;
    }

    public String getViewName()
    {
        return viewName;
    }

    public long getSchemaId()
    {
        return schemaId;
    }

    public long getCreateTime()
    {
        return createTime;
    }

    public long getUpdateTime()
    {
        return updateTime;
    }

    public String getViewData()
    {
        return viewData;
    }

    public Optional<String> getComment()
    {
        return comment;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("viewId", viewId)
                .toString();
    }

    public static class Mapper
            implements RowMapper<ViewInfo>
    {
        @Override
        public ViewInfo map(ResultSet rs, StatementContext context)
                throws SQLException
        {
            return new ViewInfo(
                    rs.getLong("view_id"),
                    utf8String(rs.getBytes("view_name")),
                    rs.getLong("schema_id"),
                    rs.getLong("create_time"),
                    rs.getLong("update_time"),
                    utf8String(rs.getBytes("view_data")),
                    optionalUtf8String(rs.getBytes("comment")));
        }
    }
}
