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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.airlift.slice.Slices.utf8Slice;

public final class SessionFunctions
{
    private SessionFunctions() {}

    @ScalarFunction(value = "$current_user", hidden = true)
    @Description("current user")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice currentUser(ConnectorSession session)
    {
        return utf8Slice(session.getUser());
    }

    @ScalarFunction(value = "$current_path", hidden = true)
    @Description("retrieve current path")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice currentPath(ConnectorSession session)
    {
        if (session.getPath() == null) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Session path is null");
        }
        return utf8Slice(session.getPath());
    }
}
