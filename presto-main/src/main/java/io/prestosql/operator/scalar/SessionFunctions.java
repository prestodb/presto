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
package io.prestosql.operator.scalar;

import io.airlift.slice.Slice;
import io.prestosql.FullConnectorSession;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

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
        // this function is a language construct and has special access to internals
        return utf8Slice(((FullConnectorSession) session).getSession().getPath().toString());
    }
}
