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
package io.prestosql.jdbc;

import java.sql.SQLNonTransientException;

import static java.lang.String.format;

/**
 * Thrown when a required JDBC method is not yet implemented.
 */
public class NotImplementedException
        extends SQLNonTransientException
{
    public NotImplementedException(String reason)
    {
        super(reason);
    }

    public NotImplementedException(String clazz, String method)
    {
        this(format("Method %s.%s is not yet implemented", clazz, method));
    }
}
