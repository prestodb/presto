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

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static java.util.Objects.requireNonNull;

public interface ObjectWriteFunction
        extends WriteFunction
{
    @Override
    Class<?> getJavaType();

    void set(PreparedStatement statement, int index, Object value) throws SQLException;

    static <T> ObjectWriteFunction of(Class<T> javaType, ObjectWriteFunctionImplementation<T> implementation)
    {
        requireNonNull(javaType, "javaType is null");
        requireNonNull(implementation, "implementation is null");

        return new ObjectWriteFunction()
        {
            @Override
            public Class<T> getJavaType()
            {
                return javaType;
            }

            @Override
            @SuppressWarnings("unchecked")
            public void set(PreparedStatement statement, int index, Object value)
                    throws SQLException
            {
                implementation.set(statement, index, (T) value);
            }
        };
    }

    @FunctionalInterface
    interface ObjectWriteFunctionImplementation<T>
    {
        void set(PreparedStatement statement, int index, T value) throws SQLException;
    }
}
