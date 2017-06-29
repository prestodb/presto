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
package com.facebook.presto.plugin.turbonium.config.db;

import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.exceptions.DBIException;

import java.lang.reflect.InvocationTargetException;

import static com.google.common.reflect.Reflection.newProxy;
import static java.util.Objects.requireNonNull;

public class DatabaseUtil
{
    // Partial copy from raptor: com.facebook.presto.raptor.util.DatabaseUtil

    private DatabaseUtil() {}

    public static <T> T onDemandDao(IDBI dbi, Class<T> daoType)
    {
        requireNonNull(dbi, "dbi is null");
        return newProxy(daoType, (proxy, method, args) -> {
            try (Handle handle = dbi.open()) {
                T dao = handle.attach(daoType);
                return method.invoke(dao, args);
            }
            catch (DBIException e) {
                throw e;
            }
            catch (InvocationTargetException e) {
                throw e.getCause();
            }
        });
    }
}
