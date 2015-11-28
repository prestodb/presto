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
package com.facebook.presto.raptor.util;

import com.facebook.presto.spi.PrestoException;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.exceptions.DBIException;

import java.lang.reflect.InvocationTargetException;

import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_METADATA_ERROR;
import static com.google.common.base.Throwables.propagateIfInstanceOf;
import static com.google.common.reflect.Reflection.newProxy;

public final class DatabaseUtil
{
    private DatabaseUtil() {}

    public static <T> T onDemandDao(IDBI dbi, Class<T> daoType)
    {
        return newProxy(daoType, (proxy, method, args) -> {
            try (Handle handle = dbi.open()) {
                T dao = handle.attach(daoType);
                return method.invoke(dao, args);
            }
            catch (DBIException e) {
                throw metadataError(e);
            }
            catch (InvocationTargetException e) {
                throw metadataError(e.getCause());
            }
        });
    }

    public static <T> T runTransaction(IDBI dbi, TransactionCallback<T> callback)
    {
        try {
            return dbi.inTransaction(callback);
        }
        catch (DBIException e) {
            propagateIfInstanceOf(e.getCause(), PrestoException.class);
            throw metadataError(e);
        }
    }

    public static PrestoException metadataError(Throwable cause)
    {
        return new PrestoException(RAPTOR_METADATA_ERROR, "Failed to perform metadata operation", cause);
    }
}
