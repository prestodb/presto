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
package io.prestosql.plugin.thrift.util;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.drift.TApplicationException;
import io.airlift.drift.TException;
import io.airlift.drift.protocol.TTransportException;
import io.prestosql.plugin.thrift.api.PrestoThriftServiceException;
import io.prestosql.spi.PrestoException;

import static com.google.common.util.concurrent.Futures.catchingAsync;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.prestosql.plugin.thrift.ThriftErrorCode.THRIFT_SERVICE_CONNECTION_ERROR;
import static io.prestosql.plugin.thrift.ThriftErrorCode.THRIFT_SERVICE_GENERIC_REMOTE_ERROR;
import static io.prestosql.plugin.thrift.ThriftErrorCode.THRIFT_SERVICE_NO_AVAILABLE_HOSTS;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public final class ThriftExceptions
{
    private ThriftExceptions() {}

    public static PrestoException toPrestoException(Exception e)
    {
        if ((e instanceof TTransportException) && "No hosts available".equals(e.getMessage())) {
            throw new PrestoException(THRIFT_SERVICE_NO_AVAILABLE_HOSTS, e);
        }
        if ((e instanceof TApplicationException) || (e instanceof PrestoThriftServiceException)) {
            return new PrestoException(THRIFT_SERVICE_GENERIC_REMOTE_ERROR, "Exception raised by remote Thrift server: " + e.getMessage(), e);
        }
        if (e instanceof TException) {
            return new PrestoException(THRIFT_SERVICE_CONNECTION_ERROR, "Error communicating with remote Thrift server", e);
        }
        throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
    }

    public static <T> ListenableFuture<T> catchingThriftException(ListenableFuture<T> future)
    {
        return catchingAsync(future, Exception.class, e -> immediateFailedFuture(toPrestoException(e)), directExecutor());
    }
}
