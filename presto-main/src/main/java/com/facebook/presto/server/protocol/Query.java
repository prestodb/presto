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
package com.facebook.presto.server.protocol;

import com.facebook.presto.client.QueryResults;
import com.facebook.presto.dispatcher.CoordinatorLocation;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.security.SelectedRole;
import com.facebook.presto.transaction.TransactionId;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.ws.rs.core.UriInfo;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface Query
{
    void cancel();

    QueryId getQueryId();

    boolean isSlugValid(String slug);

    Optional<String> getSetCatalog();

    Optional<String> getSetSchema();

    Map<String, String> getSetSessionProperties();

    Set<String> getResetSessionProperties();

    Map<String, SelectedRole> getSetRoles();

    Map<String, String> getAddedPreparedStatements();

    Set<String> getDeallocatedPreparedStatements();

    Optional<TransactionId> getStartedTransactionId();

    boolean isClearTransactionId();

    ListenableFuture<QueryResults> waitForResults(
            long token,
            UriInfo uriInfo,
            String scheme,
            Optional<CoordinatorLocation> nextCoordinatorLocation,
            Duration wait,
            DataSize targetResultSize);
}
