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
package com.facebook.presto.execution;

import com.facebook.presto.Session;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.resourceGroups.ResourceGroupQueryLimits;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.util.Optional;

public interface ManagedQueryExecution
{
    void startWaitingForPrerequisites();

    void startWaitingForResources();

    void fail(Throwable cause);

    /**
     * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
     * be taken to avoid leaking {@code this} when adding a listener in a constructor. Additionally, it is
     * possible notifications are observed out of order due to the asynchronous execution.
     */
    void addStateChangeListener(StateChangeListener<QueryState> stateChangeListener);

    Session getSession();

    DataSize getUserMemoryReservation();

    DataSize getTotalMemoryReservation();

    Duration getTotalCpuTime();

    BasicQueryInfo getBasicQueryInfo();

    void setResourceGroupQueryLimits(ResourceGroupQueryLimits resourceGroupQueryLimits);

    boolean isDone();

    /**
     * @return Returns non-empty value iff error has occurred and query failed state is visible.
     */
    Optional<ErrorCode> getErrorCode();

    boolean isRetry();
}
