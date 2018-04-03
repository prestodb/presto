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
package com.facebook.presto.spi.connector;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public interface ConnectorTableLayoutProvider
{
    List<ConnectorTableLayoutResult> provide(ConnectorSession session);

    default Optional<PredicatePushdown> getPredicatePushdown()
    {
        return Optional.empty();
    }

    default Optional<ProjectionPushdown> getProjectionPushdown()
    {
        return Optional.empty();
    }

    default Optional<LimitPushdown> getLimitPushdown()
    {
        return Optional.empty();
    }

    default Optional<JoinPushdown> getJoinPushdown()
    {
        return Optional.empty();
    }

    interface ProjectionPushdown
    {
        Optional<List<ColumnHandle>> getColumnHandles();

        void pushDownProjection(Set<ColumnHandle> columnHandles);
    }

    interface PredicatePushdown
    {
        void pushDownPredicate(Constraint<ColumnHandle> constraint);
    }

    interface LimitPushdown
    {
        void pushDownLimit(long limit);
    }

    interface JoinPushdown
    {
        void pushDownJoin(
                /*
                What should be here?
                 - how to map join types?
                 - how to map expressions from join filter?
                 - maybe only support equi inner joins?
                * */);
    }
}
