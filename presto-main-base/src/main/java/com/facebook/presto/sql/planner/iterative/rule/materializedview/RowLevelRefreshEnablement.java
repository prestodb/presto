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
package com.facebook.presto.sql.planner.iterative.rule.materializedview;

import com.facebook.presto.Session;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.getMaterializedViewRowLevelIncrementalStrategy;

/**
 * Whether row-level incremental refresh may be considered for a materialized view.
 *
 * <p>Two controls decide it, and each is decisive in one direction only: a session strategy of
 * {@code NEVER} suppresses row-level whatever the view asks for, and a view that sets
 * {@code row_level_incremental_refresh = false} opts out whatever the session asks for. Either
 * switch can therefore turn row-level off, and both must permit it for it to be considered. A view
 * that sets nothing expresses no preference and leaves the choice to the session strategy.
 *
 * <p>Returning true means row-level is eligible, not that it will run: the plan must still be
 * safe to commit, and under {@code AUTOMATIC} the cost picker chooses among the candidates.
 */
final class RowLevelRefreshEnablement
{
    private RowLevelRefreshEnablement() {}

    static boolean isEnabled(Session session, Optional<Boolean> viewPreference)
    {
        if (getMaterializedViewRowLevelIncrementalStrategy(session) == MaterializedViewRewriteStrategy.NEVER) {
            return false;
        }
        return viewPreference.orElse(true);
    }
}
