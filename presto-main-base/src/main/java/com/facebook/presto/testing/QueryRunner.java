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
package com.facebook.presto.testing;

import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.CoordinatorPlugin;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.split.PageSourceManager;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.expressions.ExpressionOptimizerManager;
import com.facebook.presto.sql.planner.ConnectorPlanOptimizerManager;
import com.facebook.presto.sql.planner.NodePartitioningManager;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.sanity.PlanCheckerProviderManager;
import com.facebook.presto.transaction.TransactionManager;
import org.intellij.lang.annotations.Language;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.Lock;

public interface QueryRunner
        extends ExpectedQueryRunner
{
    int getNodeCount();

    Session getDefaultSession();

    TransactionManager getTransactionManager();

    Metadata getMetadata();

    SplitManager getSplitManager();

    PageSourceManager getPageSourceManager();

    NodePartitioningManager getNodePartitioningManager();

    ConnectorPlanOptimizerManager getPlanOptimizerManager();

    PlanCheckerProviderManager getPlanCheckerProviderManager();

    StatsCalculator getStatsCalculator();

    @Deprecated
    default Optional<EventListener> getEventListener()
    {
        if (getEventListeners().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(getEventListeners().get(0));
    }

    List<EventListener> getEventListeners();

    TestingAccessControlManager getAccessControl();

    ExpressionOptimizerManager getExpressionManager();

    MaterializedResult execute(@Language("SQL") String sql);

    MaterializedResult execute(Session session, @Language("SQL") String sql);

    default MaterializedResult execute(Session session, @Language("SQL") String sql, List<? extends Type> resultTypes)
    {
        return this.execute(session, sql);
    }

    default MaterializedResultWithPlan executeWithPlan(Session session, @Language("SQL") String sql, WarningCollector warningCollector)
    {
        throw new UnsupportedOperationException();
    }

    default Plan createPlan(Session session, @Language("SQL") String sql, WarningCollector warningCollector)
    {
        throw new UnsupportedOperationException();
    }

    List<QualifiedObjectName> listTables(Session session, String catalog, String schema);

    boolean tableExists(Session session, String table);

    void installPlugin(Plugin plugin);

    default void installCoordinatorPlugin(CoordinatorPlugin plugin)
    {
        throw new UnsupportedOperationException();
    }

    void createCatalog(String catalogName, String connectorName, Map<String, String> properties);

    void loadFunctionNamespaceManager(String functionNamespaceManagerName, String catalogName, Map<String, String> properties);

    default void loadSessionPropertyProvider(String sessionPropertyProviderName, Map<String, String> properties)
    {
        throw new UnsupportedOperationException();
    }

    default void loadTVFProvider(String tvfProviderName)
    {
        throw new UnsupportedOperationException();
    }
    Lock getExclusiveLock();

    default void loadTypeManager(String typeManagerName)
    {
        throw new UnsupportedOperationException();
    }

    default void loadPlanCheckerProviderManager(String planCheckerProviderName, Map<String, String> properties)
    {
        throw new UnsupportedOperationException();
    }

    default void triggerConflictCheckWithBuiltInFunctions()
    {
        throw new UnsupportedOperationException();
    }

    class MaterializedResultWithPlan
    {
        private final MaterializedResult materializedResult;
        private final Plan queryPlan;

        public MaterializedResultWithPlan(MaterializedResult materializedResult, Plan queryPlan)
        {
            this.materializedResult = materializedResult;
            this.queryPlan = queryPlan;
        }

        public MaterializedResult getMaterializedResult()
        {
            return materializedResult;
        }

        public Plan getQueryPlan()
        {
            return queryPlan;
        }
    }
}
