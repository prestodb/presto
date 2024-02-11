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
package com.facebook.presto.cost;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.statistics.EmptyPlanStatisticsProvider;
import com.facebook.presto.spi.statistics.HistoryBasedPlanStatisticsProvider;
import com.facebook.presto.sql.planner.CachingPlanCanonicalInfoProvider;
import com.facebook.presto.sql.planner.PlanCanonicalInfoProvider;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Inject;

import java.io.IOException;

import static com.facebook.presto.sql.Serialization.VARIABLE_TYPE_CLOSE_BRACKET;
import static com.facebook.presto.sql.Serialization.VARIABLE_TYPE_OPEN_BRACKET;
import static com.facebook.presto.sql.planner.CanonicalPlanGenerator.CANONICAL_DELIMITTER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HistoryBasedPlanStatisticsManager
{
    private final SessionPropertyManager sessionPropertyManager;
    private final HistoryBasedStatisticsCacheManager historyBasedStatisticsCacheManager;
    private final PlanCanonicalInfoProvider planCanonicalInfoProvider;
    private final HistoryBasedOptimizationConfig config;

    private HistoryBasedPlanStatisticsProvider historyBasedPlanStatisticsProvider = EmptyPlanStatisticsProvider.getInstance();
    private boolean statisticsProviderAdded;

    @Inject
    public HistoryBasedPlanStatisticsManager(ObjectMapper objectMapper, SessionPropertyManager sessionPropertyManager, Metadata metadata, HistoryBasedOptimizationConfig config)
    {
        requireNonNull(objectMapper, "objectMapper is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.historyBasedStatisticsCacheManager = new HistoryBasedStatisticsCacheManager();
        ObjectMapper newObjectMapper =
                objectMapper.copy().configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
        SimpleModule module = new SimpleModule();
        module.addKeySerializer(VariableReferenceExpression.class, new CanonicalPlanNodeVariableSerializer());
        newObjectMapper.registerModule(module);
        this.planCanonicalInfoProvider = new CachingPlanCanonicalInfoProvider(historyBasedStatisticsCacheManager, newObjectMapper, metadata);
        this.config = requireNonNull(config, "config is null");
    }

    public class CanonicalPlanNodeVariableSerializer
            extends JsonSerializer<VariableReferenceExpression>
    {
        @Override
        public void serialize(VariableReferenceExpression value, JsonGenerator gen, SerializerProvider serializers)
                throws IOException
        {
// Get the name from the value
            String canonicalName = value.getName();
            // Check if the name contains an underscore and modify it if necessary, this is needed to canonicalize plan hashes
            if (canonicalName.contains(CANONICAL_DELIMITTER)) {
                canonicalName = canonicalName.substring(0, canonicalName.indexOf(CANONICAL_DELIMITTER) - CANONICAL_DELIMITTER.length());
            }
//            super.serialize(value, gen, serializers);
            gen.writeFieldName(format("%s%s%s%s", canonicalName, VARIABLE_TYPE_OPEN_BRACKET, value.getType().getTypeSignature(), VARIABLE_TYPE_CLOSE_BRACKET));
        }
    }

    public void addHistoryBasedPlanStatisticsProviderFactory(HistoryBasedPlanStatisticsProvider historyBasedPlanStatisticsProvider)
    {
        if (statisticsProviderAdded) {
            throw new IllegalStateException("historyBasedPlanStatisticsProvider can only be set once");
        }
        this.historyBasedPlanStatisticsProvider = historyBasedPlanStatisticsProvider;
        statisticsProviderAdded = true;
    }

    public HistoryBasedPlanStatisticsCalculator getHistoryBasedPlanStatisticsCalculator(StatsCalculator delegate)
    {
        return new HistoryBasedPlanStatisticsCalculator(() -> historyBasedPlanStatisticsProvider, historyBasedStatisticsCacheManager, delegate, planCanonicalInfoProvider, config);
    }

    public HistoryBasedPlanStatisticsTracker getHistoryBasedPlanStatisticsTracker()
    {
        return new HistoryBasedPlanStatisticsTracker(() -> historyBasedPlanStatisticsProvider, historyBasedStatisticsCacheManager, sessionPropertyManager, config);
    }

    public PlanCanonicalInfoProvider getPlanCanonicalInfoProvider()
    {
        return planCanonicalInfoProvider;
    }
}
