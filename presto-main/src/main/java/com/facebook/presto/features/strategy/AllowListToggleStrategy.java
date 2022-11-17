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
package com.facebook.presto.features.strategy;

import com.facebook.presto.SessionRepresentation;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.features.FeatureConfiguration;
import com.facebook.presto.spi.features.FeatureToggleStrategyConfig;
import com.google.inject.Inject;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

/**
 * AllowListToggleStrategy accepts {@link QueryId} as parameter to evaluate if action is allowed based on strategy configuration parameters.
 * <p>
 * Strategy uses {@link QueryManager} to extract user and source from query.
 * Strategy configuration has two parameters:
 * <ul>
 *     <li>"allow-list-source" : regex pattern for matching query source</li>
 *     <li>"allow-list-user" : regex pattern for matching query user</li>
 * </ul>
 * <p>
 * If strategy configuration is not defined for current feature evaluation is skipped.
 * Strategy uses query manager to extract query user and query source for given QueryId.
 * Method then checks if user and source patters are configured, and then matches query user and source against given patterns.
 * If either of conditions are met, method will return true.
 */
public class AllowListToggleStrategy
        implements FeatureToggleStrategy
{
    public static final String ALLOW_LIST_SOURCE = "allow-list-source";
    public static final String ALLOW_LIST_USER = "allow-list-user";

    private final QueryManager queryManager;

    @Inject
    public AllowListToggleStrategy(QueryManager queryManager)
    {
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
    }

    /**
     * Evaluates strategy configuration parameters against given {@link QueryId} object
     *
     * @param featureConfiguration the feature configuration
     * @param queryId the {@link QueryId}
     * @return result of evaluation
     */
    @Override
    public boolean check(FeatureConfiguration featureConfiguration, Object queryId)
    {
        /*
         * If strategy configuration is not defined for current feature evaluation is skipped.
         */
        if (!featureConfiguration.getFeatureToggleStrategyConfig().isPresent()) {
            return true;
        }
        /*
         * If strategy configuration is defined for current feature evaluation, and is set to not active.
         */
        FeatureToggleStrategyConfig featureToggleStrategyConfig = featureConfiguration.getFeatureToggleStrategyConfig().get();
        if (!featureToggleStrategyConfig.active()) {
            return true;
        }
        SessionRepresentation session = queryManager.getQueryInfo((QueryId) queryId).getSession();
        String user = session.getUser();
        Optional<String> source = session.getSource();
        Optional<String> userPattern = featureToggleStrategyConfig.get(ALLOW_LIST_USER);
        Optional<String> sourcePattern = featureToggleStrategyConfig.get(ALLOW_LIST_SOURCE);
        AtomicBoolean allow = new AtomicBoolean(false);
        userPattern.ifPresent(p ->
                allow.set(allow.get() || Pattern.compile(p).matcher(user).matches()));
        sourcePattern.ifPresent(p ->
                source.ifPresent(s ->
                        allow.set(allow.get() || Pattern.compile(p).matcher(source.get()).matches())));
        return allow.get();
    }
}
