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
package com.facebook.presto.connector;

import com.facebook.presto.spi.connector.ConnectorOptimizationRule;
import com.facebook.presto.spi.connector.ConnectorRuleProvider;
import com.google.common.collect.ImmutableSet;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class ConnectorOptimizationRuleManager
{
    private final ConcurrentMap<ConnectorId, ConnectorRuleProvider> providers = new ConcurrentHashMap<>();

    public void addRuleProvider(ConnectorId connectorId, ConnectorRuleProvider ruleProvider)
    {
        requireNonNull(connectorId, "connectorId is null");
        requireNonNull(ruleProvider, "ruleProvider is null");
        checkState(providers.putIfAbsent(connectorId, ruleProvider) == null, "RuleProvider for connector '%s' is already registered", connectorId);
    }

    public void removeRuleProvider(ConnectorId connectorId)
    {
        providers.remove(connectorId);
    }

    public Set<ConnectorOptimizationRule> getRules(ConnectorId connectorId)
    {
        ConnectorRuleProvider provider = providers.get(connectorId);
        if (provider == null) {
            return ImmutableSet.of();
        }
        return provider.getRules();
    }
}
