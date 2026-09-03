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
package com.facebook.presto.resourceGroups;

import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.resourceGroups.SelectionContext;
import com.facebook.presto.spi.resourceGroups.SelectionCriteria;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class StaticSelector
        implements ResourceGroupSelector
{
    private static final Pattern NAMED_GROUPS_PATTERN = Pattern.compile("\\(\\?<([a-zA-Z][a-zA-Z0-9]*)>");
    private static final String USER_VARIABLE = "USER";
    private static final String SOURCE_VARIABLE = "SOURCE";

    private static final String SCHEMA_VARIABLE = "SCHEMA";
    private static final String EMPTY_CRITERIA_STRING = "";

    private final Optional<Pattern> userRegex;
    private final Optional<Pattern> sourceRegex;
    private final Set<String> clientTags;
    private final Optional<SelectorResourceEstimate> selectorResourceEstimate;
    private final Optional<String> queryType;
    private final Optional<Pattern> clientInfoRegex;

    private final Optional<String> schema;
    private final Optional<Pattern> principalRegex;
    private final ResourceGroupIdTemplate group;
    private final Set<String> variableNames;
    private final ImmutableMap<Pattern, ImmutableSet<String>> patternNamedGroups;

    public StaticSelector(
            Optional<Pattern> userRegex,
            Optional<Pattern> sourceRegex,
            Optional<List<String>> clientTags,
            Optional<SelectorResourceEstimate> selectorResourceEstimate,
            Optional<String> queryType,
            Optional<Pattern> clientInfoRegex,
            Optional<String> schema,
            Optional<Pattern> principalRegex,
            ResourceGroupIdTemplate group)
    {
        this.userRegex = requireNonNull(userRegex, "userRegex is null");
        this.sourceRegex = requireNonNull(sourceRegex, "sourceRegex is null");
        requireNonNull(clientTags, "clientTags is null");
        this.clientTags = ImmutableSet.copyOf(clientTags.orElse(ImmutableList.of()));
        this.selectorResourceEstimate = requireNonNull(selectorResourceEstimate, "selectorResourceEstimate is null");
        this.queryType = requireNonNull(queryType, "queryType is null");
        this.clientInfoRegex = requireNonNull(clientInfoRegex, "clientInfoRegex is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.principalRegex = requireNonNull(principalRegex, "principalRegex is null");
        this.group = requireNonNull(group, "group is null");

        HashSet<String> variableNames = new HashSet<>(ImmutableList.of(USER_VARIABLE, SOURCE_VARIABLE, SCHEMA_VARIABLE));
        userRegex.ifPresent(u -> addNamedGroups(u, variableNames));
        sourceRegex.ifPresent(s -> addNamedGroups(s, variableNames));
        this.variableNames = ImmutableSet.copyOf(variableNames);

        ImmutableMap.Builder<Pattern, ImmutableSet<String>> patternGroupsBuilder = ImmutableMap.builder();
        userRegex.ifPresent(u -> patternGroupsBuilder.put(u, extractNamedGroups(u)));
        sourceRegex.ifPresent(s -> patternGroupsBuilder.put(s, extractNamedGroups(s)));
        this.patternNamedGroups = patternGroupsBuilder.build();

        Set<String> unresolvedVariables = Sets.difference(group.getVariableNames(), variableNames);
        checkArgument(unresolvedVariables.isEmpty(), "unresolved variables %s in resource group ID '%s', available: %s\"", unresolvedVariables, group, variableNames);
    }

    @Override
    public Optional<SelectionContext<VariableMap>> match(SelectionCriteria criteria)
    {
        Map<String, String> variables = new HashMap<>();

        if (userRegex.isPresent()) {
            Matcher userMatcher = userRegex.get().matcher(criteria.getUser());
            if (!userMatcher.matches()) {
                return Optional.empty();
            }

            addVariableValues(userRegex.get(), criteria.getUser(), variables);
        }

        if (sourceRegex.isPresent()) {
            String source = criteria.getSource().orElse("");
            if (!sourceRegex.get().matcher(source).matches()) {
                return Optional.empty();
            }

            addVariableValues(sourceRegex.get(), source, variables);
        }

        if (principalRegex.isPresent()) {
            String principal = criteria.getPrincipal().orElse("");
            if (!principalRegex.get().matcher(principal).matches()) {
                return Optional.empty();
            }

            addVariableValues(principalRegex.get(), principal, variables);
        }

        if (!clientTags.isEmpty() && !criteria.getTags().containsAll(clientTags)) {
            return Optional.empty();
        }

        if (clientInfoRegex.isPresent() && !clientInfoRegex.get().matcher(criteria.getClientInfo().orElse(EMPTY_CRITERIA_STRING)).matches()) {
            return Optional.empty();
        }
        if (selectorResourceEstimate.isPresent() && !selectorResourceEstimate.get().match(criteria.getResourceEstimates())) {
            return Optional.empty();
        }

        if (queryType.isPresent()) {
            String contextQueryType = criteria.getQueryType().orElse(EMPTY_CRITERIA_STRING);
            if (!queryType.get().equalsIgnoreCase(contextQueryType)) {
                return Optional.empty();
            }
        }

        if (schema.isPresent() && criteria.getSchema().isPresent()) {
            if (criteria.getSchema().get().compareToIgnoreCase(schema.get()) != 0) {
                return Optional.empty();
            }
        }

        variables.putIfAbsent(USER_VARIABLE, criteria.getUser());

        // Special handling for source, which is an optional field that is part of the standard variables
        variables.putIfAbsent(SOURCE_VARIABLE, criteria.getSource().orElse(""));

        variables.putIfAbsent(SCHEMA_VARIABLE, criteria.getSchema().orElse(""));

        VariableMap map = new VariableMap(variables);
        ResourceGroupId id = group.expandTemplate(map);
        OptionalInt firstDynamicSegment = group.getFirstDynamicSegment();

        return Optional.of(new SelectionContext<>(id, map, firstDynamicSegment));
    }

    private static void addNamedGroups(Pattern pattern, HashSet<String> variables)
    {
        Matcher matcher = NAMED_GROUPS_PATTERN.matcher(pattern.toString());
        while (matcher.find()) {
            String name = matcher.group(1);
            checkArgument(!variables.contains(name), "Multiple definitions found for variable ${" + name + "}");
            variables.add(name);
        }
    }

    private static ImmutableSet<String> extractNamedGroups(Pattern pattern)
    {
        ImmutableSet.Builder<String> groups = ImmutableSet.builder();
        Matcher matcher = NAMED_GROUPS_PATTERN.matcher(pattern.pattern());
        while (matcher.find()) {
            groups.add(matcher.group(1));
        }
        return groups.build();
    }

    private void addVariableValues(Pattern pattern, String candidate, Map<String, String> mapping)
    {
        Set<String> groups = patternNamedGroups.getOrDefault(pattern, ImmutableSet.of());
        if (groups.isEmpty()) {
            return;
        }
        Matcher keyMatcher = pattern.matcher(candidate);
        if (keyMatcher.find()) {
            for (String key : groups) {
                String value = keyMatcher.group(key);
                if (value != null) {
                    mapping.put(key, value);
                }
            }
        }
    }

    @VisibleForTesting
    public Optional<Pattern> getUserRegex()
    {
        return userRegex;
    }
}
