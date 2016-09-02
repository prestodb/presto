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

package com.facebook.presto.tests;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class QueryTemplate
{
    private final String queryTemplate;
    private final List<Parameter> defaultParameters;

    public QueryTemplate(String queryTemplate, Parameter... parameters)
    {
        for (Parameter parameter : parameters) {
            String queryParameterKey = asQueryParameterKey(parameter.getKey());
            checkArgument(
                    queryTemplate.contains(queryParameterKey),
                    "Query template does not contain: %s", queryParameterKey);
        }

        this.queryTemplate = queryTemplate;
        this.defaultParameters = ImmutableList.copyOf(parameters);
    }

    public String replace(Parameter... parameters)
    {
        String query = queryTemplate;
        for (Parameter parameter : parameters) {
            query = resolve(query, parameter);
        }
        for (Parameter parameter : defaultParameters) {
            query = resolve(query, parameter);
        }

        for (Parameter parameter : defaultParameters) {
            String queryParameterKey = asQueryParameterKey(parameter.getKey());
            checkArgument(
                    !query.contains(queryParameterKey),
                    "Query template parameters was not given: %s", queryParameterKey);
        }

        return query;
    }

    private String resolve(String query, Parameter parameter)
    {
        return parameter.getValue()
                .map(value -> query.replaceAll(asQueryParameterKey(parameter.getKey()), value))
                .orElse(query);
    }

    private String asQueryParameterKey(String key)
    {
        return "%" + key + "%";
    }

    public static final class Parameter
    {
        private final String key;
        private final Optional<String> value;

        public Parameter(String key)
        {
            this(key, Optional.empty());
        }

        public Parameter(String key, String value)
        {
            this(key, Optional.of(value));
        }

        private Parameter(String key, Optional<String> value)
        {
            this.key = requireNonNull(key, "key is null");
            this.value = requireNonNull(value, "defaultValue is null");
        }

        public Optional<String> getValue()
        {
            return value;
        }

        public String getKey()
        {
            return key;
        }

        public Parameter of(String value)
        {
            return new Parameter(key, value);
        }
    }
}
