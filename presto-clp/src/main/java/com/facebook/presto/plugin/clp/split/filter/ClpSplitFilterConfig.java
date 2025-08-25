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
package com.facebook.presto.plugin.clp.split.filter;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Options for a how a column in a Presto query should be pushed down into a query against CLP's
 * metadata database (during split pruning):
 * <ul>
 *   <li><b>{@code columnName}</b>: The column's name in the Presto query.</li>
 *
 *   <li><b>{@code customOptions}</b>: Options specific to the current
 *   {@link ClpSplitFilterProvider}.</li>
 *
 *   <li><b>{@code required}</b> (optional, defaults to {@code false}): Whether the filter must be
 *   present in the generated metadata query. If a required filter is missing or cannot be added to
 *   the metadata query, the original query will be rejected.</li>
 * </ul>
 */
public class ClpSplitFilterConfig
{
    @JsonProperty("columnName")
    public String columnName;

    @JsonProperty("customOptions")
    public CustomSplitFilterOptions customOptions;

    @JsonProperty("required")
    public boolean required;

    public interface CustomSplitFilterOptions
    {}
}
