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
package com.facebook.presto.rewriter.util;

public class ConfigConstants
{
    private ConfigConstants()
    {
    }

    public static final String WXD_OPTIMIZER_SHOW_QUERY = "wxd-optimizer-show-query";
    public static final String ENABLE_QUERY_OPTIMIZER_MATERIALIZED_QUERY_TABLE = "enable-query-optimizer-materialized-view";
    public static final String IS_QUERY_REWRITER_PLUGIN_ENABLED = "is_query_rewriter_plugin_enabled";
    public static final String IS_QUERY_REWRITER_PLUGIN_SUCCEEDED = "is_query_rewriter_plugin_succeeded";
    public static final String REORDER_JOINS = "reorder_joins";
}
