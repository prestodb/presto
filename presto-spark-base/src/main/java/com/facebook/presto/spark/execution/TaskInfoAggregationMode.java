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
package com.facebook.presto.spark.execution;

public enum TaskInfoAggregationMode
{
    /**
     * Decode and deduplicate all task infos when the query completes.
     */
    LEGACY,
    /**
     * Drain, deduplicate and fold task infos on a background thread while the query runs. Requires Spark 3.2 or later.
     */
    INCREMENTAL
}
