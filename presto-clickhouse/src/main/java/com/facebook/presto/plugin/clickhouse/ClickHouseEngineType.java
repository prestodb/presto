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
package com.facebook.presto.plugin.clickhouse;

public enum ClickHouseEngineType
{
    STRIPELOG("StripeLog"),
    LOG("Log"),
    TINYLOG("TinyLog"),
    MERGETREE("MergeTree()"),
    VERSIONEDCOLLAPSINGMERGETREE("VersionedCollapsingMergeTree"),
    GRAPHITEMERGETREE("GraphiteMergeTree"),
    AGGREGATINGMERGETREE("AggregatingMergeTree"),
    COLLAPSINGMERGETREE("CollapsingMergeTree"),
    REPLACINGMERGETREE("ReplacingMergeTree"),
    SUMMINGMERGETREE("SummingMergeTree"),
    REPLICATEDMERGETREE("ReplicatedMergeTree"),
    REPLICATEDVERSIONEDCOLLAPSINGMERGETREE("ReplicatedVersionedCollapsingMergeTree"),
    REPLICATEDGRAPHITEMERGETREE("ReplicatedGraphiteMergeTree"),
    REPLICATEDAGGREGATINGMERGETREE("ReplicatedAggregatingMergeTree"),
    REPLICATEDCOLLAPSINGMERGETREE("ReplicatedCollapsingMergeTree"),
    REPLICATEDREPLACINGMERGETREE("ReplicatedReplacingMergeTree"),
    REPLICATEDSUMMINGMERGETREE("ReplicatedSummingMergeTree");

    private final String engineType;

    ClickHouseEngineType(String engineType)
    {
        this.engineType = engineType;
    }

    public String getEngineType()
    {
        return this.engineType;
    }
}
