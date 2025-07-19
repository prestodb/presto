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
package com.facebook.presto.spark.util;

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.spark.PrestoSparkSessionProperties;
import com.facebook.presto.spiller.NodeSpillConfig;
import com.facebook.presto.sql.analyzer.JavaFeaturesConfig;
import com.google.common.collect.Streams;

import static com.facebook.presto.metadata.SessionPropertyManager.createTestingSessionPropertyManager;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class PrestoSparkTestSessionBuilder
{
    private PrestoSparkTestSessionBuilder() {}

    public static Session.SessionBuilder getPrestoSparkTestingSessionBuilder()
    {
        return testSessionBuilder(createTestingSessionPropertyManager(
                Streams.concat(
                        new SystemSessionProperties().getSessionProperties().stream(),
                        new PrestoSparkSessionProperties().getSessionProperties().stream()
                ).collect(toImmutableList()),
                new JavaFeaturesConfig(),
                new NodeSpillConfig()));
    }
}
