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
package com.facebook.presto.connector.meta;

import com.google.common.collect.ImmutableSet;

import static com.facebook.presto.connector.meta.ConnectorFeature.CREATE_SCHEMA;
import static com.facebook.presto.connector.meta.ConnectorFeature.CREATE_TABLE;
import static com.facebook.presto.connector.meta.ConnectorFeature.DROP_TABLE;

@SupportedFeatures({CREATE_SCHEMA, CREATE_TABLE, DROP_TABLE})
public class MissingChildFeaturesConnectorTest
        extends VerifyRun
        implements SubTest
{
    public MissingChildFeaturesConnectorTest()
    {
        super(ImmutableSet.of("baseTest"));
    }

    @Override
    public void createSchema()
    {
    }

    @Override
    public void createTable()
    {
    }

    @Override
    public void dropTable()
    {
    }
}
