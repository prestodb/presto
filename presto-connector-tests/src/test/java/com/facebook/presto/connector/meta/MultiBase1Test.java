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

import static com.facebook.presto.connector.meta.ConnectorFeature.CREATE_SCHEMA;
import static com.facebook.presto.connector.meta.ConnectorFeature.DROP_SCHEMA;
import static org.junit.jupiter.api.Assertions.fail;

@RequiredFeatures({CREATE_SCHEMA, DROP_SCHEMA})
public interface MultiBase1Test
        extends BaseSPITest
{
    default void dropSchema()
    {
        fail("Unsupported dropSchema");
    }
}
