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
package com.facebook.plugin.arrow;

import java.util.Optional;
public class TestArrowFlightRequest
        extends ArrowAbstractFlightRequest
{
    public TestArrowFlightRequest(String schema)
    {
        super(schema);
    }

    public TestArrowFlightRequest(String schema, String table)
    {
        super(schema, table);
    }

    public TestArrowFlightRequest(String schema, String table, String query)
    {
        super(schema, table, Optional.of(query));
    }

    @Override
    public byte[] getCommand()
    {
        return new byte[0];
    }
}
