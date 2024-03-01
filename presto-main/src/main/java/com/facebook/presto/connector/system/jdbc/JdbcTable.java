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
package com.facebook.presto.connector.system.jdbc;

import com.facebook.presto.spi.SystemTable;

import static com.facebook.presto.spi.SystemTable.Distribution.SINGLE_COORDINATOR;

abstract class JdbcTable
        implements SystemTable
{
    @Override
    public final Distribution getDistribution()
    {
        return SINGLE_COORDINATOR;
    }
}
