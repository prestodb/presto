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
package com.facebook.presto.scalar.sql;

import com.facebook.presto.spi.Plugin;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class SqlInvokedFunctionsPlugin
        implements Plugin
{
    @Override
    public Set<Class<?>> getSqlInvokedFunctions()
    {
        return ImmutableSet.<Class<?>>builder()
                .add(ArraySqlFunctions.class)
                .add(MapNormalizeFunction.class)
                .add(MapSqlFunctions.class)
                .add(SimpleSamplingPercent.class)
                .add(StringSqlFunctions.class)
                .add(ArrayIntersectFunction.class)
                .build();
    }
}
