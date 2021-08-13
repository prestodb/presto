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
package com.facebook.presto.functionNamespace.mysql;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.spi.function.RoutineCharacteristics;
import org.jdbi.v3.core.argument.AbstractArgumentFactory;
import org.jdbi.v3.core.argument.Argument;
import org.jdbi.v3.core.argument.ObjectArgument;
import org.jdbi.v3.core.config.ConfigRegistry;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static java.sql.Types.VARCHAR;

public class RoutineCharacteristicsArgumentFactory
        extends AbstractArgumentFactory<RoutineCharacteristics>
{
    private static final JsonCodec<RoutineCharacteristics> ROUTINE_CHARACTERISTICS_CODEC = jsonCodec(RoutineCharacteristics.class);

    public RoutineCharacteristicsArgumentFactory()
    {
        super(VARCHAR);
    }

    @Override
    protected Argument build(RoutineCharacteristics routineCharacteristics, ConfigRegistry config)
    {
        return new ObjectArgument(ROUTINE_CHARACTERISTICS_CODEC.toJson(routineCharacteristics), VARCHAR);
    }
}
