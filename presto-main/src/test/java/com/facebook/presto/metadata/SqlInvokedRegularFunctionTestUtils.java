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
package com.facebook.presto.metadata;

import com.facebook.presto.spi.function.QualifiedFunctionName;
import com.facebook.presto.sqlfunction.RoutineCharacteristics;
import com.facebook.presto.sqlfunction.SqlInvokedRegularFunction;
import com.facebook.presto.sqlfunction.SqlParameter;
import com.google.common.collect.ImmutableList;

import java.util.Optional;

import static com.facebook.presto.spi.type.StandardTypes.DOUBLE;
import static com.facebook.presto.spi.type.StandardTypes.INTEGER;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.sqlfunction.RoutineCharacteristics.Determinism.DETERMINISTIC;
import static com.facebook.presto.sqlfunction.RoutineCharacteristics.Language.SQL;
import static com.facebook.presto.sqlfunction.RoutineCharacteristics.NullCallClause.CALLED_ON_NULL_INPUT;
import static com.facebook.presto.sqlfunction.RoutineCharacteristics.NullCallClause.RETURNS_NULL_ON_NULL_INPUT;

public class SqlInvokedRegularFunctionTestUtils
{
    private SqlInvokedRegularFunctionTestUtils()
    {
    }

    public static final QualifiedFunctionName POWER_TOWER = QualifiedFunctionName.of("unittest.memory.power_tower");

    public static final SqlInvokedRegularFunction FUNCTION_POWER_TOWER_DOUBLE = new SqlInvokedRegularFunction(
            POWER_TOWER,
            ImmutableList.of(new SqlParameter("x", parseTypeSignature(DOUBLE))),
            parseTypeSignature(DOUBLE),
            "power tower",
            new RoutineCharacteristics(SQL, DETERMINISTIC, CALLED_ON_NULL_INPUT),
            "pow(x, x)",
            Optional.empty());

    public static final SqlInvokedRegularFunction FUNCTION_POWER_TOWER_DOUBLE_UPDATED = new SqlInvokedRegularFunction(
            POWER_TOWER,
            ImmutableList.of(new SqlParameter("x", parseTypeSignature(DOUBLE))),
            parseTypeSignature(DOUBLE),
            "power tower",
            new RoutineCharacteristics(SQL, DETERMINISTIC, RETURNS_NULL_ON_NULL_INPUT),
            "pow(x, x)",
            Optional.empty());

    public static final SqlInvokedRegularFunction FUNCTION_POWER_TOWER_INT = new SqlInvokedRegularFunction(
            POWER_TOWER,
            ImmutableList.of(new SqlParameter("x", parseTypeSignature(INTEGER))),
            parseTypeSignature(INTEGER),
            "power tower",
            new RoutineCharacteristics(SQL, DETERMINISTIC, RETURNS_NULL_ON_NULL_INPUT),
            "pow(x, x)",
            Optional.empty());
}
