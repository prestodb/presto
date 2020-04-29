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
package com.facebook.presto.functionNamespace.testing;

import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.function.QualifiedFunctionName;
import com.facebook.presto.spi.function.RoutineCharacteristics;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.function.SqlParameter;
import com.google.common.collect.ImmutableList;

import java.util.Optional;

import static com.facebook.presto.spi.function.RoutineCharacteristics.Determinism.DETERMINISTIC;
import static com.facebook.presto.spi.function.RoutineCharacteristics.NullCallClause.RETURNS_NULL_ON_NULL_INPUT;
import static com.facebook.presto.spi.type.StandardTypes.DOUBLE;
import static com.facebook.presto.spi.type.StandardTypes.INTEGER;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;

public class SqlInvokedFunctionTestUtils
{
    private SqlInvokedFunctionTestUtils()
    {
    }

    public static final String TEST_CATALOG = "unittest";
    public static final String TEST_SCHEMA = "memory";

    public static final QualifiedFunctionName POWER_TOWER = QualifiedFunctionName.of(new CatalogSchemaName(TEST_CATALOG, TEST_SCHEMA), "power_tower");
    public static final QualifiedFunctionName TANGENT = QualifiedFunctionName.of(new CatalogSchemaName(TEST_CATALOG, TEST_SCHEMA), "tangent");

    public static final SqlInvokedFunction FUNCTION_POWER_TOWER_DOUBLE = new SqlInvokedFunction(
            POWER_TOWER,
            ImmutableList.of(new SqlParameter("x", parseTypeSignature(DOUBLE))),
            parseTypeSignature(DOUBLE),
            "power tower",
            RoutineCharacteristics.builder().setDeterminism(DETERMINISTIC).build(),
            "RETURN pow(x, x)",
            Optional.empty());

    public static final SqlInvokedFunction FUNCTION_POWER_TOWER_DOUBLE_UPDATED = new SqlInvokedFunction(
            POWER_TOWER,
            ImmutableList.of(new SqlParameter("x", parseTypeSignature(DOUBLE))),
            parseTypeSignature(DOUBLE),
            "power tower",
            RoutineCharacteristics.builder().setDeterminism(DETERMINISTIC).setNullCallClause(RETURNS_NULL_ON_NULL_INPUT).build(),
            "RETURN pow(x, x)",
            Optional.empty());

    public static final SqlInvokedFunction FUNCTION_POWER_TOWER_INT = new SqlInvokedFunction(
            POWER_TOWER,
            ImmutableList.of(new SqlParameter("x", parseTypeSignature(INTEGER))),
            parseTypeSignature(INTEGER),
            "power tower",
            RoutineCharacteristics.builder().setDeterminism(DETERMINISTIC).setNullCallClause(RETURNS_NULL_ON_NULL_INPUT).build(),
            "RETURN pow(x, x)",
            Optional.empty());

    public static final SqlInvokedFunction FUNCTION_TANGENT = new SqlInvokedFunction(
            TANGENT,
            ImmutableList.of(new SqlParameter("x", parseTypeSignature(DOUBLE))),
            parseTypeSignature(DOUBLE),
            "tangent",
            RoutineCharacteristics.builder()
                    .setDeterminism(DETERMINISTIC)
                    .setNullCallClause(RETURNS_NULL_ON_NULL_INPUT)
                    .build(),
            "RETURN sin(x) / cos(x)",
            Optional.empty());
}
