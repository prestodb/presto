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
package com.facebook.presto.spi.function;

public enum OperatorType
{
    ADD("+"),
    SUBTRACT("-"),
    MULTIPLY("*"),
    DIVIDE("/"),
    MODULUS("%"),
    NEGATION("-"),
    EQUAL("="),
    NOT_EQUAL("<>"),
    LESS_THAN("<"),
    LESS_THAN_OR_EQUAL("<="),
    GREATER_THAN(">"),
    GREATER_THAN_OR_EQUAL(">="),
    BETWEEN("BETWEEN"),
    CAST("CAST"),
    SUBSCRIPT("[]"),
    HASH_CODE("HASH CODE"),
    SATURATED_FLOOR_CAST("SATURATED FLOOR CAST"),
    IS_DISTINCT_FROM("IS DISTINCT FROM");

    private final String operator;

    OperatorType(String operator)
    {
        this.operator = operator;
    }

    public String getOperator()
    {
        return operator;
    }
}
