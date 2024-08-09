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
package com.facebook.presto.session.sql.expressions;

import com.facebook.presto.spi.RowExpressionSerde;
import com.facebook.presto.spi.relation.RowExpression;

class UnimplementedRowExpressionSerde
        implements RowExpressionSerde
{
    @Override
    public String serialize(RowExpression expression)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public RowExpression deserialize(String value)
    {
        throw new UnsupportedOperationException();
    }
}
