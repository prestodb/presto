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
package com.facebook.presto.spi.predicate;

public interface TupleExpressionVisitor<R, C>
{
    R visitDomainExpression(DomainExpression expression, C context);

    R visitAndExpression(AndExpression expression, C context);

    R visitOrExpression(OrExpression expression, C context);

    R visitNotExpression(NotExpression expression, C context);

    R visitAllExpression(AllExpression expression, C context);

    R visitNoneExpression(NoneExpression expression, C context);
}
