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

import com.facebook.presto.spi.ConnectorSession;

/**
 * Defines a tree of expressions. It can be of two types:
 * 1) Unary Expression - which denotes a leaf in the expression tree
 * 2) Binary Expression - which denotes two operands and an AND/OR Operator (Not implemented)
 */
public interface SpiExpression
{
    public String toString(ConnectorSession session);
}
