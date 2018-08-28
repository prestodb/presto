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
package com.facebook.presto.sql;

import com.facebook.presto.sql.tree.CallArgument;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TableSubquery;
import com.facebook.presto.sql.tree.TransactionMode;
import com.facebook.presto.sql.tree.Use;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class QueryAbridgerUtil
{
    private static List<Class> blackList = ImmutableList.of(
            Use.class,
            Table.class,
            TableSubquery.class);

    private static List<Class> whiteList = ImmutableList.of(
            Expression.class,
            Relation.class,
            CallArgument.class,
            TransactionMode.class,
            Select.class,
            Statement.class,
            SelectItem.class);

    private QueryAbridgerUtil()
    {
    }

    private static boolean isBlackListed(Node node)
    {
        for (Class classVal : blackList) {
            if (classVal.isInstance(node)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isWhiteListed(Node node)
    {
        for (Class classVal : whiteList) {
            if (classVal.isInstance(node)) {
                return true;
            }
        }
        return false;
    }

    /**
     * A node that is an instance of any of the subclasses of classes mentioned in the whitelist
     * is allowed to be pruned unless that subclass is present in the blacklist.
     *
     * A few examples for clarification:
     *
     * - Input: a 'Query' object
     * - Output: true
     * - Reason: Query class is a subclass of Statement class. Query is not blacklisted. Statement is whitelisted.
     *
     * - input: a 'Use' object
     * - Output: false
     * - Reason: Use class also is a subclass Statement class. But Use is blacklisted.
     *
     * - input: a 'GroupBy' object
     * - Output: false
     * - Reason: Groupby is not whitelisted.
     *
     * So basically, blacklisted classes are the ones that are
     * (1) extended from a whitelisted Superclass and
     * (2) can't be pruned because of their SqlFormatter implementation.
     *
     * If we didn't have a blacklist, we would need to whitelist all the allowed subclasses explicitly.
     * For example, instances of all subclasses of 'Statement', except for 'Use', need to be whitelisted.
     * We can do that by adding Statement.class to the whitelist and Use.class to blacklist.
     * If we didn't have a blacklist, we would need to add all the subclasses of Statement (except Use) to the whitelist manually.
     * @param node
     * @return boolean
     */
    public static boolean isAllowedToBePruned(Node node)
    {
        if (isBlackListed(node)) {
            return false;
        }
        if (isWhiteListed(node)) {
            return true;
        }
        return false;
    }
}
