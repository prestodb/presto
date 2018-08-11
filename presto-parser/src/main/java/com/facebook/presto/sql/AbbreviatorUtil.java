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
import com.facebook.presto.sql.tree.OrderBy;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TableSubquery;
import com.facebook.presto.sql.tree.TransactionMode;
import com.facebook.presto.sql.tree.Use;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AbbreviatorUtil
{
    private static List<Class> blackList = new ArrayList<>(Arrays.asList(
            Use.class,
            OrderBy.class,
            Table.class,
            TableSubquery.class));

    private static List<Class> whiteList = new ArrayList<>(Arrays.asList(
            Expression.class,
            OrderBy.class,
            Relation.class,
            CallArgument.class,
            TransactionMode.class,
            Select.class,
            Statement.class,
            SelectItem.class));

    private AbbreviatorUtil()
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
