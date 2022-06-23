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
package com.facebook.presto.tests.tpcds;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Locale;

public enum TpcdsTableName
{
    CALL_CENTER,
    CATALOG_PAGE,
    CATALOG_RETURNS,
    CATALOG_SALES,
    CUSTOMER,
    CUSTOMER_ADDRESS,
    CUSTOMER_DEMOGRAPHICS,
    DATE_DIM,
    HOUSEHOLD_DEMOGRAPHICS,
    INCOME_BAND,
    INVENTORY,
    ITEM,
    PROMOTION,
    REASON,
    SHIP_MODE,
    STORE,
    STORE_RETURNS,
    STORE_SALES,
    TIME_DIM,
    WAREHOUSE,
    WEB_PAGE,
    WEB_RETURNS,
    WEB_SALES,
    WEB_SITE;

    public String getTableName()
    {
        return this.name().toLowerCase(Locale.ROOT);
    }

    public static List<TpcdsTableName> getBaseTables()
    {
        return ImmutableList.copyOf(values());
    }
}
