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
package com.facebook.presto.tests;

import com.teradata.tempto.Requirement;
import com.teradata.tempto.RequirementsProvider;
import com.teradata.tempto.configuration.Configuration;
import com.teradata.tempto.fulfillment.table.ImmutableTableRequirement;

import static com.teradata.tempto.Requirements.compose;
import static com.teradata.tempto.fulfillment.table.hive.tpch.TpchTableDefinitions.CUSTOMER;
import static com.teradata.tempto.fulfillment.table.hive.tpch.TpchTableDefinitions.LINE_ITEM;
import static com.teradata.tempto.fulfillment.table.hive.tpch.TpchTableDefinitions.NATION;
import static com.teradata.tempto.fulfillment.table.hive.tpch.TpchTableDefinitions.ORDERS;
import static com.teradata.tempto.fulfillment.table.hive.tpch.TpchTableDefinitions.PART;
import static com.teradata.tempto.fulfillment.table.hive.tpch.TpchTableDefinitions.PART_SUPPLIER;
import static com.teradata.tempto.fulfillment.table.hive.tpch.TpchTableDefinitions.REGION;
import static com.teradata.tempto.fulfillment.table.hive.tpch.TpchTableDefinitions.SUPPLIER;

public class ImmutableTpchTablesRequirements
        implements RequirementsProvider
{
    public static final Requirement PART_TABLE = new ImmutableTableRequirement(PART);
    public static final Requirement NATION_TABLE = new ImmutableTableRequirement(NATION);
    public static final Requirement REGION_TABLE = new ImmutableTableRequirement(REGION);
    public static final Requirement ORDERS_TABLE = new ImmutableTableRequirement(ORDERS);
    public static final Requirement SUPPLIER_TABLE = new ImmutableTableRequirement(SUPPLIER);
    public static final Requirement CUSTOMER_TABLE = new ImmutableTableRequirement(CUSTOMER);
    public static final Requirement LINE_ITEM_TABLE = new ImmutableTableRequirement(LINE_ITEM);
    public static final Requirement PART_SUPPLIER_TABLE = new ImmutableTableRequirement(PART_SUPPLIER);

    @Override
    public Requirement getRequirements(Configuration configuration)
    {
        return compose(
                PART_TABLE,
                NATION_TABLE,
                REGION_TABLE,
                ORDERS_TABLE,
                SUPPLIER_TABLE,
                CUSTOMER_TABLE,
                LINE_ITEM_TABLE,
                PART_SUPPLIER_TABLE
        );
    }

    public static class ImmutablePartTable
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return PART_TABLE;
        }
    }

    public static class ImmutableNationTable
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return NATION_TABLE;
        }
    }

    public static class ImmutableRegionTable
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return REGION_TABLE;
        }
    }

    public static class ImmutableOrdersTable
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return ORDERS_TABLE;
        }
    }

    public static class ImmutableSupplierTable
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return SUPPLIER_TABLE;
        }
    }

    public static class ImmutableCustomerTable
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return CUSTOMER_TABLE;
        }
    }

    public static class ImmutableLineItemTable
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return LINE_ITEM_TABLE;
        }
    }

    public static class ImmutablePartSupplierTable
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return PART_SUPPLIER_TABLE;
        }
    }
}
