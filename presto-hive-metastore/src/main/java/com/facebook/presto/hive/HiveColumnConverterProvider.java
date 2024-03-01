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
package com.facebook.presto.hive;

import com.facebook.presto.hive.metastore.HiveColumnConverter;

import javax.inject.Inject;

public class HiveColumnConverterProvider
        implements ColumnConverterProvider
{
    public static final HiveColumnConverterProvider DEFAULT_COLUMN_CONVERTER_PROVIDER = new HiveColumnConverterProvider();
    public static final HiveColumnConverter DEFAULT_COLUMN_CONVERTER = new HiveColumnConverter();

    @Inject
    public HiveColumnConverterProvider()
    {
    }

    @Override
    public ColumnConverter getColumnConverter()
    {
        return DEFAULT_COLUMN_CONVERTER;
    }
}
