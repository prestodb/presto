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
package com.facebook.presto.plugin.clickhouse;

public class ClickhouseDXLKeyWords
{
    private ClickhouseDXLKeyWords()
    {
    }

    public static final String ORDER_BY_PROPERTY = "order_by"; //required
    public static final String PARTITION_BY_PROPERTY = "partition_by"; //optional
    public static final String PRIMARY_KEY_PROPERTY = "primary_key"; //optional
    public static final String SAMPLE_BY_PROPERTY = "sample_by"; //optional
}
