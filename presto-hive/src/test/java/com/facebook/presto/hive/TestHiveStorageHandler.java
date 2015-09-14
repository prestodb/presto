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

import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;

public class TestHiveStorageHandler
        extends DefaultStorageHandler
{
    @Override
    public Class<? extends InputFormat> getInputFormatClass()
    {
        return TextInputFormat.class;
    }

    @Override
    public Class<? extends OutputFormat> getOutputFormatClass()
    {
        return HiveIgnoreKeyTextOutputFormat.class;
    }

    @Override
    public Class<? extends AbstractSerDe> getSerDeClass()
    {
        return LazySimpleSerDe.class;
    }
}
