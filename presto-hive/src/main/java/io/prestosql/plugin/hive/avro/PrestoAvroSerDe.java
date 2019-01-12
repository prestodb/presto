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
package io.prestosql.plugin.hive.avro;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.avro.AvroSerDe;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeException;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;

import java.io.IOException;
import java.util.Properties;

public class PrestoAvroSerDe
        extends AvroSerDe
{
    @Override
    public Schema determineSchemaOrReturnErrorSchema(Configuration conf, Properties props)
    {
        // AvroSerDe does not propagate initialization exceptions. Instead, it stores just an exception's message in
        // this.configErrors (see https://issues.apache.org/jira/browse/HIVE-7868). In Presto, such behavior is not
        // at all useful, as silenced exception usually carries important information which may be otherwise unavailable.
        try {
            return AvroSerdeUtils.determineSchemaOrThrowException(conf, props);
        }
        catch (IOException | AvroSerdeException e) {
            throw new RuntimeException(e);
        }
    }
}
