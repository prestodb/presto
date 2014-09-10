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

import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;

import java.util.List;

public interface BenchmarkLineItem
{
    String getName();

    <K, V extends Writable> List<Object> all(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception;

    <K, V extends Writable> List<Object> allReadOne(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception;

    <K, V extends Writable> long comment(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception;

    <K, V extends Writable> long commitDate(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception;

    <K, V extends Writable> double discount(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception;

    <K, V extends Writable> double extendedPrice(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception;

    <K, V extends Writable> long lineNumber(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception;

    <K, V extends Writable> long orderKey(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception;

    <K, V extends Writable> long partKey(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception;

    <K, V extends Writable> long quantity(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception;

    <K, V extends Writable> long receiptDate(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception;

    <K, V extends Writable> long returnFlag(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception;

    <K, V extends Writable> long shipDate(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception;

    <K, V extends Writable> long shipInstructions(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception;

    <K, V extends Writable> long shipMode(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception;

    <K, V extends Writable> long status(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception;

    <K, V extends Writable> long supplierKey(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception;

    <K, V extends Writable> double tax(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception;

    <K, V extends Writable> List<Object> tpchQuery1(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception;

    <K, V extends Writable> List<Object> tpchQuery6(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception;
}
