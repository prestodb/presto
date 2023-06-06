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
package com.facebook.presto.spark.classloader_interface;

/*
 * Represent fatal exception during presto-on-spark execution which indicate Spark to fail over current executor
 * Here is the definition of scala fatal error Spark is relying on: https://www.scala-lang.org/api/2.13.3/scala/util/control/NonFatal$.html
 */
public class PrestoSparkFatalException
        extends VirtualMachineError
{
    public PrestoSparkFatalException(String errorMessage, Throwable cause)
    {
        super(errorMessage, cause);
    }
}
