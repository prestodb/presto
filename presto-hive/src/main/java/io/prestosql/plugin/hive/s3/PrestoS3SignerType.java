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
package io.prestosql.plugin.hive.s3;

// These are the exact names used by SignerFactory in the AWS library
// and thus cannot be renamed or use the normal naming convention.
@SuppressWarnings("EnumeratedConstantNamingConvention")
public enum PrestoS3SignerType
{
    S3SignerType,
    AWS3SignerType,
    AWS4SignerType,
    AWSS3V4SignerType,
    CloudFrontSignerType,
    QueryStringSignerType,
}
