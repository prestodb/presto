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
package com.facebook.presto.hive.s3;

import org.apache.hadoop.conf.Configuration;

public interface S3ConfigurationUpdater
{
    String S3_USER_AGENT_SUFFIX = "presto";
    String S3_USER_AGENT_PREFIX = "presto.s3.user-agent-prefix";
    String S3_CREDENTIALS_PROVIDER = "presto.s3.credentials-provider";
    String S3_SSE_TYPE = "presto.s3.sse.type";
    String S3_SSE_ENABLED = "presto.s3.sse.enabled";
    String S3_SSE_KMS_KEY_ID = "presto.s3.sse.kms-key-id";
    String S3_KMS_KEY_ID = "presto.s3.kms-key-id";
    String S3_ENCRYPTION_MATERIALS_PROVIDER = "presto.s3.encryption-materials-provider";
    String S3_PIN_CLIENT_TO_CURRENT_REGION = "presto.s3.pin-client-to-current-region";
    String S3_USE_INSTANCE_CREDENTIALS = "presto.s3.use-instance-credentials";
    String S3_IAM_ROLE = "presto.hive.s3.iam-role";
    String S3_IAM_ROLE_SESSION_NAME = "presto.hive.s3.iam-role-session-name";
    String S3_MULTIPART_MIN_PART_SIZE = "presto.s3.multipart.min-part-size";
    String S3_MULTIPART_MIN_FILE_SIZE = "presto.s3.multipart.min-file-size";
    String S3_STAGING_DIRECTORY = "presto.s3.staging-directory";
    String S3_MAX_CONNECTIONS = "presto.s3.max-connections";
    String S3_SOCKET_TIMEOUT = "presto.s3.socket-timeout";
    String S3_CONNECT_TIMEOUT = "presto.s3.connect-timeout";
    String S3_MAX_RETRY_TIME = "presto.s3.max-retry-time";
    String S3_MAX_BACKOFF_TIME = "presto.s3.max-backoff-time";
    String S3_MAX_CLIENT_RETRIES = "presto.s3.max-client-retries";
    String S3_MAX_ERROR_RETRIES = "presto.s3.max-error-retries";
    String S3_SSL_ENABLED = "presto.s3.ssl.enabled";
    String S3_PATH_STYLE_ACCESS = "presto.s3.path-style-access";
    String S3_SIGNER_TYPE = "presto.s3.signer-type";
    String S3_ENDPOINT = "presto.s3.endpoint";
    String S3_SECRET_KEY = "presto.s3.secret-key";
    String S3_ACCESS_KEY = "presto.s3.access-key";
    String S3_ACL_TYPE = "presto.s3.upload-acl-type";
    String S3_SKIP_GLACIER_OBJECTS = "presto.s3.skip-glacier-objects";
    String S3_STORAGE_CLASS = "presto.s3.storage-class";

    void updateConfiguration(Configuration config);
}
