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
package com.facebook.presto.spi;

import com.facebook.presto.common.ErrorCode;
import com.facebook.presto.common.ErrorType;

import static com.facebook.presto.common.ErrorType.INSUFFICIENT_RESOURCES;
import static com.facebook.presto.common.ErrorType.INTERNAL_ERROR;
import static com.facebook.presto.common.ErrorType.USER_ERROR;

public enum StandardErrorCode
        implements ErrorCodeSupplier
{
    GENERIC_USER_ERROR(0x0000_0000, USER_ERROR),
    SYNTAX_ERROR(0x0000_0001, USER_ERROR),
    ABANDONED_QUERY(0x0000_0002, USER_ERROR),
    USER_CANCELED(0x0000_0003, USER_ERROR),
    PERMISSION_DENIED(0x0000_0004, USER_ERROR),
    NOT_FOUND(0x0000_0005, USER_ERROR),
    FUNCTION_NOT_FOUND(0x0000_0006, USER_ERROR),
    INVALID_FUNCTION_ARGUMENT(0x0000_0007, USER_ERROR),             // caught by TRY
    DIVISION_BY_ZERO(0x0000_0008, USER_ERROR),                      // caught by TRY
    INVALID_CAST_ARGUMENT(0x0000_0009, USER_ERROR),                 // caught by TRY
    OPERATOR_NOT_FOUND(0x0000_000A, USER_ERROR),
    INVALID_VIEW(0x0000_000B, USER_ERROR),
    ALREADY_EXISTS(0x0000_000C, USER_ERROR),
    NOT_SUPPORTED(0x0000_000D, USER_ERROR),
    INVALID_SESSION_PROPERTY(0x0000_000E, USER_ERROR),
    INVALID_WINDOW_FRAME(0x0000_000F, USER_ERROR),
    CONSTRAINT_VIOLATION(0x0000_0010, USER_ERROR),
    TRANSACTION_CONFLICT(0x0000_0011, USER_ERROR),
    INVALID_TABLE_PROPERTY(0x0000_0012, USER_ERROR),
    NUMERIC_VALUE_OUT_OF_RANGE(0x0000_0013, USER_ERROR),            // caught by TRY
    UNKNOWN_TRANSACTION(0x0000_0014, USER_ERROR),
    NOT_IN_TRANSACTION(0x0000_0015, USER_ERROR),
    TRANSACTION_ALREADY_ABORTED(0x0000_0016, USER_ERROR),
    READ_ONLY_VIOLATION(0x0000_0017, USER_ERROR),
    MULTI_CATALOG_WRITE_CONFLICT(0x0000_0018, USER_ERROR),
    AUTOCOMMIT_WRITE_CONFLICT(0x0000_0019, USER_ERROR),
    UNSUPPORTED_ISOLATION_LEVEL(0x0000_001A, USER_ERROR),
    INCOMPATIBLE_CLIENT(0x0000_001B, USER_ERROR),
    SUBQUERY_MULTIPLE_ROWS(0x0000_001C, USER_ERROR),
    PROCEDURE_NOT_FOUND(0x0000_001D, USER_ERROR),
    INVALID_PROCEDURE_ARGUMENT(0x0000_001E, USER_ERROR),
    QUERY_REJECTED(0x0000_001F, USER_ERROR),
    AMBIGUOUS_FUNCTION_CALL(0x0000_0020, USER_ERROR),
    INVALID_SCHEMA_PROPERTY(0x0000_0021, USER_ERROR),
    SCHEMA_NOT_EMPTY(0x0000_0022, USER_ERROR),
    QUERY_TEXT_TOO_LARGE(0x0000_0023, USER_ERROR),
    UNSUPPORTED_SUBQUERY(0x0000_0024, USER_ERROR),
    EXCEEDED_FUNCTION_MEMORY_LIMIT(0x0000_0025, USER_ERROR),
    ADMINISTRATIVELY_KILLED(0x0000_0026, USER_ERROR),
    INVALID_COLUMN_PROPERTY(0x0000_0027, USER_ERROR),
    QUERY_HAS_TOO_MANY_STAGES(0x0000_0028, USER_ERROR),
    INVALID_SPATIAL_PARTITIONING(0x0000_0029, USER_ERROR),
    INVALID_ANALYZE_PROPERTY(0x0000_002A, USER_ERROR),
    GENERATED_BYTECODE_TOO_LARGE(0x0000_002B, USER_ERROR),
    WARNING_AS_ERROR(0x0000_002C, USER_ERROR),
    INVALID_ARGUMENTS(0x0000_002D, USER_ERROR),
    EXCEEDED_PLAN_NODE_LIMIT(0x0000_002E, USER_ERROR),
    INVALID_TYPE_DEFINITION(0x0000_002F, USER_ERROR),
    VIEW_NOT_FOUND(0x0000_0030, USER_ERROR),
    INVALID_LIMIT_CLAUSE(0x0000_0031, USER_ERROR),
    COLUMN_NOT_FOUND(0x0000_0032, USER_ERROR),
    UNKNOWN_TYPE(0x0000_0033, USER_ERROR),

    GENERIC_INTERNAL_ERROR(0x0001_0000, INTERNAL_ERROR),
    TOO_MANY_REQUESTS_FAILED(0x0001_0001, INTERNAL_ERROR, true),
    PAGE_TOO_LARGE(0x0001_0002, INTERNAL_ERROR),
    PAGE_TRANSPORT_ERROR(0x0001_0003, INTERNAL_ERROR, true),
    PAGE_TRANSPORT_TIMEOUT(0x0001_0004, INTERNAL_ERROR, true),
    NO_NODES_AVAILABLE(0x0001_0005, INTERNAL_ERROR),
    REMOTE_TASK_ERROR(0x0001_0006, INTERNAL_ERROR, true),
    COMPILER_ERROR(0x0001_0007, INTERNAL_ERROR),
    REMOTE_TASK_MISMATCH(0x0001_0008, INTERNAL_ERROR),
    SERVER_SHUTTING_DOWN(0x0001_0009, INTERNAL_ERROR),
    FUNCTION_IMPLEMENTATION_MISSING(0x0001_000A, INTERNAL_ERROR),
    REMOTE_BUFFER_CLOSE_FAILED(0x0001_000B, INTERNAL_ERROR, true),
    SERVER_STARTING_UP(0x0001_000C, INTERNAL_ERROR),
    FUNCTION_IMPLEMENTATION_ERROR(0x0001_000D, INTERNAL_ERROR),
    INVALID_PROCEDURE_DEFINITION(0x0001_000E, INTERNAL_ERROR),
    PROCEDURE_CALL_FAILED(0x0001_000F, INTERNAL_ERROR),
    AMBIGUOUS_FUNCTION_IMPLEMENTATION(0x0001_0010, INTERNAL_ERROR),
    ABANDONED_TASK(0x0001_0011, INTERNAL_ERROR),
    CORRUPT_SERIALIZED_IDENTITY(0x0001_0012, INTERNAL_ERROR),
    CORRUPT_PAGE(0x0001_0013, INTERNAL_ERROR),
    OPTIMIZER_TIMEOUT(0x0001_0014, INTERNAL_ERROR),
    OUT_OF_SPILL_SPACE(0x0001_0015, INTERNAL_ERROR),
    REMOTE_HOST_GONE(0x0001_0016, INTERNAL_ERROR, true),
    CONFIGURATION_INVALID(0x0001_0017, INTERNAL_ERROR),
    CONFIGURATION_UNAVAILABLE(0x0001_0018, INTERNAL_ERROR),
    INVALID_RESOURCE_GROUP(0x0001_0019, INTERNAL_ERROR),
    GENERIC_RECOVERY_ERROR(0x0001_001A, INTERNAL_ERROR),
    TOO_MANY_TASK_FAILED(0x0001_001B, INTERNAL_ERROR),
    INDEX_LOADER_TIMEOUT(0x0001_001C, INTERNAL_ERROR),
    EXCEEDED_TASK_UPDATE_SIZE_LIMIT(0x0001_001D, INTERNAL_ERROR),
    NODE_SELECTION_NOT_SUPPORTED(0x0001_001E, INTERNAL_ERROR),
    SPOOLING_STORAGE_ERROR(0x0001_001F, INTERNAL_ERROR),
    SERIALIZED_PAGE_CHECKSUM_ERROR(0x0001_0020, INTERNAL_ERROR),
    RETRY_QUERY_NOT_FOUND(0x0001_0021, INTERNAL_ERROR),
    DISTRIBUTED_TRACING_ERROR(0x0001_0022, INTERNAL_ERROR),
    GENERIC_SPILL_FAILURE(0x0001_0023, INTERNAL_ERROR),
    INVALID_PLAN_ERROR(0x0001_0024, INTERNAL_ERROR),
    INVALID_RETRY_EXECUTION_STRATEGY(0x0001_0025, INTERNAL_ERROR),
    PLAN_SERIALIZATION_ERROR(0x0001_0026, INTERNAL_ERROR),
    QUERY_PLANNING_TIMEOUT(0x0001_0027, INTERNAL_ERROR),
    NATIVE_EXECUTION_TASK_ERROR(0x0001_0028, INTERNAL_ERROR),
    UNSUPPORTED_ANALYZER_TYPE(0x0001_0029, INTERNAL_ERROR),
    SLICE_TOO_LARGE(0x0001_002A, INTERNAL_ERROR),
    HBO_FAILURE(0x0001_002B, INTERNAL_ERROR),
    CLUSTER_HAS_TOO_MANY_RUNNING_TASKS(0x0001_002C, INTERNAL_ERROR),
    // AMS error codes
    AMS_MISSING_CRN(0x0001_0400, INTERNAL_ERROR),
    AMS_MISSING_URL(0x0001_0401, INTERNAL_ERROR),
    AMS_MISSING_LH_INSTANCE_ID(0x0001_0402, INTERNAL_ERROR),
    AMS_INPUT_USER(0x0001_0403, USER_ERROR),
    AMS_INPUT_PASSWORD(0x0001_0404, USER_ERROR),
    AMS_INCORRECT_PASSWORD(0x0001_0405, USER_ERROR),
    AMS_INVALID_USER_FORMAT(0x0001_0406, INTERNAL_ERROR),
    AMS_TOKEN_VALIDATION_FAILED(0x0001_0407, INTERNAL_ERROR),
    AMS_USERNAME_NOT_MATCHED_IN_TOKEN(0x0001_0408, INTERNAL_ERROR),
    AMS_NETWORK_ERROR(0x0001_0409, INTERNAL_ERROR),
    AMS_SERVER_ERROR(0x0001_040A, INTERNAL_ERROR),
    AMS_QUERY_INTEGRITY_CHECK_FAIL(0x0001_040B, INTERNAL_ERROR),
    AMS_PRINCIPAL_CANNOT_BECOME_USER(0x0001_040C, INTERNAL_ERROR),
    AMS_CANNOT_ACCESS_CATALOG(0x0001_040D, INTERNAL_ERROR),
    AMS_CANNOT_CREATE_SCHEMA(0x0001_040E, INTERNAL_ERROR),
    AMS_CANNOT_DROP_SCHEMA(0x0001_040F, INTERNAL_ERROR),
    AMS_CANNOT_RENAME_SCHEMA(0x0001_0410, INTERNAL_ERROR),
    AMS_CANNOT_SHOW_SCHEMA(0x0001_0411, INTERNAL_ERROR),
    AMS_CANNOT_SHOW_CREATE_TABLE(0x0001_0412, INTERNAL_ERROR),
    AMS_CANNOT_CREATE_TABLE(0x0001_0413, INTERNAL_ERROR),
    AMS_CANNOT_DROP_TABLE(0x0001_0414, INTERNAL_ERROR),
    AMS_CANNOT_RENAME_TABLE(0x0001_0415, INTERNAL_ERROR),
    AMS_CANNOT_SHOW_METADATA_OF_TABLE(0x0001_0416, INTERNAL_ERROR),
    AMS_CANNOT_SHOW_COLUMNS_OF_TABLE(0x0001_0417, INTERNAL_ERROR),
    AMS_CANNOT_ADD_COLUMN_TO_TABLE(0x0001_0418, INTERNAL_ERROR),
    AMS_CANNOT_DROP_COLUMN_FROM_TABLE(0x0001_0419, INTERNAL_ERROR),
    AMS_CANNOT_RENAME_COLUMN_IN_TABLE(0x0001_041A, INTERNAL_ERROR),
    AMS_CANNOT_SELECT_FROM_TABLE(0x0001_041B, INTERNAL_ERROR),
    AMS_CANNOT_INSERT_INTO_TABLE(0x0001_041C, INTERNAL_ERROR),
    AMS_CANNOT_DELETE_FROM_TABLE(0x0001_041D, INTERNAL_ERROR),
    AMS_CANNOT_TRUNCATE_TABLE(0x0001_041E, INTERNAL_ERROR),
    AMS_CANNOT_CREATE_VIEW(0x0001_041F, INTERNAL_ERROR),
    AMS_VIEW_OWNER_CANNOT_CREATE_VIEW(0x0001_0420, INTERNAL_ERROR),
    AMS_CANNOT_DROP_VIEW(0x0001_0421, INTERNAL_ERROR),
    AMS_CANNOT_SELECT_FROM_VIEW(0x0001_0422, INTERNAL_ERROR),
    AMS_CANNOT_GRANT_PRIVILEGE_ON_TABLE(0x0001_0423, INTERNAL_ERROR),
    AMS_CANNOT_REVOKE_PRIVILEGE_ON_TABLE(0x0001_0424, INTERNAL_ERROR),
    AMS_CANNOT_SHOW_ROLES_FROM_CATALOG(0x0001_0425, INTERNAL_ERROR),
    AMS_CANNOT_SHOW_CURRENT_ROLES_FROM_CATALOG(0x0001_0426, INTERNAL_ERROR),
    AMS_CANNOT_SHOW_ROLE_GRANTS_FROM_CATALOG(0x0001_0427, INTERNAL_ERROR),
    AMS_CANNOT_SET_SYSTEM_SESSION_PROPERTY(0x0001_0428, INTERNAL_ERROR),
    AMS_CANNOT_SET_CATALOG_SESSION_PROPERTY(0x0001_0429, INTERNAL_ERROR),
    AMS_CANNOT_SELECT_FROM_COLUMNS_IN_TABLE(0x0001_042A, INTERNAL_ERROR),
    AMS_CANNOT_CREATE_ROLE(0x0001_042B, INTERNAL_ERROR),
    AMS_CANNOT_DROP_ROLE(0x0001_042C, INTERNAL_ERROR),
    AMS_CANNOT_GRANT_ROLES(0x0001_042D, INTERNAL_ERROR),
    AMS_CANNOT_REVOKE_ROLES(0x0001_042E, INTERNAL_ERROR),
    AMS_CANNOT_SET_ROLE(0x0001_042F, INTERNAL_ERROR),
    AMS_CANNOT_ACCESS_ENGINE(0x0001_0430, INTERNAL_ERROR),
    AMS_CANNOT_GET_TOKEN(0x0001_0431, INTERNAL_ERROR),
    AMS_CANNOT_GET_PUBLIC_KEY(0x0001_0432, INTERNAL_ERROR),
    AMS_INVALID_CRN_TOKEN(0x001_0433, INTERNAL_ERROR),
    AMS_CANNOT_RENAME_VIEW(0x0001_0434, INTERNAL_ERROR),
// End AMS

    // Ahana-only internal error codes start from here
    GENERIC_AHANA_ERROR(0X0001_0500, INTERNAL_ERROR),
    UPDATE_EVENT_LISTENER_FAILURE(0X0001_0501, INTERNAL_ERROR),
    INVALID_ROW_FILTER(0x0001_0502, USER_ERROR),
    INVALID_COLUMN_MASK(0x0001_0503, USER_ERROR),
    DATATYPE_MISMATCH(0x0001_0504, USER_ERROR),
    // End Ahana-only

    GENERIC_INSUFFICIENT_RESOURCES(0x0002_0000, INSUFFICIENT_RESOURCES),
    EXCEEDED_GLOBAL_MEMORY_LIMIT(0x0002_0001, INSUFFICIENT_RESOURCES),
    QUERY_QUEUE_FULL(0x0002_0002, INSUFFICIENT_RESOURCES),
    EXCEEDED_TIME_LIMIT(0x0002_0003, INSUFFICIENT_RESOURCES),
    CLUSTER_OUT_OF_MEMORY(0x0002_0004, INSUFFICIENT_RESOURCES),
    EXCEEDED_CPU_LIMIT(0x0002_0005, INSUFFICIENT_RESOURCES),
    EXCEEDED_SPILL_LIMIT(0x0002_0006, INSUFFICIENT_RESOURCES),
    EXCEEDED_LOCAL_MEMORY_LIMIT(0x0002_0007, INSUFFICIENT_RESOURCES),
    ADMINISTRATIVELY_PREEMPTED(0x0002_0008, INSUFFICIENT_RESOURCES),
    EXCEEDED_SCAN_RAW_BYTES_READ_LIMIT(0x0002_0009, INSUFFICIENT_RESOURCES),
    EXCEEDED_OUTPUT_SIZE_LIMIT(0x0002_000A, INSUFFICIENT_RESOURCES),
    EXCEEDED_REVOCABLE_MEMORY_LIMIT(0x0002_000B, INSUFFICIENT_RESOURCES),
    EXCEEDED_LOCAL_BROADCAST_JOIN_MEMORY_LIMIT(0x0002_000C, INSUFFICIENT_RESOURCES),
    EXCEEDED_OUTPUT_POSITIONS_LIMIT(0x0002_000D, INSUFFICIENT_RESOURCES),
    NATIVE_EXECUTION_BINARY_NOT_EXIST(0x0002_000E, USER_ERROR),
    NATIVE_EXECUTION_PROCESS_LAUNCH_ERROR(0x0002_000F, INTERNAL_ERROR),
    MISSING_RESOURCE_GROUP_SELECTOR(0x0002_0010, INTERNAL_ERROR),
    EXCEEDED_HEAP_MEMORY_LIMIT(0x0002_0011, INSUFFICIENT_RESOURCES),
    EXCEEDED_WRITTEN_INTERMEDIATE_BYTES_LIMIT(0x0002_0012, INSUFFICIENT_RESOURCES),
    TOO_MANY_SIDECARS(0x0002_0013, INTERNAL_ERROR),
    NO_CPP_SIDECARS(0x0002_0014, INTERNAL_ERROR),
    HEADER_MODIFICATION_ATTEMPT(0x0002_0015, INTERNAL_ERROR),
    DUPLICATE_FUNCTION_ERROR(0x0002_0016, INTERNAL_ERROR),
    MEMORY_ARBITRATION_FAILURE(0x0002_0017, INSUFFICIENT_RESOURCES),
    /**/;

    // Error code range 0x0003 is reserved for Presto-on-Spark
    // See com.facebook.presto.spark.SparkErrorCode

    // Connectors can use error codes starting at the range 0x0100_0000
    // See https://github.com/prestodb/presto/wiki/Error-Codes

    private final ErrorCode errorCode;

    StandardErrorCode(int code, ErrorType type)
    {
        this(code, type, false);
    }

    StandardErrorCode(int code, ErrorType type, boolean retriable)
    {
        errorCode = new ErrorCode(code, name(), type, retriable);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
