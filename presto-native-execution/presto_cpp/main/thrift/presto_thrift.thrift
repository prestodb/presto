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

namespace cpp2 facebook.presto.thrift

enum TaskState {
  PLANNED = 0,
  RUNNING = 1,
  FINISHED = 2,
  CANCELED = 3,
  ABORTED = 4,
  FAILED = 5,
}

enum ErrorType {
  USER_ERROR = 0,
  INTERNAL_ERROR = 1,
  INSUFFICIENT_RESOURCES = 2,
  EXTERNAL = 3,
}

struct Lifespan {
  1: bool grouped;
  2: i32 groupId;
}

struct ErrorLocation {
  1: i32 lineNumber;
  2: i32 columnNumber;
}

struct HostAddress {
  1: string host;
  2: i32 port;
}

struct TaskStatus {
  1: i64 taskInstanceIdLeastSignificantBits;
  2: i64 taskInstanceIdMostSignificantBits;
  3: i64 version;
  4: TaskState state;
  5: string taskName;
  6: set<Lifespan> completedDriverGroups;
  7: list<ExecutionFailureInfo> failures;
  8: i32 queuedPartitionedDrivers;
  9: i32 runningPartitionedDrivers;
  10: double outputBufferUtilization;
  11: bool outputBufferOverutilized;
  12: i64 physicalWrittenDataSizeInBytes;
  13: i64 memoryReservationInBytes;
  14: i64 systemMemoryReservationInBytes;
  15: i64 fullGcCount;
  16: i64 fullGcTimeInMillis;
  17: i64 peakNodeTotalMemoryReservationInBytes;
}

struct ErrorCode {
  1: i32 code;
  2: string name;
  3: ErrorType type;
}

struct ExecutionFailureInfo {
  1: string type;
  2: string message;
  3: optional ExecutionFailureInfo cause (cpp.ref_type = "shared");
  4: list<ExecutionFailureInfo> suppressed;
  5: list<string> stack;
  6: ErrorLocation errorLocation;
  7: ErrorCode errorCode;
  8: HostAddress remoteHost;
}

service PrestoThrift {
  void fake();
}
