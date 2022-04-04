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
// Copyright (c) 2004, Google Inc.
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#pragma once

#include <folly/chrono/Hardware.h>

#if defined(__MACH__) && defined(__APPLE__)
#include <mach/mach_time.h>
#endif

// ----------------------------------------------------------------------
// CycleClock
//    A CycleClock tells you the current time in Cycles.  The "time"
//    is actually time since power-on.  This is like time() but doesn't
//    involve a system call and is much more precise.
//
// NOTE: Not all cpu/platform/kernel combinations guarantee that this
// clock increments at a constant rate or is synchronized across all logical
// cpus in a system.
//
// Also, in some out of order CPU implementations, the CycleClock is not
// serializing. So if you're trying to count at cycles granularity, your
// data might be inaccurate due to out of order instruction execution.
// ----------------------------------------------------------------------
struct CycleClock {
  static inline int64_t Now() {
#if defined(__MACH__) && defined(__APPLE__)
    // this goes at the top because we need ALL Macs, regardless of
    // architecture, to return the number of "mach time units" that
    // have passed since startup.  See sysinfo.cc where
    // InitializeSystemInfo() sets the supposed cpu clock frequency of
    // macs to the number of mach time units per second, not actual
    // CPU clock frequency (which can change in the face of CPU
    // frequency scaling).  Also note that when the Mac sleeps, this
    // counter pauses; it does not continue counting, nor does it
    // reset to zero.
    return mach_absolute_time();
#else
    return folly::hardware_timestamp();
#endif
  }
};
