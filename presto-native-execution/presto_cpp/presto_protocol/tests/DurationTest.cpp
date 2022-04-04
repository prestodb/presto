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
#include "presto_cpp/presto_protocol/Duration.h"
#include <gtest/gtest.h>

using namespace facebook::presto::protocol;

void assertDuration(
    Duration duration,
    double value,
    TimeUnit unit,
    const std::string& serialized) {
  EXPECT_EQ(duration.getValue(), value);
  EXPECT_EQ(duration.getTimeUnit(), unit);
  EXPECT_EQ(duration.toString(), serialized);
}

void assertDuration(
    const std::string& durationString,
    double value,
    TimeUnit unit) {
  {
    Duration duration(durationString);
    assertDuration(duration, value, unit, durationString);
  }

  {
    Duration duration(value, unit);
    assertDuration(duration, value, unit, durationString);
  }
}

TEST(DurationTest, basic) {
  assertDuration(Duration(), 0, TimeUnit::SECONDS, "0.00s");
  assertDuration("1.00d", 1, TimeUnit::DAYS);
  assertDuration("1.00m", 1, TimeUnit::MINUTES);
  assertDuration("1800000.00ns", 1'800'000, TimeUnit::NANOSECONDS);

  Duration d("4 ms");
  assertDuration(d, 4, TimeUnit::MILLISECONDS, "4.00ms");

  ASSERT_NEAR(d.getValue(TimeUnit::NANOSECONDS), 4000000, 0.0000000001);
  ASSERT_NEAR(d.getValue(TimeUnit::MICROSECONDS), 4000, 0.0000000001);
  ASSERT_NEAR(d.getValue(TimeUnit::MILLISECONDS), 4, 0.0000000001);
  ASSERT_NEAR(d.getValue(TimeUnit::SECONDS), 0.004, 0.0000000001);
  ASSERT_NEAR(d.getValue(TimeUnit::MINUTES), 0.000066666666667, 0.0000000001);
  ASSERT_NEAR(d.getValue(TimeUnit::HOURS), 1.11111e-06, 0.0000000001);
  ASSERT_NEAR(d.getValue(TimeUnit::DAYS), 4.6296296296296295e-08, 0.0000000001);
}

TEST(DurationTest, chronoConversion) {
  Duration d("4 ms");
  assertDuration(d, 4, TimeUnit::MILLISECONDS, "4.00ms");
  EXPECT_EQ(d.asChronoDuration<std::chrono::nanoseconds>().count(), 4000000);
  EXPECT_EQ(d.asChronoDuration<std::chrono::microseconds>().count(), 4000);
  EXPECT_EQ(d.asChronoDuration<std::chrono::milliseconds>().count(), 4);

  d = Duration("120m");
  EXPECT_EQ(d.asChronoDuration<std::chrono::seconds>().count(), 7200);
  EXPECT_EQ(d.asChronoDuration<std::chrono::minutes>().count(), 120);
  EXPECT_EQ(d.asChronoDuration<std::chrono::hours>().count(), 2);
}
