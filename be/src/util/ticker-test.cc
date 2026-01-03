// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "util/ticker.h"

#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>

#include "testutil/gtest-util.h"

#include "common/status.h"
#include "util/stopwatch.h"

using namespace std;

namespace impala {

static inline float NsToMs(int64_t nanos) {
  return static_cast<float>(nanos / 1000000);
}

TEST(TickerTest, TickerSecondsBoolHappyPath) {
  condition_variable cv;
  mutex mu;
  uint8_t cntr = 0;

  TickerSecondsBool fixture(1, cv, mu);
  MonotonicStopWatch sw;

  sw.Start();
  ABORT_IF_ERROR(fixture.Start("category", "tickersecondsbool-happy-path"));

  while (cntr < 3) {
    unique_lock<mutex> l(mu);
    cv.wait(l, [&fixture]() -> bool { return fixture; });
    fixture.ResetWakeupGuard();
    cntr++;
  }

  sw.Stop();

  fixture.Stop();
  fixture.Join();

  EXPECT_EQ(cntr, 3);
  // Include a 30 millisecond (1%) margin of error to tolerate differences in the
  // precision of time measurements.
  EXPECT_NEAR(NsToMs(sw.ElapsedTime()), static_cast<float>(3000), 30);
}

TEST(TickerTest, TickerHappyPath) {
  condition_variable cv;
  mutex mu;
  shared_ptr<string> wakeup_guard = make_shared<string>();
  uint8_t cntr = 0;
  const string wakeup_val = "wakeup";

  Ticker<chrono::milliseconds, string> fixture(chrono::milliseconds(5), cv, mu,
      wakeup_guard, wakeup_val);
  MonotonicStopWatch sw;

  sw.Start();
  ABORT_IF_ERROR(fixture.Start("category", "ticker-happy-path"));

  while (cntr < 100) {
    unique_lock<mutex> l(mu);
    cv.wait(l, [&fixture]() -> bool { return fixture; });
    *wakeup_guard = "";
    cntr++;
  }

  sw.Stop();

  fixture.Stop();
  fixture.Join();

  EXPECT_EQ(cntr, 100);
  // Include a 10 millisecond (2%) margin of error to tolerate differences in the
  // precision of time measurements.
  EXPECT_NEAR(NsToMs(sw.ElapsedTime()), static_cast<float>(500), 10);
}

// Tests the case where the wakeup guard is not reset by the consuming code.
TEST(TickerTest, TickerNoWakeupGuardReset) {
  condition_variable cv;
  mutex mu;
  shared_ptr<string> wakeup_guard = make_shared<string>();
  uint8_t cntr = 0;
  const string wakeup_val = "wakeup";

  Ticker<chrono::milliseconds, string> fixture(chrono::milliseconds(5), cv, mu,
      wakeup_guard, wakeup_val);
  MonotonicStopWatch sw;

  sw.Start();
  ABORT_IF_ERROR(fixture.Start("category", "ticker-happy-path"));

  while (cntr < 10) {
    unique_lock<mutex> l(mu);
    cv.wait(l, [&fixture]() -> bool { return fixture; });
    // No wakeup guard reset here.
    cntr++;
  }

  sw.Stop();

  fixture.Stop();
  fixture.Join();

  EXPECT_EQ(cntr, 10);
  // If the wakeup guard was set properly, elapsed time would be 50 milliseconds. Since
  // the wakeup guard does not get set, spurious wakeups of the condition variable happen
  // much more frequently than they should.
  EXPECT_NEAR(NsToMs(sw.ElapsedTime()), static_cast<float>(5), 5);
}

// Stop a Ticker that has not been started.
TEST(TickerTest, TickerStopWithoutStart) {
  condition_variable cv;
  mutex mu;
  shared_ptr<string> wakeup_guard = make_shared<string>("");
  Ticker<chrono::milliseconds, string> fixture(chrono::milliseconds(5), cv, mu,
      wakeup_guard, "wakeup");
  fixture.Stop();
  fixture.Join();
}

// Test joining a Ticker that has not been started.
TEST(TickerTest, TickerJoinWithoutStart) {
  condition_variable cv;
  mutex mu;
  shared_ptr<string> wakeup_guard = make_shared<string>("");
  Ticker<chrono::milliseconds, string>(chrono::milliseconds(5), cv, mu,
      wakeup_guard, "wakeup").Join();
}

// Test stopping a Ticker with a long interval.
TEST(TickerTest, TickerStopLongInterval) {
  condition_variable cv;
  mutex mu;
  shared_ptr<string> wakeup_guard = make_shared<string>("");
  Ticker<chrono::milliseconds, string> fixture(chrono::milliseconds(5000), cv, mu,
      wakeup_guard, "wakeup");
  MonotonicStopWatch sw;

  sw.Start();
  ABORT_IF_ERROR(fixture.Start("category", "ticker-stop-long-interval"));
  SleepForMs(250);
  fixture.Stop();
  fixture.Join();
  sw.Stop();

  EXPECT_NEAR(NsToMs(sw.ElapsedTime()), static_cast<float>(250), 100);
}

} // namespace impala
