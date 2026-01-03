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

#pragma once

#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>

#include "common/logging.h"
#include "common/status.h"
#include "util/thread.h"

namespace impala {

// Manages a thread that periodically notifies a condition variable. This thread never
// returns. An indicator variable must be specified to guard against spurious wakeups.
//
// Immediately before this class notfies the condition variable, it sets the indicator
// variable to the `wakeup_value` specified in the constructor. It is the responsibility
// of the thread consuming this class to reset the indicator variable to a value other
// than `wakeup_value` before the consuming thread goes to sleep.
//
// If the periodic code takes longer to run than the specified duration, then the code
// will immediately execute the next time around.
//
// Internally, this class uses std::this_thread:sleep_for which may sleep for longer than
// the specified duration due to scheduling or resource contention delays.
// For details, see https://en.cppreference.com/w/cpp/thread/sleep_for.
//
// Example usage:
//
//   #include <chrono>
//   #include <condition_variable>
//   #include <memory>
//   #include <mutex>
//
//   #include "common/status.h"
//
//   std::condition_variable cv;
//   std::mutex mu;
//   std::shared_ptr<bool> wakeup_guard = make_shared<bool>();
//   Ticker<std::chrono::seconds, bool> ticker(std::chrono::seconds(30), cv, mu,
//       wakeup_guard, true);
//
//   ABORT_IF_ERROR(ticker.Start());
//
//   unique_lock<mutex> l(mu);
//   while(true) {
//     cv.wait(l, ticker.WakeupGuard());
//     *wakeup_guard = false;
//
//     run_my_code();
//   }

template <typename DurationType, typename IndicatorType>
class Ticker {
  public:
    Ticker(DurationType interval, std::condition_variable& cv,
        std::mutex& mu, std::shared_ptr<IndicatorType> indicator,
        IndicatorType wakeup_value) : indicator_(indicator), interval_(interval), cv_(cv),
        mu_(mu), wakeup_value_(wakeup_value) {
      DCHECK(indicator_) << "indicator shared_ptr must be initialized";
    }

    // Starts the ticker loop by spinning up a separate thread. Sets is_running_ to true
    // if the thread was successfully created. The category and name parameters are passed
    // to the Thread::Create() function.
    const Status Start(const std::string& category, const std::string& name) {
      std::lock_guard<std::mutex> l(looper_mu_);
      const Status stat = Thread::Create(category, name, &Ticker::run, this, &my_thread_);

      if (stat.ok()) {
        is_running_ = true;
      }

      return stat;
    }

    // Specify that the ticker stop as soon as possible. If the ticker is sleeping, it
    // will be woken up to process the stop request.
    // Does not block after notifying the ticker to stop. Call Join() to wait for the
    // ticker to fully stop.
    void Stop() {
      {
        std::lock_guard l(looper_mu_);
        stop_requested_ = true;
      }
      looper_.notify_all();
    }

    // Join the thread running the ticker loop.
    void Join() {
      std::unique_lock<std::mutex> l(looper_mu_);
      if (is_running_ && my_thread_) {
        l.unlock();
        my_thread_->Join();
      }
    }

    // Provides a default implementation for the condition variable predicate lambda.
    operator bool() {
      return *indicator_ == wakeup_value_;
    }

  protected:
    // Shared pointer to the indicator variable that signals the condition variable cv_
    // was notified by this ticker and thus the condition variable is not experiencing a
    // spurious wakeup.
    std::shared_ptr<IndicatorType> indicator_;

  private:
    // Duration between ticks of this ticker. Initialized in the constructor.
    const DurationType interval_;

    // Condition variable to notify on each tick by calling the condition variable's
    // notify_all() function and the mutex to protect that condition variable.
    std::condition_variable& cv_;
    std::mutex& mu_;

    // When this ticker is notifying the condition variable, it sets the indicator_ shared
    // pointer to this value.
    const IndicatorType wakeup_value_;

    // Thread that runs the ticker loop.
    std::unique_ptr<Thread> my_thread_;

    // Condition variable and mutex used by the ticker loop to periodically wake up.
    std::condition_variable looper_;
    std::mutex looper_mu_;

    // Specifies this ticker should stop.
    bool stop_requested_ = false;

    // Set to true if the Start() function was called and the ticker loop thread
    // successfully started.
    bool is_running_ = false;

    // The function to perform the ticker loop.
    void run() {
      std::unique_lock<std::mutex> looper_lock(looper_mu_);

      while (true) {
        const std::chrono::time_point<std::chrono::steady_clock> next_wakeup =
            std::chrono::steady_clock::now() + interval_;
        looper_.wait_until(looper_lock, next_wakeup);

        if (stop_requested_) {
          break;
        }

        {
          std::lock_guard<std::mutex> l(mu_);
          *indicator_ = wakeup_value_;
        }

        cv_.notify_all();
      }

      is_running_ = false;
    }
}; // class Ticker

// Specialization of the Ticker class that uses seconds for the duration and bool as the
// wakeup indicator. The boolean shared_ptr indicator is internally managed. Use the
// ResetWakeupGuard() function in your code immediately after the condition variable wait
// to set the internally managed wakeup guard for the next iteration.
class TickerSecondsBool : public Ticker<std::chrono::seconds, bool> {
  public:
    TickerSecondsBool(uint32_t interval, std::condition_variable& cv,
      std::mutex& lock) :
      Ticker(std::chrono::seconds(interval), cv, lock, std::make_shared<bool>(), true) {}

    void ResetWakeupGuard() {
      *indicator_ = false;
    }
}; // class TickerSecondsBool

} // namespace impala
