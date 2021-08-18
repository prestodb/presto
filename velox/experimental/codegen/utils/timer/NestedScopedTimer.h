/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
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
#pragma once
#include <fmt/format.h>
#include <algorithm>
#include <chrono>
#include <map>
#include <queue>
#include <stack>
#include <thread>
#include <vector>
#include "boost/accumulators/accumulators.hpp"
#include "boost/accumulators/statistics.hpp"

namespace facebook::velox::codegen {

/// Represent a sequence of Start/End events from collected form Scoped Timers
/// We split the recorded event as Start/End to be able to recover nesting
/// structure offline \tparam Cl Time source Clock type, follow TrivialClock
/// concept \tparam EI EventInfo stored for each events
template <typename Cl, typename EI>
struct EventSequence {
  using Clock = Cl;
  using EventInfo = EI;
  static constexpr size_t defaultSize = 100;
  EventSequence() {
    startEvents.reserve(defaultSize);
    endEvents.reserve(defaultSize);
  }

  explicit EventSequence(size_t reservedSize) {
    startEvents.reserve(reservedSize);
    endEvents.reserve(reservedSize);
  }

  struct Event {
    EventInfo info;
    typename Cl::time_point timePoint;
    Event(const EventInfo& i, const typename Cl::time_point& t)
        : info(i), timePoint(t) {}
    Event() = default;
  };

  void addStartEvent(const EventInfo& info) {
    startEvents.emplace_back(info, Clock::now());
  }

  void addEndEvent(const EventInfo& info) {
    endEvents.emplace_back(info, Clock::now());
  }

  void clear() {
    startEvents.clear();
    endEvents.clear();
  }

  std::vector<Event> startEvents;
  std::vector<Event> endEvents;
  size_t pos;
};

template <typename Clock>
using NamedEventSequence = EventSequence<Clock, const char*>;
using NamedSteadyClockEventSequence =
    NamedEventSequence<std::chrono::steady_clock>;

/// Scoped Timer nested structure modeled as a tree
/// \tparam Clock Time source
template <typename Clock>
struct TimerTree {
  enum class EventType {
    START,
    END,
  };

  using EventVector = std::vector<
      std::pair<EventType, typename NamedEventSequence<Clock>::Event>>;

  typename NamedEventSequence<Clock>::EventInfo
      info; // Timer information (eg name)

  std::map<
      typename NamedEventSequence<Clock>::EventInfo,
      std::shared_ptr<TimerTree<Clock>>>
      children;

  std::vector<typename Clock::time_point> timePoints; // Start/End time events

  // Absolute time statistics (ns)
  double min_, max_, mean_, sum_, variance_;

  // relative times (ns)
  double timeRelativeToParentTimer_;
  double timeRelativeToTotalTime_;

  /// compute current timer statistics

  void computeNodeStatistics() {
    boost::accumulators::accumulator_set<
        double,
        boost::accumulators::stats<
            boost::accumulators::tag::sum,
            boost::accumulators::tag::min,
            boost::accumulators::tag::max,
            boost::accumulators::tag::mean,
            boost::accumulators::tag::lazy_variance>>
        accumulatorSet;

    for (size_t i = 0; i + 1 < timePoints.size(); i += 2) {
      auto durations = timePoints[i + 1] - timePoints[i];
      accumulatorSet(static_cast<double>(durations.count()));
    };

    min_ = boost::accumulators::min(accumulatorSet);
    max_ = boost::accumulators::max(accumulatorSet);
    mean_ = boost::accumulators::mean(accumulatorSet);
    sum_ = boost::accumulators::sum(accumulatorSet);
    variance_ = boost::accumulators::variance(accumulatorSet);
  }

  static void computeStatistics(TimerTree<Clock> root) {
    std::vector<TimerTree<Clock>*> nodes;
    nodes.push_back(&root);
    root.computeNodeStatistics();

    for (size_t currentNode = 0; currentNode < nodes.size(); ++currentNode) {
      for (auto& [name, child] : nodes[currentNode]->children) {
        child->computeNodeStatistics();
        child->timeRelativeToParentTimer_ =
            (child->sum_ / nodes[currentNode]->sum_) * 100.00;
        child->timeRelativeToTotalTime_ = (child->sum_ / root.sum_) * 100.00;
        nodes.push_back(child.get());
      }
    }
  }

  /// Given an eventSequence object, merge startEvents and endEvents into a
  /// single sorted event streams and tag the new events as start/end
  /// startEvents = [T1,T3],endEvents = [T2,T4] -->
  /// [{START,T1},{END,T2},{START,T3},{END,T4}] \param eventSequence \return
  /// merged tagged event stream
  static EventVector mergeEventVectors(
      const NamedEventSequence<Clock>& eventSequence) {
    EventVector startEvents;
    EventVector endEvents;

    std::transform(
        eventSequence.startEvents.begin(),
        eventSequence.startEvents.end(),
        std::back_inserter(startEvents),
        [](const auto& event) {
          return std::make_pair(EventType::START, event);
        });

    std::transform(
        eventSequence.endEvents.begin(),
        eventSequence.endEvents.end(),
        std::back_inserter(endEvents),
        [](const auto& event) {
          return std::make_pair(EventType::END, event);
        });

    EventVector mergedEvents;

    std::merge(
        startEvents.begin(),
        startEvents.end(),
        endEvents.begin(),
        endEvents.end(),
        std::back_inserter(mergedEvents),
        [](const auto& left, const auto& right) {
          return left.second.timePoint < right.second.timePoint;
        });
    return mergedEvents;
  }

  /// Build a new tree from an event Sequence, this is done by recovering the
  /// nested structure of the orignal scoped timers
  /// \param eventSequence
  /// \return
  static std::shared_ptr<TimerTree> fromEventSequence(
      const NamedEventSequence<Clock>& eventSequence) {
    auto mergedEvents = mergeEventVectors(eventSequence);

    // Create a root timer to enclosing all created timer,
    auto root = std::make_shared<TimerTree>();
    root->info = "<ROOT>";
    root->timePoints.push_back(mergedEvents.begin()->second.timePoint);
    root->timePoints.push_back(mergedEvents.rbegin()->second.timePoint);

    // Add the root timer as the first timer in the stack
    std::stack<std::shared_ptr<TimerTree>> activeTimers;
    activeTimers.push(root);

    for (const auto& event : mergedEvents) {
      // New timer starts
      if (event.first == EventType::START) {
        // Adds the new timer to the parent timer
        auto [newTimerIterator, inserted] = activeTimers.top()->children.insert(
            {event.second.info, std::make_shared<TimerTree>()});

        // Set new timer infos
        auto newTimer = newTimerIterator->second;
        newTimer->info = event.second.info;
        newTimer->timePoints.push_back(event.second.timePoint);

        // new timer is now the inner most timer
        activeTimers.push(newTimer);
        continue;
      };

      // Timer Ends
      if (event.first == EventType::END) {
        auto currentTimer = activeTimers.top();
        if (event.second.info != currentTimer->info) {
          throw std::runtime_error(fmt::format(
              "Improper timer nesting, closing timer {}, while timer {} is open",
              event.second.info,
              currentTimer->info));
        };
        currentTimer->timePoints.push_back(event.second.timePoint);
        activeTimers.pop();
      };
    }
    if (activeTimers.top() != root) {
      throw std::runtime_error(
          fmt::format("Timer {} was never closed", activeTimers.top()->info));
    };

    return root;
  }

  void print(
      std::ostream& out,
      const std::string& currentIdent,
      const std::string& ident) {
    out << currentIdent
        << fmt::format(
               "[{}] : count = {:d}, min = {:.0f}, max = {:.0f}, avg = {:.0f} , total (ns) = {:.0f} , % parent = {:.2f} , % total = {:.2f}\n",
               this->info,
               this->timePoints.size() / 2,
               min_,
               max_,
               mean_,
               sum_,
               timeRelativeToParentTimer_,
               timeRelativeToTotalTime_);

    auto newIndent = currentIdent + ident;
    for (const auto& [key, value] : this->children) {
      value->print(out, newIndent, ident);
    };
  }
};

/// Scoped timer with a name
/// When displayed, timer with the same name a considered equivalement and merge
/// as one WARNING : name is not owned nor copied and expected to be live when
/// the timer is displayed \tparam Clock
template <typename Clock>
struct NamedNestedScopedTimer {
  using EventSequence = NamedEventSequence<Clock>;
  NamedNestedScopedTimer(const char* name, EventSequence& eventSequence)
      : eventSequence_(eventSequence), name_(name) {
    eventSequence_.addStartEvent(name);
  }
  ~NamedNestedScopedTimer() {
    eventSequence_.addEndEvent(name_);
  }

  static std::string printCounters(EventSequence& eventSequence) {
    auto timerTree = computeTimerTreeStatistics(eventSequence);
    return printTimerTreeStatistics(timerTree);
  }

  static std::shared_ptr<TimerTree<Clock>> computeTimerTreeStatistics(
      EventSequence& eventSequence) {
    auto timerTree = TimerTree<Clock>::fromEventSequence(eventSequence);
    TimerTree<Clock>::computeStatistics(*timerTree);
    return timerTree;
  }

  static std::string printTimerTreeStatistics(
      std::shared_ptr<TimerTree<Clock>>& timerTree) {
    std::stringstream stream;
    timerTree->print(stream, "", "+");
    return stream.str();
  }

  NamedEventSequence<Clock>& eventSequence_;
  const char* name_;
};
using SteadyClockNamedScopedTimer =
    NamedNestedScopedTimer<std::chrono::steady_clock>;

using DefaultScopedTimer = SteadyClockNamedScopedTimer;
using DefaultEventSequence = DefaultScopedTimer::EventSequence;

} // namespace facebook::velox::codegen
