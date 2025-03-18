// The MIT License (MIT)
//
// Copyright (c) 2015, 2016, 2017 Howard Hinnant
// Copyright (c) 2016 Adrian Colomitchi
// Copyright (c) 2017 Florian Dang
// Copyright (c) 2017 Paul Thompson
// Copyright (c) 2018, 2019 Tomasz Kamiński
// Copyright (c) 2019 Jiangang Zhuang
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//
// Our apologies.  When the previous paragraph was written, lowercase had not yet
// been invented (that would involve another several millennia of evolution).
// We did not mean to shout.

#pragma once

#include <chrono>
#include <fstream>
#include <vector>
#include "velox/external/date/date.h"

namespace facebook::velox::date {
struct ttinfo
{
    std::int32_t  tt_gmtoff;
    unsigned char tt_isdst;
    unsigned char tt_abbrind;
    unsigned char pad[2];
};

static_assert(sizeof(ttinfo) == 8, "");

struct expanded_ttinfo
{
    std::chrono::seconds offset;
    std::string          abbrev;
    bool                 is_dst;

    expanded_ttinfo(std::chrono::seconds offset, const std::string& abbrev, bool is_dst) : offset(offset), abbrev(abbrev), is_dst(is_dst) {}
};

struct transition
{
    date::sys_seconds            timepoint;
    const expanded_ttinfo* info;

    explicit transition(date::sys_seconds tp, const expanded_ttinfo* i = nullptr)
        : timepoint(tp)
        , info(i)
        {}

    friend
    std::ostream&
    operator<<(std::ostream& os, const transition& t)
    {
        date::operator<<(os, t.timepoint) << "Z ";
        if (t.info->offset >= std::chrono::seconds{0}) {
            os << '+';
        }
        os << date::make_time(t.info->offset);
        if (t.info->is_dst > 0) {
            os << " daylight ";
        } else {
            os << " standard ";
        }
        os << t.info->abbrev;
        return os;
    }
};

void populate_transitions(std::vector<transition>& transitions, std::vector<expanded_ttinfo>& ttinfos, std::istream& inf);
}
