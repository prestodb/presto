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

#include "velox/external/date/tz.h"

namespace facebook::velox::date {
CONSTDATA auto min_year = date::year::min();
CONSTDATA auto max_year = date::year::max();

CONSTDATA auto min_day = date::January/1;
CONSTDATA auto max_day = date::December/31;

CONSTCD14 const sys_seconds min_seconds = date::sys_days(min_year/min_day);

enum class endian
{
    native = __BYTE_ORDER__,
    little = __ORDER_LITTLE_ENDIAN__,
    big    = __ORDER_BIG_ENDIAN__
};

inline
std::uint32_t
reverse_bytes(std::uint32_t i)
{
    return
        (i & 0xff000000u) >> 24 |
        (i & 0x00ff0000u) >> 8 |
        (i & 0x0000ff00u) << 8 |
        (i & 0x000000ffu) << 24;
}

inline
std::uint64_t
reverse_bytes(std::uint64_t i)
{
    return
        (i & 0xff00000000000000ull) >> 56 |
        (i & 0x00ff000000000000ull) >> 40 |
        (i & 0x0000ff0000000000ull) >> 24 |
        (i & 0x000000ff00000000ull) >> 8 |
        (i & 0x00000000ff000000ull) << 8 |
        (i & 0x0000000000ff0000ull) << 24 |
        (i & 0x000000000000ff00ull) << 40 |
        (i & 0x00000000000000ffull) << 56;
}

template <class T>
inline
void
maybe_reverse_bytes(T&, std::false_type)
{
}

inline
void
maybe_reverse_bytes(std::int32_t& t, std::true_type)
{
    t = static_cast<std::int32_t>(reverse_bytes(static_cast<std::uint32_t>(t)));
}

inline
void
maybe_reverse_bytes(std::int64_t& t, std::true_type)
{
    t = static_cast<std::int64_t>(reverse_bytes(static_cast<std::uint64_t>(t)));
}

template <class T>
inline
void
maybe_reverse_bytes(T& t)
{
    maybe_reverse_bytes(t, std::integral_constant<bool,
                                                  endian::native == endian::little>{});
}

void
load_header(std::istream& inf)
{
    // Read TZif
    auto t = inf.get();
    auto z = inf.get();
    auto i = inf.get();
    auto f = inf.get();
#ifndef NDEBUG
    assert(t == 'T');
    assert(z == 'Z');
    assert(i == 'i');
    assert(f == 'f');
#else
    (void)t;
    (void)z;
    (void)i;
    (void)f;
#endif
}

unsigned char
load_version(std::istream& inf)
{
    // Read version
    auto v = inf.get();
    assert(v != EOF);
    return static_cast<unsigned char>(v);
}

void
skip_reserve(std::istream& inf)
{
    inf.ignore(15);
}

void
load_counts(std::istream& inf,
            std::int32_t& tzh_ttisgmtcnt, std::int32_t& tzh_ttisstdcnt,
            std::int32_t& tzh_leapcnt,    std::int32_t& tzh_timecnt,
            std::int32_t& tzh_typecnt,    std::int32_t& tzh_charcnt)
{
    // Read counts;
    inf.read(reinterpret_cast<char*>(&tzh_ttisgmtcnt), 4);
    maybe_reverse_bytes(tzh_ttisgmtcnt);
    inf.read(reinterpret_cast<char*>(&tzh_ttisstdcnt), 4);
    maybe_reverse_bytes(tzh_ttisstdcnt);
    inf.read(reinterpret_cast<char*>(&tzh_leapcnt), 4);
    maybe_reverse_bytes(tzh_leapcnt);
    inf.read(reinterpret_cast<char*>(&tzh_timecnt), 4);
    maybe_reverse_bytes(tzh_timecnt);
    inf.read(reinterpret_cast<char*>(&tzh_typecnt), 4);
    maybe_reverse_bytes(tzh_typecnt);
    inf.read(reinterpret_cast<char*>(&tzh_charcnt), 4);
    maybe_reverse_bytes(tzh_charcnt);
}

template <class TimeType>
std::vector<transition>
load_transitions(std::istream& inf, std::int32_t tzh_timecnt)
{
    // Read transitions
    using namespace std::chrono;
    std::vector<transition> transitions;
    transitions.reserve(static_cast<unsigned>(tzh_timecnt));
    for (std::int32_t i = 0; i < tzh_timecnt; ++i)
    {
        TimeType t;
        inf.read(reinterpret_cast<char*>(&t), sizeof(t));
        maybe_reverse_bytes(t);
        transitions.emplace_back(sys_seconds{seconds{t}});
        if (transitions.back().timepoint < min_seconds) {
            transitions.back().timepoint = min_seconds;
        }
    }
    return transitions;
}

std::vector<std::uint8_t>
load_indices(std::istream& inf, std::int32_t tzh_timecnt)
{
    // Read indices
    std::vector<std::uint8_t> indices;
    indices.reserve(static_cast<unsigned>(tzh_timecnt));
    for (std::int32_t i = 0; i < tzh_timecnt; ++i)
    {
        std::uint8_t t;
        inf.read(reinterpret_cast<char*>(&t), sizeof(t));
        indices.emplace_back(t);
    }
    return indices;
}

std::vector<ttinfo>
load_ttinfo(std::istream& inf, std::int32_t tzh_typecnt)
{
    // Read ttinfo
    std::vector<ttinfo> ttinfos;
    ttinfos.reserve(static_cast<unsigned>(tzh_typecnt));
    for (std::int32_t i = 0; i < tzh_typecnt; ++i)
    {
        ttinfo t;
        inf.read(reinterpret_cast<char*>(&t), 6);
        maybe_reverse_bytes(t.tt_gmtoff);
        ttinfos.emplace_back(t);
    }
    return ttinfos;
}

std::string
load_abbreviations(std::istream& inf, std::int32_t tzh_charcnt)
{
    // Read abbreviations
    std::string abbrev;
    abbrev.resize(static_cast<unsigned>(tzh_charcnt), '\0');
    inf.read(&abbrev[0], tzh_charcnt);
    return abbrev;
}

namespace {
class leap_second
{
private:
    sys_seconds date_;

public:
    explicit leap_second(const sys_seconds& s) : date_(s) {}

    sys_seconds date() const {return date_;}

    friend bool operator==(const leap_second& x, const leap_second& y) {return x.date_ == y.date_;}
    friend bool operator< (const leap_second& x, const leap_second& y) {return x.date_ < y.date_;}

    template <class Duration>
    friend
    bool
    operator==(const leap_second& x, const sys_time<Duration>& y)
    {
        return x.date_ == y;
    }

    template <class Duration>
    friend
    bool
    operator< (const leap_second& x, const sys_time<Duration>& y)
    {
        return x.date_ < y;
    }

    template <class Duration>
    friend
    bool
    operator< (const sys_time<Duration>& x, const leap_second& y)
    {
        return x < y.date_;
    }

    friend std::ostream& operator<<(std::ostream& os, const leap_second& x);
};
}

template <class TimeType>
std::vector<leap_second>
load_leaps(std::istream& inf, std::int32_t tzh_leapcnt)
{
    // Read tzh_leapcnt pairs
    std::vector<leap_second> leap_seconds;
    leap_seconds.reserve(static_cast<std::size_t>(tzh_leapcnt));
    for (std::int32_t i = 0; i < tzh_leapcnt; ++i)
    {
        TimeType     t0;
        std::int32_t t1;
        inf.read(reinterpret_cast<char*>(&t0), sizeof(t0));
        inf.read(reinterpret_cast<char*>(&t1), sizeof(t1));
        maybe_reverse_bytes(t0);
        maybe_reverse_bytes(t1);
        leap_seconds.emplace_back(sys_seconds{std::chrono::seconds{t0 - (t1-1)}});
    }
    return leap_seconds;
}

template <class TimeType>
void
load_data(std::istream& inf,
                     std::vector<transition>& transitions, std::vector<expanded_ttinfo>& ttinfos,
                     std::vector<leap_second>& leap_seconds,
                     std::int32_t tzh_leapcnt, std::int32_t tzh_timecnt,
                     std::int32_t tzh_typecnt, std::int32_t tzh_charcnt)
{
    using namespace std::chrono;
    transitions = load_transitions<TimeType>(inf, tzh_timecnt);
    auto indices = load_indices(inf, tzh_timecnt);
    auto infos = load_ttinfo(inf, tzh_typecnt);
    auto abbrev = load_abbreviations(inf, tzh_charcnt);
    if (tzh_leapcnt > 0) {
      leap_seconds = load_leaps<TimeType>(inf, tzh_leapcnt);
    }
    ttinfos.reserve(infos.size());
    for (auto& info : infos)
    {
        ttinfos.push_back({seconds{info.tt_gmtoff},
                            abbrev.c_str() + info.tt_abbrind,
                            info.tt_isdst != 0});
    }
    auto i = 0u;
    transitions.emplace(transitions.begin(), min_seconds);
    auto tf = std::find_if(ttinfos.begin(), ttinfos.end(),
                            [](const expanded_ttinfo& ti)
                                {return ti.is_dst == 0;});
    if (tf == ttinfos.end()) {
        tf = ttinfos.begin();
    }
    transitions[i].info = &*tf;
    ++i;
    for (auto j = 0u; i < transitions.size(); ++i, ++j) {
        transitions[i].info = ttinfos.data() + indices[j];
    }
}

void populate_transitions(std::vector<transition>& transitions, std::vector<expanded_ttinfo>& ttinfos, std::istream& inf) {
    using namespace std;
    using namespace std::chrono;
    inf.exceptions(std::ios::failbit | std::ios::badbit);
    load_header(inf);
    auto v = load_version(inf);
    std::int32_t tzh_ttisgmtcnt, tzh_ttisstdcnt, tzh_leapcnt,
                 tzh_timecnt,    tzh_typecnt,    tzh_charcnt;
    skip_reserve(inf);
    load_counts(inf, tzh_ttisgmtcnt, tzh_ttisstdcnt, tzh_leapcnt,
                     tzh_timecnt,    tzh_typecnt,    tzh_charcnt);
    std::vector<leap_second> leap_seconds;
    if (v == 0)
    {
        load_data<int32_t>(inf, transitions, ttinfos, leap_seconds, tzh_leapcnt, tzh_timecnt, tzh_typecnt, tzh_charcnt);
    }
    else
    {
#if !defined(NDEBUG)
        inf.ignore((4+1)*tzh_timecnt + 6*tzh_typecnt + tzh_charcnt + 8*tzh_leapcnt +
                   tzh_ttisstdcnt + tzh_ttisgmtcnt);
        load_header(inf);
        auto v2 = load_version(inf);
        assert(v == v2);
        skip_reserve(inf);
#else  // defined(NDEBUG)
        inf.ignore((4+1)*tzh_timecnt + 6*tzh_typecnt + tzh_charcnt + 8*tzh_leapcnt +
                   tzh_ttisstdcnt + tzh_ttisgmtcnt + (4+1+15));
#endif  // defined(NDEBUG)
        load_counts(inf, tzh_ttisgmtcnt, tzh_ttisstdcnt, tzh_leapcnt,
                         tzh_timecnt,    tzh_typecnt,    tzh_charcnt);
        load_data<int64_t>(inf, transitions, ttinfos, leap_seconds, tzh_leapcnt, tzh_timecnt, tzh_typecnt, tzh_charcnt);
    }
    if (tzh_leapcnt > 0)
    {
        auto itr = leap_seconds.begin();
        auto l = itr->date();
        seconds leap_count{0};
        for (auto t = std::upper_bound(transitions.begin(), transitions.end(), l,
                                       [](const sys_seconds& x, const transition& ct)
                                       {
                                           return x < ct.timepoint;
                                       });
                  t != transitions.end(); ++t)
        {
            while (t->timepoint >= l)
            {
                ++leap_count;
                if (++itr == leap_seconds.end()) {
                    l = sys_days(max_year/max_day);
                } else {
                    l = itr->date() + leap_count;
                }
            }
            t->timepoint -= leap_count;
        }
    }
    auto b = transitions.begin();
    auto i = transitions.end();
    if (i != b)
    {
        for (--i; i != b; --i)
        {
            if (i->info->offset == i[-1].info->offset &&
                i->info->abbrev == i[-1].info->abbrev &&
                i->info->is_dst == i[-1].info->is_dst) {
                i = transitions.erase(i);
            }
        }
    }
}
}
