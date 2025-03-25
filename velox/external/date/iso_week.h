#ifndef ISO_WEEK_H
#define ISO_WEEK_H

// The MIT License (MIT)
//
// Copyright (c) 2015, 2016, 2017 Howard Hinnant
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

#include "date.h"

#include <climits>

namespace facebook
{
namespace velox
{
namespace date
{
namespace iso_week
{

// y/wn/wd
// wn/wd/y
// wd/wn/y

using days = date::days;
using weeks = date::weeks;
using years = date::years;

// time_point

using sys_days = date::sys_days;
using local_days = date::local_days;

// types

struct last_week
{
    explicit last_week() = default;
};

class weeknum;

class year_weeknum;
class year_lastweek;
class weeknum_weekday;
class lastweek_weekday;

class year_weeknum_weekday;
class year_lastweek_weekday;

// date composition operators

CONSTCD11 year_weeknum operator/(const year& y, const weeknum& wn) NOEXCEPT;
CONSTCD11 year_weeknum operator/(const year& y, int            wn) NOEXCEPT;

CONSTCD11 year_lastweek operator/(const year& y, last_week      wn) NOEXCEPT;

CONSTCD11 weeknum_weekday operator/(const weeknum& wn, const weekday& wd) NOEXCEPT;
CONSTCD11 weeknum_weekday operator/(const weeknum& wn, int            wd) NOEXCEPT;
CONSTCD11 weeknum_weekday operator/(const weekday& wd, const weeknum& wn) NOEXCEPT;
CONSTCD11 weeknum_weekday operator/(const weekday& wd, int            wn) NOEXCEPT;

CONSTCD11 lastweek_weekday operator/(const last_week& wn, const weekday& wd) NOEXCEPT;
CONSTCD11 lastweek_weekday operator/(const last_week& wn, int            wd) NOEXCEPT;
CONSTCD11 lastweek_weekday operator/(const weekday& wd, const last_week& wn) NOEXCEPT;

CONSTCD11 year_weeknum_weekday operator/(const year_weeknum& ywn, const weekday& wd) NOEXCEPT;
CONSTCD11 year_weeknum_weekday operator/(const year_weeknum& ywn, int            wd) NOEXCEPT;
CONSTCD11 year_weeknum_weekday operator/(const weeknum_weekday& wnwd, const year& y) NOEXCEPT;
CONSTCD11 year_weeknum_weekday operator/(const weeknum_weekday& wnwd, int         y) NOEXCEPT;

CONSTCD11 year_lastweek_weekday operator/(const year_lastweek& ylw, const weekday& wd) NOEXCEPT;
CONSTCD11 year_lastweek_weekday operator/(const year_lastweek& ylw, int            wd) NOEXCEPT;

CONSTCD11 year_lastweek_weekday operator/(const lastweek_weekday& lwwd, const year& y) NOEXCEPT;
CONSTCD11 year_lastweek_weekday operator/(const lastweek_weekday& lwwd, int         y) NOEXCEPT;

// weeknum

class weeknum
{
    unsigned char wn_;

public:
    explicit CONSTCD11 weeknum(unsigned wn) NOEXCEPT;

    weeknum& operator++()    NOEXCEPT;
    weeknum  operator++(int) NOEXCEPT;
    weeknum& operator--()    NOEXCEPT;
    weeknum  operator--(int) NOEXCEPT;

    weeknum& operator+=(const weeks& y) NOEXCEPT;
    weeknum& operator-=(const weeks& y) NOEXCEPT;

    CONSTCD11 explicit operator unsigned() const NOEXCEPT;
    CONSTCD11 bool ok() const NOEXCEPT;
};

CONSTCD11 bool operator==(const weeknum& x, const weeknum& y) NOEXCEPT;
CONSTCD11 bool operator!=(const weeknum& x, const weeknum& y) NOEXCEPT;
CONSTCD11 bool operator< (const weeknum& x, const weeknum& y) NOEXCEPT;
CONSTCD11 bool operator> (const weeknum& x, const weeknum& y) NOEXCEPT;
CONSTCD11 bool operator<=(const weeknum& x, const weeknum& y) NOEXCEPT;
CONSTCD11 bool operator>=(const weeknum& x, const weeknum& y) NOEXCEPT;

CONSTCD11 weeknum  operator+(const weeknum& x, const weeks&   y) NOEXCEPT;
CONSTCD11 weeknum  operator+(const weeks&   x, const weeknum& y) NOEXCEPT;
CONSTCD11 weeknum  operator-(const weeknum& x, const weeks&   y) NOEXCEPT;
CONSTCD11 weeks    operator-(const weeknum& x, const weeknum& y) NOEXCEPT;

template<class CharT, class Traits>
std::basic_ostream<CharT, Traits>&
operator<<(std::basic_ostream<CharT, Traits>& os, const weeknum& wn);

// year_weeknum

class year_weeknum
{
    date::year    y_;
    iso_week::weeknum wn_;

public:
    CONSTCD11 year_weeknum(const date::year& y, const iso_week::weeknum& wn) NOEXCEPT;

    CONSTCD11 date::year    year()    const NOEXCEPT;
    CONSTCD11 iso_week::weeknum weeknum() const NOEXCEPT;

    year_weeknum& operator+=(const years& dy) NOEXCEPT;
    year_weeknum& operator-=(const years& dy) NOEXCEPT;

    CONSTCD11 bool ok() const NOEXCEPT;
};

CONSTCD11 bool operator==(const year_weeknum& x, const year_weeknum& y) NOEXCEPT;
CONSTCD11 bool operator!=(const year_weeknum& x, const year_weeknum& y) NOEXCEPT;
CONSTCD11 bool operator< (const year_weeknum& x, const year_weeknum& y) NOEXCEPT;
CONSTCD11 bool operator> (const year_weeknum& x, const year_weeknum& y) NOEXCEPT;
CONSTCD11 bool operator<=(const year_weeknum& x, const year_weeknum& y) NOEXCEPT;
CONSTCD11 bool operator>=(const year_weeknum& x, const year_weeknum& y) NOEXCEPT;

CONSTCD11 year_weeknum operator+(const year_weeknum& ym, const years& dy) NOEXCEPT;
CONSTCD11 year_weeknum operator+(const years& dy, const year_weeknum& ym) NOEXCEPT;
CONSTCD11 year_weeknum operator-(const year_weeknum& ym, const years& dy) NOEXCEPT;

template<class CharT, class Traits>
std::basic_ostream<CharT, Traits>&
operator<<(std::basic_ostream<CharT, Traits>& os, const year_weeknum& ym);

// year_lastweek

class year_lastweek
{
    date::year    y_;

public:
    CONSTCD11 explicit year_lastweek(const date::year& y) NOEXCEPT;

    CONSTCD11 date::year    year()    const NOEXCEPT;
    CONSTCD14 iso_week::weeknum weeknum() const NOEXCEPT;

    year_lastweek& operator+=(const years& dy) NOEXCEPT;
    year_lastweek& operator-=(const years& dy) NOEXCEPT;

    CONSTCD11 bool ok() const NOEXCEPT;
};

CONSTCD11 bool operator==(const year_lastweek& x, const year_lastweek& y) NOEXCEPT;
CONSTCD11 bool operator!=(const year_lastweek& x, const year_lastweek& y) NOEXCEPT;
CONSTCD11 bool operator< (const year_lastweek& x, const year_lastweek& y) NOEXCEPT;
CONSTCD11 bool operator> (const year_lastweek& x, const year_lastweek& y) NOEXCEPT;
CONSTCD11 bool operator<=(const year_lastweek& x, const year_lastweek& y) NOEXCEPT;
CONSTCD11 bool operator>=(const year_lastweek& x, const year_lastweek& y) NOEXCEPT;

CONSTCD11 year_lastweek operator+(const year_lastweek& ym, const years& dy) NOEXCEPT;
CONSTCD11 year_lastweek operator+(const years& dy, const year_lastweek& ym) NOEXCEPT;
CONSTCD11 year_lastweek operator-(const year_lastweek& ym, const years& dy) NOEXCEPT;

template<class CharT, class Traits>
std::basic_ostream<CharT, Traits>&
operator<<(std::basic_ostream<CharT, Traits>& os, const year_lastweek& ym);

// weeknum_weekday

class weeknum_weekday
{
    iso_week::weeknum wn_;
    date::weekday wd_;

public:
    CONSTCD11 weeknum_weekday(const iso_week::weeknum& wn,
                              const date::weekday& wd) NOEXCEPT;

    CONSTCD11 iso_week::weeknum weeknum() const NOEXCEPT;
    CONSTCD11 date::weekday weekday() const NOEXCEPT;

    CONSTCD14 bool ok() const NOEXCEPT;
};

CONSTCD11 bool operator==(const weeknum_weekday& x, const weeknum_weekday& y) NOEXCEPT;
CONSTCD11 bool operator!=(const weeknum_weekday& x, const weeknum_weekday& y) NOEXCEPT;
CONSTCD11 bool operator< (const weeknum_weekday& x, const weeknum_weekday& y) NOEXCEPT;
CONSTCD11 bool operator> (const weeknum_weekday& x, const weeknum_weekday& y) NOEXCEPT;
CONSTCD11 bool operator<=(const weeknum_weekday& x, const weeknum_weekday& y) NOEXCEPT;
CONSTCD11 bool operator>=(const weeknum_weekday& x, const weeknum_weekday& y) NOEXCEPT;

template<class CharT, class Traits>
std::basic_ostream<CharT, Traits>&
operator<<(std::basic_ostream<CharT, Traits>& os, const weeknum_weekday& md);

// lastweek_weekday

class lastweek_weekday
{
    date::weekday wd_;

public:
    CONSTCD11 explicit lastweek_weekday(const date::weekday& wd) NOEXCEPT;

    CONSTCD11 date::weekday weekday() const NOEXCEPT;

    CONSTCD14 bool ok() const NOEXCEPT;
};

CONSTCD11 bool operator==(const lastweek_weekday& x, const lastweek_weekday& y) NOEXCEPT;
CONSTCD11 bool operator!=(const lastweek_weekday& x, const lastweek_weekday& y) NOEXCEPT;
CONSTCD11 bool operator< (const lastweek_weekday& x, const lastweek_weekday& y) NOEXCEPT;
CONSTCD11 bool operator> (const lastweek_weekday& x, const lastweek_weekday& y) NOEXCEPT;
CONSTCD11 bool operator<=(const lastweek_weekday& x, const lastweek_weekday& y) NOEXCEPT;
CONSTCD11 bool operator>=(const lastweek_weekday& x, const lastweek_weekday& y) NOEXCEPT;

template<class CharT, class Traits>
std::basic_ostream<CharT, Traits>&
operator<<(std::basic_ostream<CharT, Traits>& os, const lastweek_weekday& md);

// year_lastweek_weekday

class year_lastweek_weekday
{
    date::year    y_;
    date::weekday wd_;

public:
    CONSTCD11 year_lastweek_weekday(const date::year& y,
                                    const date::weekday& wd) NOEXCEPT;

    year_lastweek_weekday& operator+=(const years& y) NOEXCEPT;
    year_lastweek_weekday& operator-=(const years& y) NOEXCEPT;

    CONSTCD11 date::year    year()    const NOEXCEPT;
    CONSTCD14 iso_week::weeknum weeknum() const NOEXCEPT;
    CONSTCD11 date::weekday weekday() const NOEXCEPT;

    CONSTCD14 operator sys_days() const NOEXCEPT;
    CONSTCD14 explicit operator local_days() const NOEXCEPT;
    CONSTCD11 bool ok() const NOEXCEPT;
};

CONSTCD11 bool operator==(const year_lastweek_weekday& x, const year_lastweek_weekday& y) NOEXCEPT;
CONSTCD11 bool operator!=(const year_lastweek_weekday& x, const year_lastweek_weekday& y) NOEXCEPT;
CONSTCD11 bool operator< (const year_lastweek_weekday& x, const year_lastweek_weekday& y) NOEXCEPT;
CONSTCD11 bool operator> (const year_lastweek_weekday& x, const year_lastweek_weekday& y) NOEXCEPT;
CONSTCD11 bool operator<=(const year_lastweek_weekday& x, const year_lastweek_weekday& y) NOEXCEPT;
CONSTCD11 bool operator>=(const year_lastweek_weekday& x, const year_lastweek_weekday& y) NOEXCEPT;

CONSTCD11 year_lastweek_weekday operator+(const year_lastweek_weekday& ywnwd, const years& y) NOEXCEPT;
CONSTCD11 year_lastweek_weekday operator+(const years& y, const year_lastweek_weekday& ywnwd) NOEXCEPT;
CONSTCD11 year_lastweek_weekday operator-(const year_lastweek_weekday& ywnwd, const years& y) NOEXCEPT;

template<class CharT, class Traits>
std::basic_ostream<CharT, Traits>&
operator<<(std::basic_ostream<CharT, Traits>& os, const year_lastweek_weekday& ywnwd);

// class year_weeknum_weekday

class year_weeknum_weekday
{
    date::year    y_;
    iso_week::weeknum wn_;
    date::weekday wd_;

public:
    CONSTCD11 year_weeknum_weekday(const date::year& y, const iso_week::weeknum& wn,
                                   const date::weekday& wd) NOEXCEPT;
    CONSTCD14 year_weeknum_weekday(const year_lastweek_weekday& ylwwd) NOEXCEPT;
    CONSTCD14 year_weeknum_weekday(const sys_days& dp) NOEXCEPT;
    CONSTCD14 explicit year_weeknum_weekday(const local_days& dp) NOEXCEPT;

    year_weeknum_weekday& operator+=(const years& y) NOEXCEPT;
    year_weeknum_weekday& operator-=(const years& y) NOEXCEPT;

    CONSTCD11 date::year    year()    const NOEXCEPT;
    CONSTCD11 iso_week::weeknum weeknum() const NOEXCEPT;
    CONSTCD11 date::weekday weekday() const NOEXCEPT;

    CONSTCD14 operator sys_days() const NOEXCEPT;
    CONSTCD14 explicit operator local_days() const NOEXCEPT;
    CONSTCD14 bool ok() const NOEXCEPT;

private:
    static CONSTCD14 year_weeknum_weekday from_days(days dp) NOEXCEPT;
};

CONSTCD11 bool operator==(const year_weeknum_weekday& x, const year_weeknum_weekday& y) NOEXCEPT;
CONSTCD11 bool operator!=(const year_weeknum_weekday& x, const year_weeknum_weekday& y) NOEXCEPT;
CONSTCD11 bool operator< (const year_weeknum_weekday& x, const year_weeknum_weekday& y) NOEXCEPT;
CONSTCD11 bool operator> (const year_weeknum_weekday& x, const year_weeknum_weekday& y) NOEXCEPT;
CONSTCD11 bool operator<=(const year_weeknum_weekday& x, const year_weeknum_weekday& y) NOEXCEPT;
CONSTCD11 bool operator>=(const year_weeknum_weekday& x, const year_weeknum_weekday& y) NOEXCEPT;

CONSTCD11 year_weeknum_weekday operator+(const year_weeknum_weekday& ywnwd, const years& y) NOEXCEPT;
CONSTCD11 year_weeknum_weekday operator+(const years& y, const year_weeknum_weekday& ywnwd) NOEXCEPT;
CONSTCD11 year_weeknum_weekday operator-(const year_weeknum_weekday& ywnwd, const years& y) NOEXCEPT;

template<class CharT, class Traits>
std::basic_ostream<CharT, Traits>&
operator<<(std::basic_ostream<CharT, Traits>& os, const year_weeknum_weekday& ywnwd);

//----------------+
// Implementation |
//----------------+
#if !defined(_MSC_VER) || (_MSC_VER >= 1900)
inline namespace literals
{

CONSTCD11
inline
date::year
operator "" _y(unsigned long long y) NOEXCEPT
{
    return date::year(static_cast<int64_t>(y));
}

CONSTCD11
inline
iso_week::weeknum
operator "" _w(unsigned long long wn) NOEXCEPT
{
    return iso_week::weeknum(static_cast<unsigned>(wn));
}

#endif  // !defined(_MSC_VER) || (_MSC_VER >= 1900)

CONSTDATA iso_week::last_week last{};

CONSTDATA date::weekday sun{7u};
CONSTDATA date::weekday mon{1u};
CONSTDATA date::weekday tue{2u};
CONSTDATA date::weekday wed{3u};
CONSTDATA date::weekday thu{4u};
CONSTDATA date::weekday fri{5u};
CONSTDATA date::weekday sat{6u};

#if !defined(_MSC_VER) || (_MSC_VER >= 1900)
}  // inline namespace literals
#endif

// weeknum

CONSTCD11
inline
weeknum::weeknum(unsigned wn) NOEXCEPT
    : wn_(static_cast<decltype(wn_)>(wn))
    {}

inline weeknum& weeknum::operator++() NOEXCEPT {++wn_; return *this;}
inline weeknum weeknum::operator++(int) NOEXCEPT {auto tmp(*this); ++(*this); return tmp;}
inline weeknum& weeknum::operator--() NOEXCEPT {--wn_; return *this;}
inline weeknum weeknum::operator--(int) NOEXCEPT {auto tmp(*this); --(*this); return tmp;}

inline
weeknum&
weeknum::operator+=(const weeks& y) NOEXCEPT
{
    *this = *this + y;
    return *this;
}

inline
weeknum&
weeknum::operator-=(const weeks& y) NOEXCEPT
{
    *this = *this - y;
    return *this;
}

CONSTCD11 inline weeknum::operator unsigned() const NOEXCEPT {return wn_;}
CONSTCD11 inline bool weeknum::ok() const NOEXCEPT {return 1 <= wn_ && wn_ <= 53;}

CONSTCD11
inline
bool
operator==(const weeknum& x, const weeknum& y) NOEXCEPT
{
    return static_cast<unsigned>(x) == static_cast<unsigned>(y);
}

CONSTCD11
inline
bool
operator!=(const weeknum& x, const weeknum& y) NOEXCEPT
{
    return !(x == y);
}

CONSTCD11
inline
bool
operator<(const weeknum& x, const weeknum& y) NOEXCEPT
{
    return static_cast<unsigned>(x) < static_cast<unsigned>(y);
}

CONSTCD11
inline
bool
operator>(const weeknum& x, const weeknum& y) NOEXCEPT
{
    return y < x;
}

CONSTCD11
inline
bool
operator<=(const weeknum& x, const weeknum& y) NOEXCEPT
{
    return !(y < x);
}

CONSTCD11
inline
bool
operator>=(const weeknum& x, const weeknum& y) NOEXCEPT
{
    return !(x < y);
}

CONSTCD11
inline
weeks
operator-(const weeknum& x, const weeknum& y) NOEXCEPT
{
    return weeks{static_cast<weeks::rep>(static_cast<unsigned>(x)) -
                 static_cast<weeks::rep>(static_cast<unsigned>(y))};
}

CONSTCD11
inline
weeknum
operator+(const weeknum& x, const weeks& y) NOEXCEPT
{
    return weeknum{static_cast<unsigned>(x) + static_cast<unsigned>(y.count())};
}

CONSTCD11
inline
weeknum
operator+(const weeks& x, const weeknum& y) NOEXCEPT
{
    return y + x;
}

CONSTCD11
inline
weeknum
operator-(const weeknum& x, const weeks& y) NOEXCEPT
{
    return x + -y;
}

template<class CharT, class Traits>
inline
std::basic_ostream<CharT, Traits>&
operator<<(std::basic_ostream<CharT, Traits>& os, const weeknum& wn)
{
    date::detail::save_ostream<CharT, Traits> _(os);
    os << 'W';
    os.fill('0');
    os.flags(std::ios::dec | std::ios::right);
    os.width(2);
    os << static_cast<unsigned>(wn);
    return os;
}

// year_weeknum

CONSTCD11
inline
year_weeknum::year_weeknum(const date::year& y, const iso_week::weeknum& wn) NOEXCEPT
    : y_(y)
    , wn_(wn)
    {}

CONSTCD11 inline year year_weeknum::year() const NOEXCEPT {return y_;}
CONSTCD11 inline weeknum year_weeknum::weeknum() const NOEXCEPT {return wn_;}
CONSTCD11 inline bool year_weeknum::ok() const NOEXCEPT
{
    return y_.ok() && 1u <= static_cast<unsigned>(wn_) && wn_ <= (y_/last).weeknum();
}

inline
year_weeknum&
year_weeknum::operator+=(const years& dy) NOEXCEPT
{
    *this = *this + dy;
    return *this;
}

inline
year_weeknum&
year_weeknum::operator-=(const years& dy) NOEXCEPT
{
    *this = *this - dy;
    return *this;
}

CONSTCD11
inline
bool
operator==(const year_weeknum& x, const year_weeknum& y) NOEXCEPT
{
    return x.year() == y.year() && x.weeknum() == y.weeknum();
}

CONSTCD11
inline
bool
operator!=(const year_weeknum& x, const year_weeknum& y) NOEXCEPT
{
    return !(x == y);
}

CONSTCD11
inline
bool
operator<(const year_weeknum& x, const year_weeknum& y) NOEXCEPT
{
    return x.year() < y.year() ? true
        : (x.year() > y.year() ? false
        : (x.weeknum() < y.weeknum()));
}

CONSTCD11
inline
bool
operator>(const year_weeknum& x, const year_weeknum& y) NOEXCEPT
{
    return y < x;
}

CONSTCD11
inline
bool
operator<=(const year_weeknum& x, const year_weeknum& y) NOEXCEPT
{
    return !(y < x);
}

CONSTCD11
inline
bool
operator>=(const year_weeknum& x, const year_weeknum& y) NOEXCEPT
{
    return !(x < y);
}

CONSTCD11
inline
year_weeknum
operator+(const year_weeknum& ym, const years& dy) NOEXCEPT
{
    return (ym.year() + dy) / ym.weeknum();
}

CONSTCD11
inline
year_weeknum
operator+(const years& dy, const year_weeknum& ym) NOEXCEPT
{
    return ym + dy;
}

CONSTCD11
inline
year_weeknum
operator-(const year_weeknum& ym, const years& dy) NOEXCEPT
{
    return ym + -dy;
}

template<class CharT, class Traits>
inline
std::basic_ostream<CharT, Traits>&
operator<<(std::basic_ostream<CharT, Traits>& os, const year_weeknum& ywn)
{
    return os << ywn.year() << '-' << ywn.weeknum();
}


// year_lastweek

CONSTCD11
inline
year_lastweek::year_lastweek(const date::year& y) NOEXCEPT
    : y_(y)
    {}

CONSTCD11 inline year year_lastweek::year() const NOEXCEPT {return y_;}

CONSTCD14
inline
weeknum
year_lastweek::weeknum() const NOEXCEPT
{
    return iso_week::weeknum(y_.is_leap() ? 53u : 52u);
}

CONSTCD11 inline bool year_lastweek::ok() const NOEXCEPT {return y_.ok();}

inline
year_lastweek&
year_lastweek::operator+=(const years& dy) NOEXCEPT
{
    *this = *this + dy;
    return *this;
}

inline
year_lastweek&
year_lastweek::operator-=(const years& dy) NOEXCEPT
{
    *this = *this - dy;
    return *this;
}

CONSTCD11
inline
bool
operator==(const year_lastweek& x, const year_lastweek& y) NOEXCEPT
{
    return x.year() == y.year();
}

CONSTCD11
inline
bool
operator!=(const year_lastweek& x, const year_lastweek& y) NOEXCEPT
{
    return !(x == y);
}

CONSTCD11
inline
bool
operator<(const year_lastweek& x, const year_lastweek& y) NOEXCEPT
{
    return x.year() < y.year();
}

CONSTCD11
inline
bool
operator>(const year_lastweek& x, const year_lastweek& y) NOEXCEPT
{
    return y < x;
}

CONSTCD11
inline
bool
operator<=(const year_lastweek& x, const year_lastweek& y) NOEXCEPT
{
    return !(y < x);
}

CONSTCD11
inline
bool
operator>=(const year_lastweek& x, const year_lastweek& y) NOEXCEPT
{
    return !(x < y);
}

CONSTCD11
inline
year_lastweek
operator+(const year_lastweek& ym, const years& dy) NOEXCEPT
{
    return year_lastweek{ym.year() + dy};
}

CONSTCD11
inline
year_lastweek
operator+(const years& dy, const year_lastweek& ym) NOEXCEPT
{
    return ym + dy;
}

CONSTCD11
inline
year_lastweek
operator-(const year_lastweek& ym, const years& dy) NOEXCEPT
{
    return ym + -dy;
}

template<class CharT, class Traits>
inline
std::basic_ostream<CharT, Traits>&
operator<<(std::basic_ostream<CharT, Traits>& os, const year_lastweek& ywn)
{
    return os << ywn.year() << "-W last";
}

// weeknum_weekday

CONSTCD11
inline
weeknum_weekday::weeknum_weekday(const iso_week::weeknum& wn,
                                 const date::weekday& wd) NOEXCEPT
    : wn_(wn)
    , wd_(wd)
    {}

CONSTCD11 inline weeknum weeknum_weekday::weeknum() const NOEXCEPT {return wn_;}
CONSTCD11 inline weekday weeknum_weekday::weekday() const NOEXCEPT {return wd_;}

CONSTCD14
inline
bool
weeknum_weekday::ok() const NOEXCEPT
{
    return wn_.ok() && wd_.ok();
}

CONSTCD11
inline
bool
operator==(const weeknum_weekday& x, const weeknum_weekday& y) NOEXCEPT
{
    return x.weeknum() == y.weeknum() && x.weekday() == y.weekday();
}

CONSTCD11
inline
bool
operator!=(const weeknum_weekday& x, const weeknum_weekday& y) NOEXCEPT
{
    return !(x == y);
}

CONSTCD11
inline
bool
operator<(const weeknum_weekday& x, const weeknum_weekday& y) NOEXCEPT
{
    return x.weeknum() < y.weeknum() ? true
        : (x.weeknum() > y.weeknum() ? false
        : (static_cast<unsigned>(x.weekday()) < static_cast<unsigned>(y.weekday())));
}

CONSTCD11
inline
bool
operator>(const weeknum_weekday& x, const weeknum_weekday& y) NOEXCEPT
{
    return y < x;
}

CONSTCD11
inline
bool
operator<=(const weeknum_weekday& x, const weeknum_weekday& y) NOEXCEPT
{
    return !(y < x);
}

CONSTCD11
inline
bool
operator>=(const weeknum_weekday& x, const weeknum_weekday& y) NOEXCEPT
{
    return !(x < y);
}

template<class CharT, class Traits>
inline
std::basic_ostream<CharT, Traits>&
operator<<(std::basic_ostream<CharT, Traits>& os, const weeknum_weekday& md)
{
    return os << md.weeknum() << '-' << md.weekday();
}

// lastweek_weekday

CONSTCD11
inline
lastweek_weekday::lastweek_weekday(const date::weekday& wd) NOEXCEPT
    : wd_(wd)
    {}

CONSTCD11 inline weekday lastweek_weekday::weekday() const NOEXCEPT {return wd_;}

CONSTCD14
inline
bool
lastweek_weekday::ok() const NOEXCEPT
{
    return wd_.ok();
}

CONSTCD11
inline
bool
operator==(const lastweek_weekday& x, const lastweek_weekday& y) NOEXCEPT
{
    return x.weekday() == y.weekday();
}

CONSTCD11
inline
bool
operator!=(const lastweek_weekday& x, const lastweek_weekday& y) NOEXCEPT
{
    return !(x == y);
}

CONSTCD11
inline
bool
operator<(const lastweek_weekday& x, const lastweek_weekday& y) NOEXCEPT
{
    return static_cast<unsigned>(x.weekday()) < static_cast<unsigned>(y.weekday());
}

CONSTCD11
inline
bool
operator>(const lastweek_weekday& x, const lastweek_weekday& y) NOEXCEPT
{
    return y < x;
}

CONSTCD11
inline
bool
operator<=(const lastweek_weekday& x, const lastweek_weekday& y) NOEXCEPT
{
    return !(y < x);
}

CONSTCD11
inline
bool
operator>=(const lastweek_weekday& x, const lastweek_weekday& y) NOEXCEPT
{
    return !(x < y);
}

template<class CharT, class Traits>
inline
std::basic_ostream<CharT, Traits>&
operator<<(std::basic_ostream<CharT, Traits>& os, const lastweek_weekday& md)
{
    return os << "W last-" << md.weekday();
}

// year_lastweek_weekday

CONSTCD11
inline
year_lastweek_weekday::year_lastweek_weekday(const date::year& y,
                                             const date::weekday& wd) NOEXCEPT
    : y_(y)
    , wd_(wd)
    {}

inline
year_lastweek_weekday&
year_lastweek_weekday::operator+=(const years& y) NOEXCEPT
{
    *this = *this + y;
    return *this;
}

inline
year_lastweek_weekday&
year_lastweek_weekday::operator-=(const years& y) NOEXCEPT
{
    *this = *this - y;
    return *this;
}

CONSTCD11 inline year year_lastweek_weekday::year() const NOEXCEPT {return y_;}

CONSTCD14
inline
weeknum
year_lastweek_weekday::weeknum() const NOEXCEPT
{
    return (y_ / last).weeknum();
}

CONSTCD11 inline weekday year_lastweek_weekday::weekday() const NOEXCEPT {return wd_;}

CONSTCD14
inline
year_lastweek_weekday::operator sys_days() const NOEXCEPT
{
    return sys_days(date::year{static_cast<int64_t>(y_)}/date::dec/date::thu[date::last])
         + (sun - thu) - (sun - wd_);
}

CONSTCD14
inline
year_lastweek_weekday::operator local_days() const NOEXCEPT
{
    return local_days(date::year{static_cast<int64_t>(y_)}/date::dec/date::thu[date::last])
         + (sun - thu) - (sun - wd_);
}

CONSTCD11
inline
bool
year_lastweek_weekday::ok() const NOEXCEPT
{
    return y_.ok() && wd_.ok();
}

CONSTCD11
inline
bool
operator==(const year_lastweek_weekday& x, const year_lastweek_weekday& y) NOEXCEPT
{
    return x.year() == y.year() && x.weekday() == y.weekday();
}

CONSTCD11
inline
bool
operator!=(const year_lastweek_weekday& x, const year_lastweek_weekday& y) NOEXCEPT
{
    return !(x == y);
}

CONSTCD11
inline
bool
operator<(const year_lastweek_weekday& x, const year_lastweek_weekday& y) NOEXCEPT
{
    return x.year() < y.year() ? true
        : (x.year() > y.year() ? false
        : (static_cast<unsigned>(x.weekday()) < static_cast<unsigned>(y.weekday())));
}

CONSTCD11
inline
bool
operator>(const year_lastweek_weekday& x, const year_lastweek_weekday& y) NOEXCEPT
{
    return y < x;
}

CONSTCD11
inline
bool
operator<=(const year_lastweek_weekday& x, const year_lastweek_weekday& y) NOEXCEPT
{
    return !(y < x);
}

CONSTCD11
inline
bool
operator>=(const year_lastweek_weekday& x, const year_lastweek_weekday& y) NOEXCEPT
{
    return !(x < y);
}

CONSTCD11
inline
year_lastweek_weekday
operator+(const year_lastweek_weekday& ywnwd, const years& y)  NOEXCEPT
{
    return (ywnwd.year() + y) / last / ywnwd.weekday();
}

CONSTCD11
inline
year_lastweek_weekday
operator+(const years& y, const year_lastweek_weekday& ywnwd)  NOEXCEPT
{
    return ywnwd + y;
}

CONSTCD11
inline
year_lastweek_weekday
operator-(const year_lastweek_weekday& ywnwd, const years& y)  NOEXCEPT
{
    return ywnwd + -y;
}

template<class CharT, class Traits>
inline
std::basic_ostream<CharT, Traits>&
operator<<(std::basic_ostream<CharT, Traits>& os, const year_lastweek_weekday& ywnwd)
{
    return os << ywnwd.year() << "-W last-" << ywnwd.weekday();
}

// year_weeknum_weekday

CONSTCD11
inline
year_weeknum_weekday::year_weeknum_weekday(const date::year& y,
                                           const iso_week::weeknum& wn,
                                           const date::weekday& wd) NOEXCEPT
    : y_(y)
    , wn_(wn)
    , wd_(wd)
    {}

CONSTCD14
inline
year_weeknum_weekday::year_weeknum_weekday(const year_lastweek_weekday& ylwwd) NOEXCEPT
    : y_(ylwwd.year())
    , wn_(ylwwd.weeknum())
    , wd_(ylwwd.weekday())
    {}

CONSTCD14
inline
year_weeknum_weekday::year_weeknum_weekday(const sys_days& dp) NOEXCEPT
    : year_weeknum_weekday(from_days(dp.time_since_epoch()))
    {}

CONSTCD14
inline
year_weeknum_weekday::year_weeknum_weekday(const local_days& dp) NOEXCEPT
    : year_weeknum_weekday(from_days(dp.time_since_epoch()))
    {}

inline
year_weeknum_weekday&
year_weeknum_weekday::operator+=(const years& y) NOEXCEPT
{
    *this = *this + y;
    return *this;
}

inline
year_weeknum_weekday&
year_weeknum_weekday::operator-=(const years& y) NOEXCEPT
{
    *this = *this - y;
    return *this;
}

CONSTCD11 inline year year_weeknum_weekday::year() const NOEXCEPT {return y_;}
CONSTCD11 inline weeknum year_weeknum_weekday::weeknum() const NOEXCEPT {return wn_;}
CONSTCD11 inline weekday year_weeknum_weekday::weekday() const NOEXCEPT {return wd_;}

CONSTCD14
inline
year_weeknum_weekday::operator sys_days() const NOEXCEPT
{
    return sys_days(date::year{static_cast<int64_t>(y_)-1}/date::dec/date::thu[date::last])
         + (date::mon - date::thu) + weeks{static_cast<unsigned>(wn_)-1} + (wd_ - mon);
}

CONSTCD14
inline
year_weeknum_weekday::operator local_days() const NOEXCEPT
{
    return local_days(date::year{static_cast<int64_t>(y_)-1}/date::dec/date::thu[date::last])
         + (date::mon - date::thu) + weeks{static_cast<unsigned>(wn_)-1} + (wd_ - mon);
}

CONSTCD14
inline
bool
year_weeknum_weekday::ok() const NOEXCEPT
{
    return y_.ok() && wd_.ok() && iso_week::weeknum{1u} <= wn_ && wn_ <= year_lastweek{y_}.weeknum();
}

CONSTCD14
inline
year_weeknum_weekday
year_weeknum_weekday::from_days(days d) NOEXCEPT
{
    const auto dp = sys_days{d};
    const auto wd = date::weekday{dp};
    auto y = date::year_month_day{dp + days{3}}.year();
    auto start = sys_days((y - date::years{1})/date::dec/date::thu[date::last]) + (mon-thu);
    if (dp < start)
    {
        --y;
        start = sys_days((y - date::years{1})/date::dec/date::thu[date::last]) + (mon-thu);
    }
    const auto wn = iso_week::weeknum(
                       static_cast<unsigned>(date::trunc<weeks>(dp - start).count() + 1));
    return {date::year(static_cast<int64_t>(y)), wn, wd};
}

CONSTCD11
inline
bool
operator==(const year_weeknum_weekday& x, const year_weeknum_weekday& y) NOEXCEPT
{
    return x.year() == y.year() && x.weeknum() == y.weeknum() && x.weekday() == y.weekday();
}

CONSTCD11
inline
bool
operator!=(const year_weeknum_weekday& x, const year_weeknum_weekday& y) NOEXCEPT
{
    return !(x == y);
}

CONSTCD11
inline
bool
operator<(const year_weeknum_weekday& x, const year_weeknum_weekday& y) NOEXCEPT
{
    return x.year() < y.year() ? true
        : (x.year() > y.year() ? false
        : (x.weeknum() < y.weeknum() ? true
        : (x.weeknum() > y.weeknum() ? false
        : (static_cast<unsigned>(x.weekday()) < static_cast<unsigned>(y.weekday())))));
}

CONSTCD11
inline
bool
operator>(const year_weeknum_weekday& x, const year_weeknum_weekday& y) NOEXCEPT
{
    return y < x;
}

CONSTCD11
inline
bool
operator<=(const year_weeknum_weekday& x, const year_weeknum_weekday& y) NOEXCEPT
{
    return !(y < x);
}

CONSTCD11
inline
bool
operator>=(const year_weeknum_weekday& x, const year_weeknum_weekday& y) NOEXCEPT
{
    return !(x < y);
}

CONSTCD11
inline
year_weeknum_weekday
operator+(const year_weeknum_weekday& ywnwd, const years& y)  NOEXCEPT
{
    return (ywnwd.year() + y) / ywnwd.weeknum() / ywnwd.weekday();
}

CONSTCD11
inline
year_weeknum_weekday
operator+(const years& y, const year_weeknum_weekday& ywnwd)  NOEXCEPT
{
    return ywnwd + y;
}

CONSTCD11
inline
year_weeknum_weekday
operator-(const year_weeknum_weekday& ywnwd, const years& y)  NOEXCEPT
{
    return ywnwd + -y;
}

template<class CharT, class Traits>
inline
std::basic_ostream<CharT, Traits>&
operator<<(std::basic_ostream<CharT, Traits>& os, const year_weeknum_weekday& ywnwd)
{
    return os << ywnwd.year() << '-' << ywnwd.weeknum() << '-' << ywnwd.weekday();
}

// date composition operators

CONSTCD11
inline
year_weeknum
operator/(const year& y, const weeknum& wn) NOEXCEPT
{
    return {y, wn};
}

CONSTCD11
inline
year_weeknum
operator/(const year& y, int wn) NOEXCEPT
{
    return y/weeknum(static_cast<unsigned>(wn));
}

CONSTCD11
inline
year_lastweek
operator/(const year& y, last_week) NOEXCEPT
{
    return year_lastweek{y};
}

CONSTCD11
inline
weeknum_weekday
operator/(const weeknum& wn, const weekday& wd) NOEXCEPT
{
    return {wn, wd};
}

CONSTCD11
inline
weeknum_weekday
operator/(const weeknum& wn, int wd) NOEXCEPT
{
    return wn/weekday{static_cast<unsigned>(wd)};
}

CONSTCD11
inline
weeknum_weekday
operator/(const weekday& wd, const weeknum& wn) NOEXCEPT
{
    return wn/wd;
}

CONSTCD11
inline
weeknum_weekday
operator/(const weekday& wd, int wn) NOEXCEPT
{
    return weeknum{static_cast<unsigned>(wn)}/wd;
}

CONSTCD11
inline
lastweek_weekday
operator/(const last_week&, const weekday& wd) NOEXCEPT
{
    return lastweek_weekday{wd};
}

CONSTCD11
inline
lastweek_weekday
operator/(const last_week& wn, int wd) NOEXCEPT
{
    return wn / weekday{static_cast<unsigned>(wd)};
}

CONSTCD11
inline
lastweek_weekday
operator/(const weekday& wd, const last_week& wn) NOEXCEPT
{
    return wn / wd;
}

CONSTCD11
inline
year_weeknum_weekday
operator/(const year_weeknum& ywn, const weekday& wd) NOEXCEPT
{
    return {ywn.year(), ywn.weeknum(), wd};
}

CONSTCD11
inline
year_weeknum_weekday
operator/(const year_weeknum& ywn, int wd) NOEXCEPT
{
    return ywn / weekday(static_cast<unsigned>(wd));
}

CONSTCD11
inline
year_weeknum_weekday
operator/(const weeknum_weekday& wnwd, const year& y) NOEXCEPT
{
    return {y, wnwd.weeknum(), wnwd.weekday()};
}

CONSTCD11
inline
year_weeknum_weekday
operator/(const weeknum_weekday& wnwd, int y) NOEXCEPT
{
    return wnwd / year{y};
}

CONSTCD11
inline
year_lastweek_weekday
operator/(const year_lastweek& ylw, const weekday& wd) NOEXCEPT
{
    return {ylw.year(), wd};
}

CONSTCD11
inline
year_lastweek_weekday
operator/(const year_lastweek& ylw, int wd) NOEXCEPT
{
    return ylw / weekday(static_cast<unsigned>(wd));
}

CONSTCD11
inline
year_lastweek_weekday
operator/(const lastweek_weekday& lwwd, const year& y) NOEXCEPT
{
    return {y, lwwd.weekday()};
}

CONSTCD11
inline
year_lastweek_weekday
operator/(const lastweek_weekday& lwwd, int y) NOEXCEPT
{
    return lwwd / year{y};
}

} // namespace iso_week
} // namespace date
} // namespace velox
} // namespace facebook

#endif  // ISO_WEEK_H
