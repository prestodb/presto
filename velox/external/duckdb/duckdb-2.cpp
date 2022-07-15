// See https://raw.githubusercontent.com/duckdb/duckdb/master/LICENSE for licensing information

#include "duckdb.hpp"
#include "duckdb-internal.hpp"
#ifndef DUCKDB_AMALGAMATION
#error header mismatch
#endif









#include <cstring>
#include <cctype>
#include <algorithm>

namespace duckdb {

static_assert(sizeof(date_t) == sizeof(int32_t), "date_t was padded");

const char *Date::PINF = "infinity";  // NOLINT
const char *Date::NINF = "-infinity"; // NOLINT
const char *Date::EPOCH = "epoch";    // NOLINT

const string_t Date::MONTH_NAMES_ABBREVIATED[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun",
                                                  "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
const string_t Date::MONTH_NAMES[] = {"January", "February", "March",     "April",   "May",      "June",
                                      "July",    "August",   "September", "October", "November", "December"};
const string_t Date::DAY_NAMES[] = {"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"};
const string_t Date::DAY_NAMES_ABBREVIATED[] = {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};

const int32_t Date::NORMAL_DAYS[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
const int32_t Date::CUMULATIVE_DAYS[] = {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365};
const int32_t Date::LEAP_DAYS[] = {0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
const int32_t Date::CUMULATIVE_LEAP_DAYS[] = {0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366};
const int8_t Date::MONTH_PER_DAY_OF_YEAR[] = {
    1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,
    1,  1,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,
    2,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,
    3,  3,  3,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,
    4,  4,  4,  4,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,
    5,  5,  5,  5,  5,  5,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,
    6,  6,  6,  6,  6,  6,  6,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,
    7,  7,  7,  7,  7,  7,  7,  7,  7,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,
    8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,
    9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10,
    10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11,
    11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12,
    12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12};
const int8_t Date::LEAP_MONTH_PER_DAY_OF_YEAR[] = {
    1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,
    1,  1,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,
    2,  2,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,
    3,  3,  3,  3,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,
    4,  4,  4,  4,  4,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,
    5,  5,  5,  5,  5,  5,  5,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,
    6,  6,  6,  6,  6,  6,  6,  6,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,
    7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,
    8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,
    9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10,
    10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11,
    11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12,
    12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12};
const int32_t Date::CUMULATIVE_YEAR_DAYS[] = {
    0,      365,    730,    1096,   1461,   1826,   2191,   2557,   2922,   3287,   3652,   4018,   4383,   4748,
    5113,   5479,   5844,   6209,   6574,   6940,   7305,   7670,   8035,   8401,   8766,   9131,   9496,   9862,
    10227,  10592,  10957,  11323,  11688,  12053,  12418,  12784,  13149,  13514,  13879,  14245,  14610,  14975,
    15340,  15706,  16071,  16436,  16801,  17167,  17532,  17897,  18262,  18628,  18993,  19358,  19723,  20089,
    20454,  20819,  21184,  21550,  21915,  22280,  22645,  23011,  23376,  23741,  24106,  24472,  24837,  25202,
    25567,  25933,  26298,  26663,  27028,  27394,  27759,  28124,  28489,  28855,  29220,  29585,  29950,  30316,
    30681,  31046,  31411,  31777,  32142,  32507,  32872,  33238,  33603,  33968,  34333,  34699,  35064,  35429,
    35794,  36160,  36525,  36890,  37255,  37621,  37986,  38351,  38716,  39082,  39447,  39812,  40177,  40543,
    40908,  41273,  41638,  42004,  42369,  42734,  43099,  43465,  43830,  44195,  44560,  44926,  45291,  45656,
    46021,  46387,  46752,  47117,  47482,  47847,  48212,  48577,  48942,  49308,  49673,  50038,  50403,  50769,
    51134,  51499,  51864,  52230,  52595,  52960,  53325,  53691,  54056,  54421,  54786,  55152,  55517,  55882,
    56247,  56613,  56978,  57343,  57708,  58074,  58439,  58804,  59169,  59535,  59900,  60265,  60630,  60996,
    61361,  61726,  62091,  62457,  62822,  63187,  63552,  63918,  64283,  64648,  65013,  65379,  65744,  66109,
    66474,  66840,  67205,  67570,  67935,  68301,  68666,  69031,  69396,  69762,  70127,  70492,  70857,  71223,
    71588,  71953,  72318,  72684,  73049,  73414,  73779,  74145,  74510,  74875,  75240,  75606,  75971,  76336,
    76701,  77067,  77432,  77797,  78162,  78528,  78893,  79258,  79623,  79989,  80354,  80719,  81084,  81450,
    81815,  82180,  82545,  82911,  83276,  83641,  84006,  84371,  84736,  85101,  85466,  85832,  86197,  86562,
    86927,  87293,  87658,  88023,  88388,  88754,  89119,  89484,  89849,  90215,  90580,  90945,  91310,  91676,
    92041,  92406,  92771,  93137,  93502,  93867,  94232,  94598,  94963,  95328,  95693,  96059,  96424,  96789,
    97154,  97520,  97885,  98250,  98615,  98981,  99346,  99711,  100076, 100442, 100807, 101172, 101537, 101903,
    102268, 102633, 102998, 103364, 103729, 104094, 104459, 104825, 105190, 105555, 105920, 106286, 106651, 107016,
    107381, 107747, 108112, 108477, 108842, 109208, 109573, 109938, 110303, 110669, 111034, 111399, 111764, 112130,
    112495, 112860, 113225, 113591, 113956, 114321, 114686, 115052, 115417, 115782, 116147, 116513, 116878, 117243,
    117608, 117974, 118339, 118704, 119069, 119435, 119800, 120165, 120530, 120895, 121260, 121625, 121990, 122356,
    122721, 123086, 123451, 123817, 124182, 124547, 124912, 125278, 125643, 126008, 126373, 126739, 127104, 127469,
    127834, 128200, 128565, 128930, 129295, 129661, 130026, 130391, 130756, 131122, 131487, 131852, 132217, 132583,
    132948, 133313, 133678, 134044, 134409, 134774, 135139, 135505, 135870, 136235, 136600, 136966, 137331, 137696,
    138061, 138427, 138792, 139157, 139522, 139888, 140253, 140618, 140983, 141349, 141714, 142079, 142444, 142810,
    143175, 143540, 143905, 144271, 144636, 145001, 145366, 145732, 146097};

void Date::ExtractYearOffset(int32_t &n, int32_t &year, int32_t &year_offset) {
	year = Date::EPOCH_YEAR;
	// first we normalize n to be in the year range [1970, 2370]
	// since leap years repeat every 400 years, we can safely normalize just by "shifting" the CumulativeYearDays array
	while (n < 0) {
		n += Date::DAYS_PER_YEAR_INTERVAL;
		year -= Date::YEAR_INTERVAL;
	}
	while (n >= Date::DAYS_PER_YEAR_INTERVAL) {
		n -= Date::DAYS_PER_YEAR_INTERVAL;
		year += Date::YEAR_INTERVAL;
	}
	// interpolation search
	// we can find an upper bound of the year by assuming each year has 365 days
	year_offset = n / 365;
	// because of leap years we might be off by a little bit: compensate by decrementing the year offset until we find
	// our year
	while (n < Date::CUMULATIVE_YEAR_DAYS[year_offset]) {
		year_offset--;
		D_ASSERT(year_offset >= 0);
	}
	year += year_offset;
	D_ASSERT(n >= Date::CUMULATIVE_YEAR_DAYS[year_offset]);
}

void Date::Convert(date_t d, int32_t &year, int32_t &month, int32_t &day) {
	auto n = d.days;
	int32_t year_offset;
	Date::ExtractYearOffset(n, year, year_offset);

	day = n - Date::CUMULATIVE_YEAR_DAYS[year_offset];
	D_ASSERT(day >= 0 && day <= 365);

	bool is_leap_year = (Date::CUMULATIVE_YEAR_DAYS[year_offset + 1] - Date::CUMULATIVE_YEAR_DAYS[year_offset]) == 366;
	if (is_leap_year) {
		month = Date::LEAP_MONTH_PER_DAY_OF_YEAR[day];
		day -= Date::CUMULATIVE_LEAP_DAYS[month - 1];
	} else {
		month = Date::MONTH_PER_DAY_OF_YEAR[day];
		day -= Date::CUMULATIVE_DAYS[month - 1];
	}
	day++;
	D_ASSERT(day > 0 && day <= (is_leap_year ? Date::LEAP_DAYS[month] : Date::NORMAL_DAYS[month]));
	D_ASSERT(month > 0 && month <= 12);
}

bool Date::TryFromDate(int32_t year, int32_t month, int32_t day, date_t &result) {
	int32_t n = 0;
	if (!Date::IsValid(year, month, day)) {
		return false;
	}
	n += Date::IsLeapYear(year) ? Date::CUMULATIVE_LEAP_DAYS[month - 1] : Date::CUMULATIVE_DAYS[month - 1];
	n += day - 1;
	if (year < 1970) {
		int32_t diff_from_base = 1970 - year;
		int32_t year_index = 400 - (diff_from_base % 400);
		int32_t fractions = diff_from_base / 400;
		n += Date::CUMULATIVE_YEAR_DAYS[year_index];
		n -= Date::DAYS_PER_YEAR_INTERVAL;
		n -= fractions * Date::DAYS_PER_YEAR_INTERVAL;
	} else if (year >= 2370) {
		int32_t diff_from_base = year - 2370;
		int32_t year_index = diff_from_base % 400;
		int32_t fractions = diff_from_base / 400;
		n += Date::CUMULATIVE_YEAR_DAYS[year_index];
		n += Date::DAYS_PER_YEAR_INTERVAL;
		n += fractions * Date::DAYS_PER_YEAR_INTERVAL;
	} else {
		n += Date::CUMULATIVE_YEAR_DAYS[year - 1970];
	}
#ifdef DEBUG
	int32_t y, m, d;
	Date::Convert(date_t(n), y, m, d);
	D_ASSERT(year == y);
	D_ASSERT(month == m);
	D_ASSERT(day == d);
#endif
	result = date_t(n);
	return true;
}

date_t Date::FromDate(int32_t year, int32_t month, int32_t day) {
	date_t result;
	if (!Date::TryFromDate(year, month, day, result)) {
		throw ConversionException("Date out of range: %d-%d-%d", year, month, day);
	}
	return result;
}

bool Date::ParseDoubleDigit(const char *buf, idx_t len, idx_t &pos, int32_t &result) {
	if (pos < len && StringUtil::CharacterIsDigit(buf[pos])) {
		result = buf[pos++] - '0';
		if (pos < len && StringUtil::CharacterIsDigit(buf[pos])) {
			result = (buf[pos++] - '0') + result * 10;
		}
		return true;
	}
	return false;
}

static bool TryConvertDateSpecial(const char *buf, idx_t len, idx_t &pos, const char *special) {
	auto p = pos;
	for (; p < len && *special; ++p) {
		const auto s = *special++;
		if (!s || StringUtil::CharacterToLower(buf[p]) != s) {
			return false;
		}
	}
	pos = p;
	return true;
}

bool Date::TryConvertDate(const char *buf, idx_t len, idx_t &pos, date_t &result, bool strict) {
	pos = 0;
	if (len == 0) {
		return false;
	}

	int32_t day = 0;
	int32_t month = -1;
	int32_t year = 0;
	bool yearneg = false;
	int sep;

	// skip leading spaces
	while (pos < len && StringUtil::CharacterIsSpace(buf[pos])) {
		pos++;
	}

	if (pos >= len) {
		return false;
	}
	if (buf[pos] == '-') {
		yearneg = true;
		pos++;
		if (pos >= len) {
			return false;
		}
	}
	if (!StringUtil::CharacterIsDigit(buf[pos])) {
		// Check for special values
		if (TryConvertDateSpecial(buf, len, pos, PINF)) {
			result = yearneg ? date_t::ninfinity() : date_t::infinity();
		} else if (TryConvertDateSpecial(buf, len, pos, EPOCH)) {
			result = date_t::epoch();
		} else {
			return false;
		}
		// skip trailing spaces - parsing must be strict here
		while (pos < len && StringUtil::CharacterIsSpace(buf[pos])) {
			pos++;
		}
		return pos == len;
	}
	// first parse the year
	for (; pos < len && StringUtil::CharacterIsDigit(buf[pos]); pos++) {
		if (year >= 100000000) {
			return false;
		}
		year = (buf[pos] - '0') + year * 10;
	}
	if (yearneg) {
		year = -year;
	}

	if (pos >= len) {
		return false;
	}

	// fetch the separator
	sep = buf[pos++];
	if (sep != ' ' && sep != '-' && sep != '/' && sep != '\\') {
		// invalid separator
		return false;
	}

	// parse the month
	if (!Date::ParseDoubleDigit(buf, len, pos, month)) {
		return false;
	}

	if (pos >= len) {
		return false;
	}

	if (buf[pos++] != sep) {
		return false;
	}

	if (pos >= len) {
		return false;
	}

	// now parse the day
	if (!Date::ParseDoubleDigit(buf, len, pos, day)) {
		return false;
	}

	// check for an optional trailing " (BC)""
	if (len - pos >= 5 && StringUtil::CharacterIsSpace(buf[pos]) && buf[pos + 1] == '(' &&
	    StringUtil::CharacterToLower(buf[pos + 2]) == 'b' && StringUtil::CharacterToLower(buf[pos + 3]) == 'c' &&
	    buf[pos + 4] == ')') {
		if (yearneg || year == 0) {
			return false;
		}
		year = -year + 1;
		pos += 5;
	}

	// in strict mode, check remaining string for non-space characters
	if (strict) {
		// skip trailing spaces
		while (pos < len && StringUtil::CharacterIsSpace((unsigned char)buf[pos])) {
			pos++;
		}
		// check position. if end was not reached, non-space chars remaining
		if (pos < len) {
			return false;
		}
	} else {
		// in non-strict mode, check for any direct trailing digits
		if (pos < len && StringUtil::CharacterIsDigit((unsigned char)buf[pos])) {
			return false;
		}
	}

	return Date::TryFromDate(year, month, day, result);
}

string Date::ConversionError(const string &str) {
	return StringUtil::Format("date field value out of range: \"%s\", "
	                          "expected format is (YYYY-MM-DD)",
	                          str);
}

string Date::ConversionError(string_t str) {
	return ConversionError(str.GetString());
}

date_t Date::FromCString(const char *buf, idx_t len, bool strict) {
	date_t result;
	idx_t pos;
	if (!TryConvertDate(buf, len, pos, result, strict)) {
		throw ConversionException(ConversionError(string(buf, len)));
	}
	return result;
}

date_t Date::FromString(const string &str, bool strict) {
	return Date::FromCString(str.c_str(), str.size(), strict);
}

string Date::ToString(date_t date) {
	// PG displays temporal infinities in lowercase,
	// but numerics in Titlecase.
	if (date == date_t::infinity()) {
		return PINF;
	} else if (date == date_t::ninfinity()) {
		return NINF;
	}
	int32_t date_units[3];
	idx_t year_length;
	bool add_bc;
	Date::Convert(date, date_units[0], date_units[1], date_units[2]);

	auto length = DateToStringCast::Length(date_units, year_length, add_bc);
	auto buffer = unique_ptr<char[]>(new char[length]);
	DateToStringCast::Format(buffer.get(), date_units, year_length, add_bc);
	return string(buffer.get(), length);
}

string Date::Format(int32_t year, int32_t month, int32_t day) {
	return ToString(Date::FromDate(year, month, day));
}

bool Date::IsLeapYear(int32_t year) {
	return year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
}

bool Date::IsValid(int32_t year, int32_t month, int32_t day) {
	if (month < 1 || month > 12) {
		return false;
	}
	if (day < 1) {
		return false;
	}
	if (year <= DATE_MIN_YEAR) {
		if (year < DATE_MIN_YEAR) {
			return false;
		} else if (year == DATE_MIN_YEAR) {
			if (month < DATE_MIN_MONTH || (month == DATE_MIN_MONTH && day < DATE_MIN_DAY)) {
				return false;
			}
		}
	}
	if (year >= DATE_MAX_YEAR) {
		if (year > DATE_MAX_YEAR) {
			return false;
		} else if (year == DATE_MAX_YEAR) {
			if (month > DATE_MAX_MONTH || (month == DATE_MAX_MONTH && day > DATE_MAX_DAY)) {
				return false;
			}
		}
	}
	return Date::IsLeapYear(year) ? day <= Date::LEAP_DAYS[month] : day <= Date::NORMAL_DAYS[month];
}

int32_t Date::MonthDays(int32_t year, int32_t month) {
	D_ASSERT(month >= 1 && month <= 12);
	return Date::IsLeapYear(year) ? Date::LEAP_DAYS[month] : Date::NORMAL_DAYS[month];
}

date_t Date::EpochDaysToDate(int32_t epoch) {
	return (date_t)epoch;
}

int32_t Date::EpochDays(date_t date) {
	return date.days;
}

date_t Date::EpochToDate(int64_t epoch) {
	return date_t(epoch / Interval::SECS_PER_DAY);
}

int64_t Date::Epoch(date_t date) {
	return ((int64_t)date.days) * Interval::SECS_PER_DAY;
}

int64_t Date::EpochNanoseconds(date_t date) {
	return ((int64_t)date.days) * (Interval::MICROS_PER_DAY * 1000);
}

int32_t Date::ExtractYear(date_t d, int32_t *last_year) {
	auto n = d.days;
	// cached look up: check if year of this date is the same as the last one we looked up
	// note that this only works for years in the range [1970, 2370]
	if (n >= Date::CUMULATIVE_YEAR_DAYS[*last_year] && n < Date::CUMULATIVE_YEAR_DAYS[*last_year + 1]) {
		return Date::EPOCH_YEAR + *last_year;
	}
	int32_t year;
	Date::ExtractYearOffset(n, year, *last_year);
	return year;
}

int32_t Date::ExtractYear(timestamp_t ts, int32_t *last_year) {
	return Date::ExtractYear(Timestamp::GetDate(ts), last_year);
}

int32_t Date::ExtractYear(date_t d) {
	int32_t year, year_offset;
	Date::ExtractYearOffset(d.days, year, year_offset);
	return year;
}

int32_t Date::ExtractMonth(date_t date) {
	int32_t out_year, out_month, out_day;
	Date::Convert(date, out_year, out_month, out_day);
	return out_month;
}

int32_t Date::ExtractDay(date_t date) {
	int32_t out_year, out_month, out_day;
	Date::Convert(date, out_year, out_month, out_day);
	return out_day;
}

int32_t Date::ExtractDayOfTheYear(date_t date) {
	int32_t year, year_offset;
	Date::ExtractYearOffset(date.days, year, year_offset);
	return date.days - Date::CUMULATIVE_YEAR_DAYS[year_offset] + 1;
}

int32_t Date::ExtractISODayOfTheWeek(date_t date) {
	// date of 0 is 1970-01-01, which was a Thursday (4)
	// -7 = 4
	// -6 = 5
	// -5 = 6
	// -4 = 7
	// -3 = 1
	// -2 = 2
	// -1 = 3
	// 0  = 4
	// 1  = 5
	// 2  = 6
	// 3  = 7
	// 4  = 1
	// 5  = 2
	// 6  = 3
	// 7  = 4
	if (date.days < 0) {
		// negative date: start off at 4 and cycle downwards
		return (7 - ((-int64_t(date.days) + 3) % 7));
	} else {
		// positive date: start off at 4 and cycle upwards
		return ((int64_t(date.days) + 3) % 7) + 1;
	}
}

static int32_t GetISOYearWeek(int32_t &year, int32_t month, int32_t day) {
	auto day_of_the_year =
	    (Date::IsLeapYear(year) ? Date::CUMULATIVE_LEAP_DAYS[month] : Date::CUMULATIVE_DAYS[month]) + day;
	// get the first day of the first week of the year
	// the first week is the week that has the 4th of January in it
	const auto weekday_of_the_fourth = Date::ExtractISODayOfTheWeek(Date::FromDate(year, 1, 4));
	// if fourth is monday, then fourth is the first day
	// if fourth is tuesday, third is the first day
	// if fourth is wednesday, second is the first day
	// if fourth is thursday, first is the first day
	// if fourth is friday - sunday, day is in the previous year
	// (day is 0-based, weekday is 1-based)
	const auto first_day_of_the_first_isoweek = 4 - weekday_of_the_fourth;
	if (day_of_the_year < first_day_of_the_first_isoweek) {
		// day is part of last year (13th month)
		--year;
		return GetISOYearWeek(year, 12, day);
	} else {
		return ((day_of_the_year - first_day_of_the_first_isoweek) / 7) + 1;
	}
}

void Date::ExtractISOYearWeek(date_t date, int32_t &year, int32_t &week) {
	int32_t month, day;
	Date::Convert(date, year, month, day);
	week = GetISOYearWeek(year, month - 1, day - 1);
}

int32_t Date::ExtractISOWeekNumber(date_t date) {
	int32_t year, week;
	ExtractISOYearWeek(date, year, week);
	return week;
}

int32_t Date::ExtractISOYearNumber(date_t date) {
	int32_t year, week;
	ExtractISOYearWeek(date, year, week);
	return year;
}

int32_t Date::ExtractWeekNumberRegular(date_t date, bool monday_first) {
	int32_t year, month, day;
	Date::Convert(date, year, month, day);
	month -= 1;
	day -= 1;
	// get the day of the year
	auto day_of_the_year =
	    (Date::IsLeapYear(year) ? Date::CUMULATIVE_LEAP_DAYS[month] : Date::CUMULATIVE_DAYS[month]) + day;
	// now figure out the first monday or sunday of the year
	// what day is January 1st?
	auto day_of_jan_first = Date::ExtractISODayOfTheWeek(Date::FromDate(year, 1, 1));
	// monday = 1, sunday = 7
	int32_t first_week_start;
	if (monday_first) {
		// have to find next "1"
		if (day_of_jan_first == 1) {
			// jan 1 is monday: starts immediately
			first_week_start = 0;
		} else {
			// jan 1 is not monday: count days until next monday
			first_week_start = 8 - day_of_jan_first;
		}
	} else {
		first_week_start = 7 - day_of_jan_first;
	}
	if (day_of_the_year < first_week_start) {
		// day occurs before first week starts: week 0
		return 0;
	}
	return ((day_of_the_year - first_week_start) / 7) + 1;
}

// Returns the date of the monday of the current week.
date_t Date::GetMondayOfCurrentWeek(date_t date) {
	int32_t dotw = Date::ExtractISODayOfTheWeek(date);
	return date - (dotw - 1);
}

} // namespace duckdb



namespace duckdb {

template <class SIGNED, class UNSIGNED>
string TemplatedDecimalToString(SIGNED value, uint8_t scale) {
	auto len = DecimalToString::DecimalLength<SIGNED, UNSIGNED>(value, scale);
	auto data = unique_ptr<char[]>(new char[len + 1]);
	DecimalToString::FormatDecimal<SIGNED, UNSIGNED>(value, scale, data.get(), len);
	return string(data.get(), len);
}

string Decimal::ToString(int16_t value, uint8_t scale) {
	return TemplatedDecimalToString<int16_t, uint16_t>(value, scale);
}

string Decimal::ToString(int32_t value, uint8_t scale) {
	return TemplatedDecimalToString<int32_t, uint32_t>(value, scale);
}

string Decimal::ToString(int64_t value, uint8_t scale) {
	return TemplatedDecimalToString<int64_t, uint64_t>(value, scale);
}

string Decimal::ToString(hugeint_t value, uint8_t scale) {
	auto len = HugeintToStringCast::DecimalLength(value, scale);
	auto data = unique_ptr<char[]>(new char[len + 1]);
	HugeintToStringCast::FormatDecimal(value, scale, data.get(), len);
	return string(data.get(), len);
}

} // namespace duckdb





#include <functional>

namespace duckdb {

template <>
hash_t Hash(uint64_t val) {
	return murmurhash64(val);
}

template <>
hash_t Hash(int64_t val) {
	return murmurhash64((uint64_t)val);
}

template <>
hash_t Hash(hugeint_t val) {
	return murmurhash64(val.lower) ^ murmurhash64(val.upper);
}

template <>
hash_t Hash(float val) {
	return std::hash<float> {}(val);
}

template <>
hash_t Hash(double val) {
	return std::hash<double> {}(val);
}

template <>
hash_t Hash(interval_t val) {
	return Hash(val.days) ^ Hash(val.months) ^ Hash(val.micros);
}

template <>
hash_t Hash(const char *str) {
	return Hash(str, strlen(str));
}

template <>
hash_t Hash(string_t val) {
	return Hash(val.GetDataUnsafe(), val.GetSize());
}

template <>
hash_t Hash(char *val) {
	return Hash<const char *>(val);
}

// MIT License
// Copyright (c) 2018-2021 Martin Ankerl
// https://github.com/martinus/robin-hood-hashing/blob/3.11.5/LICENSE
hash_t HashBytes(void *ptr, size_t len) noexcept {
	static constexpr uint64_t M = UINT64_C(0xc6a4a7935bd1e995);
	static constexpr uint64_t SEED = UINT64_C(0xe17a1465);
	static constexpr unsigned int R = 47;

	auto const *const data64 = static_cast<uint64_t const *>(ptr);
	uint64_t h = SEED ^ (len * M);

	size_t const n_blocks = len / 8;
	for (size_t i = 0; i < n_blocks; ++i) {
		auto k = Load<uint64_t>(reinterpret_cast<const_data_ptr_t>(data64 + i));

		k *= M;
		k ^= k >> R;
		k *= M;

		h ^= k;
		h *= M;
	}

	auto const *const data8 = reinterpret_cast<uint8_t const *>(data64 + n_blocks);
	switch (len & 7U) {
	case 7:
		h ^= static_cast<uint64_t>(data8[6]) << 48U;
		DUCKDB_EXPLICIT_FALLTHROUGH;
	case 6:
		h ^= static_cast<uint64_t>(data8[5]) << 40U;
		DUCKDB_EXPLICIT_FALLTHROUGH;
	case 5:
		h ^= static_cast<uint64_t>(data8[4]) << 32U;
		DUCKDB_EXPLICIT_FALLTHROUGH;
	case 4:
		h ^= static_cast<uint64_t>(data8[3]) << 24U;
		DUCKDB_EXPLICIT_FALLTHROUGH;
	case 3:
		h ^= static_cast<uint64_t>(data8[2]) << 16U;
		DUCKDB_EXPLICIT_FALLTHROUGH;
	case 2:
		h ^= static_cast<uint64_t>(data8[1]) << 8U;
		DUCKDB_EXPLICIT_FALLTHROUGH;
	case 1:
		h ^= static_cast<uint64_t>(data8[0]);
		h *= M;
		DUCKDB_EXPLICIT_FALLTHROUGH;
	default:
		break;
	}
	h ^= h >> R;
	h *= M;
	h ^= h >> R;
	return static_cast<hash_t>(h);
}

hash_t Hash(const char *val, size_t size) {
	return HashBytes((void *)val, size);
}

hash_t Hash(uint8_t *val, size_t size) {
	return HashBytes((void *)val, size);
}

} // namespace duckdb







#include <cmath>
#include <limits>

namespace duckdb {

//===--------------------------------------------------------------------===//
// String Conversion
//===--------------------------------------------------------------------===//
const hugeint_t Hugeint::POWERS_OF_TEN[] {
    hugeint_t(1),
    hugeint_t(10),
    hugeint_t(100),
    hugeint_t(1000),
    hugeint_t(10000),
    hugeint_t(100000),
    hugeint_t(1000000),
    hugeint_t(10000000),
    hugeint_t(100000000),
    hugeint_t(1000000000),
    hugeint_t(10000000000),
    hugeint_t(100000000000),
    hugeint_t(1000000000000),
    hugeint_t(10000000000000),
    hugeint_t(100000000000000),
    hugeint_t(1000000000000000),
    hugeint_t(10000000000000000),
    hugeint_t(100000000000000000),
    hugeint_t(1000000000000000000),
    hugeint_t(1000000000000000000) * hugeint_t(10),
    hugeint_t(1000000000000000000) * hugeint_t(100),
    hugeint_t(1000000000000000000) * hugeint_t(1000),
    hugeint_t(1000000000000000000) * hugeint_t(10000),
    hugeint_t(1000000000000000000) * hugeint_t(100000),
    hugeint_t(1000000000000000000) * hugeint_t(1000000),
    hugeint_t(1000000000000000000) * hugeint_t(10000000),
    hugeint_t(1000000000000000000) * hugeint_t(100000000),
    hugeint_t(1000000000000000000) * hugeint_t(1000000000),
    hugeint_t(1000000000000000000) * hugeint_t(10000000000),
    hugeint_t(1000000000000000000) * hugeint_t(100000000000),
    hugeint_t(1000000000000000000) * hugeint_t(1000000000000),
    hugeint_t(1000000000000000000) * hugeint_t(10000000000000),
    hugeint_t(1000000000000000000) * hugeint_t(100000000000000),
    hugeint_t(1000000000000000000) * hugeint_t(1000000000000000),
    hugeint_t(1000000000000000000) * hugeint_t(10000000000000000),
    hugeint_t(1000000000000000000) * hugeint_t(100000000000000000),
    hugeint_t(1000000000000000000) * hugeint_t(1000000000000000000),
    hugeint_t(1000000000000000000) * hugeint_t(1000000000000000000) * hugeint_t(10),
    hugeint_t(1000000000000000000) * hugeint_t(1000000000000000000) * hugeint_t(100)};

static uint8_t PositiveHugeintHighestBit(hugeint_t bits) {
	uint8_t out = 0;
	if (bits.upper) {
		out = 64;
		uint64_t up = bits.upper;
		while (up) {
			up >>= 1;
			out++;
		}
	} else {
		uint64_t low = bits.lower;
		while (low) {
			low >>= 1;
			out++;
		}
	}
	return out;
}

static bool PositiveHugeintIsBitSet(hugeint_t lhs, uint8_t bit_position) {
	if (bit_position < 64) {
		return lhs.lower & (uint64_t(1) << uint64_t(bit_position));
	} else {
		return lhs.upper & (uint64_t(1) << uint64_t(bit_position - 64));
	}
}

hugeint_t PositiveHugeintLeftShift(hugeint_t lhs, uint32_t amount) {
	D_ASSERT(amount > 0 && amount < 64);
	hugeint_t result;
	result.lower = lhs.lower << amount;
	result.upper = (lhs.upper << amount) + (lhs.lower >> (64 - amount));
	return result;
}

hugeint_t Hugeint::DivModPositive(hugeint_t lhs, uint64_t rhs, uint64_t &remainder) {
	D_ASSERT(lhs.upper >= 0);
	// DivMod code adapted from:
	// https://github.com/calccrypto/uint128_t/blob/master/uint128_t.cpp

	// initialize the result and remainder to 0
	hugeint_t div_result;
	div_result.lower = 0;
	div_result.upper = 0;
	remainder = 0;

	uint8_t highest_bit_set = PositiveHugeintHighestBit(lhs);
	// now iterate over the amount of bits that are set in the LHS
	for (uint8_t x = highest_bit_set; x > 0; x--) {
		// left-shift the current result and remainder by 1
		div_result = PositiveHugeintLeftShift(div_result, 1);
		remainder <<= 1;
		// we get the value of the bit at position X, where position 0 is the least-significant bit
		if (PositiveHugeintIsBitSet(lhs, x - 1)) {
			// increment the remainder
			remainder++;
		}
		if (remainder >= rhs) {
			// the remainder has passed the division multiplier: add one to the divide result
			remainder -= rhs;
			div_result.lower++;
			if (div_result.lower == 0) {
				// overflow
				div_result.upper++;
			}
		}
	}
	return div_result;
}

string Hugeint::ToString(hugeint_t input) {
	uint64_t remainder;
	string result;
	bool negative = input.upper < 0;
	if (negative) {
		NegateInPlace(input);
	}
	while (true) {
		if (!input.lower && !input.upper) {
			break;
		}
		input = Hugeint::DivModPositive(input, 10, remainder);
		result = string(1, '0' + remainder) + result; // NOLINT
	}
	if (result.empty()) {
		// value is zero
		return "0";
	}
	return negative ? "-" + result : result;
}

//===--------------------------------------------------------------------===//
// Multiply
//===--------------------------------------------------------------------===//
bool Hugeint::TryMultiply(hugeint_t lhs, hugeint_t rhs, hugeint_t &result) {
	bool lhs_negative = lhs.upper < 0;
	bool rhs_negative = rhs.upper < 0;
	if (lhs_negative) {
		NegateInPlace(lhs);
	}
	if (rhs_negative) {
		NegateInPlace(rhs);
	}
#if ((__GNUC__ >= 5) || defined(__clang__)) && defined(__SIZEOF_INT128__)
	__uint128_t left = __uint128_t(lhs.lower) + (__uint128_t(lhs.upper) << 64);
	__uint128_t right = __uint128_t(rhs.lower) + (__uint128_t(rhs.upper) << 64);
	__uint128_t result_i128;
	if (__builtin_mul_overflow(left, right, &result_i128)) {
		return false;
	}
	uint64_t upper = uint64_t(result_i128 >> 64);
	if (upper & 0x8000000000000000) {
		return false;
	}
	result.upper = int64_t(upper);
	result.lower = uint64_t(result_i128 & 0xffffffffffffffff);
#else
	// Multiply code adapted from:
	// https://github.com/calccrypto/uint128_t/blob/master/uint128_t.cpp

	// split values into 4 32-bit parts
	uint64_t top[4] = {uint64_t(lhs.upper) >> 32, uint64_t(lhs.upper) & 0xffffffff, lhs.lower >> 32,
	                   lhs.lower & 0xffffffff};
	uint64_t bottom[4] = {uint64_t(rhs.upper) >> 32, uint64_t(rhs.upper) & 0xffffffff, rhs.lower >> 32,
	                      rhs.lower & 0xffffffff};
	uint64_t products[4][4];

	// multiply each component of the values
	for (auto x = 0; x < 4; x++) {
		for (auto y = 0; y < 4; y++) {
			products[x][y] = top[x] * bottom[y];
		}
	}

	// if any of these products are set to a non-zero value, there is always an overflow
	if (products[0][0] || products[0][1] || products[0][2] || products[1][0] || products[2][0] || products[1][1]) {
		return false;
	}
	// if the high bits of any of these are set, there is always an overflow
	if ((products[0][3] & 0xffffffff80000000) || (products[1][2] & 0xffffffff80000000) ||
	    (products[2][1] & 0xffffffff80000000) || (products[3][0] & 0xffffffff80000000)) {
		return false;
	}

	// otherwise we merge the result of the different products together in-order

	// first row
	uint64_t fourth32 = (products[3][3] & 0xffffffff);
	uint64_t third32 = (products[3][2] & 0xffffffff) + (products[3][3] >> 32);
	uint64_t second32 = (products[3][1] & 0xffffffff) + (products[3][2] >> 32);
	uint64_t first32 = (products[3][0] & 0xffffffff) + (products[3][1] >> 32);

	// second row
	third32 += (products[2][3] & 0xffffffff);
	second32 += (products[2][2] & 0xffffffff) + (products[2][3] >> 32);
	first32 += (products[2][1] & 0xffffffff) + (products[2][2] >> 32);

	// third row
	second32 += (products[1][3] & 0xffffffff);
	first32 += (products[1][2] & 0xffffffff) + (products[1][3] >> 32);

	// fourth row
	first32 += (products[0][3] & 0xffffffff);

	// move carry to next digit
	third32 += fourth32 >> 32;
	second32 += third32 >> 32;
	first32 += second32 >> 32;

	// check if the combination of the different products resulted in an overflow
	if (first32 & 0xffffff80000000) {
		return false;
	}

	// remove carry from current digit
	fourth32 &= 0xffffffff;
	third32 &= 0xffffffff;
	second32 &= 0xffffffff;
	first32 &= 0xffffffff;

	// combine components
	result.lower = (third32 << 32) | fourth32;
	result.upper = (first32 << 32) | second32;
#endif
	if (lhs_negative ^ rhs_negative) {
		NegateInPlace(result);
	}
	return true;
}

hugeint_t Hugeint::Multiply(hugeint_t lhs, hugeint_t rhs) {
	hugeint_t result;
	if (!TryMultiply(lhs, rhs, result)) {
		throw OutOfRangeException("Overflow in HUGEINT multiplication!");
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Divide
//===--------------------------------------------------------------------===//
hugeint_t Hugeint::DivMod(hugeint_t lhs, hugeint_t rhs, hugeint_t &remainder) {
	// division by zero not allowed
	D_ASSERT(!(rhs.upper == 0 && rhs.lower == 0));

	bool lhs_negative = lhs.upper < 0;
	bool rhs_negative = rhs.upper < 0;
	if (lhs_negative) {
		Hugeint::NegateInPlace(lhs);
	}
	if (rhs_negative) {
		Hugeint::NegateInPlace(rhs);
	}
	// DivMod code adapted from:
	// https://github.com/calccrypto/uint128_t/blob/master/uint128_t.cpp

	// initialize the result and remainder to 0
	hugeint_t div_result;
	div_result.lower = 0;
	div_result.upper = 0;
	remainder.lower = 0;
	remainder.upper = 0;

	uint8_t highest_bit_set = PositiveHugeintHighestBit(lhs);
	// now iterate over the amount of bits that are set in the LHS
	for (uint8_t x = highest_bit_set; x > 0; x--) {
		// left-shift the current result and remainder by 1
		div_result = PositiveHugeintLeftShift(div_result, 1);
		remainder = PositiveHugeintLeftShift(remainder, 1);

		// we get the value of the bit at position X, where position 0 is the least-significant bit
		if (PositiveHugeintIsBitSet(lhs, x - 1)) {
			// increment the remainder
			Hugeint::AddInPlace(remainder, 1);
		}
		if (Hugeint::GreaterThanEquals(remainder, rhs)) {
			// the remainder has passed the division multiplier: add one to the divide result
			remainder = Hugeint::Subtract(remainder, rhs);
			Hugeint::AddInPlace(div_result, 1);
		}
	}
	if (lhs_negative ^ rhs_negative) {
		Hugeint::NegateInPlace(div_result);
	}
	if (lhs_negative) {
		Hugeint::NegateInPlace(remainder);
	}
	return div_result;
}

hugeint_t Hugeint::Divide(hugeint_t lhs, hugeint_t rhs) {
	hugeint_t remainder;
	return Hugeint::DivMod(lhs, rhs, remainder);
}

hugeint_t Hugeint::Modulo(hugeint_t lhs, hugeint_t rhs) {
	hugeint_t remainder;
	Hugeint::DivMod(lhs, rhs, remainder);
	return remainder;
}

//===--------------------------------------------------------------------===//
// Add/Subtract
//===--------------------------------------------------------------------===//
bool Hugeint::AddInPlace(hugeint_t &lhs, hugeint_t rhs) {
	int overflow = lhs.lower + rhs.lower < lhs.lower;
	if (rhs.upper >= 0) {
		// RHS is positive: check for overflow
		if (lhs.upper > (std::numeric_limits<int64_t>::max() - rhs.upper - overflow)) {
			return false;
		}
		lhs.upper = lhs.upper + overflow + rhs.upper;
	} else {
		// RHS is negative: check for underflow
		if (lhs.upper < std::numeric_limits<int64_t>::min() - rhs.upper - overflow) {
			return false;
		}
		lhs.upper = lhs.upper + (overflow + rhs.upper);
	}
	lhs.lower += rhs.lower;
	if (lhs.upper == std::numeric_limits<int64_t>::min() && lhs.lower == 0) {
		return false;
	}
	return true;
}

bool Hugeint::SubtractInPlace(hugeint_t &lhs, hugeint_t rhs) {
	// underflow
	int underflow = lhs.lower - rhs.lower > lhs.lower;
	if (rhs.upper >= 0) {
		// RHS is positive: check for underflow
		if (lhs.upper < (std::numeric_limits<int64_t>::min() + rhs.upper + underflow)) {
			return false;
		}
		lhs.upper = (lhs.upper - rhs.upper) - underflow;
	} else {
		// RHS is negative: check for overflow
		if (lhs.upper > std::numeric_limits<int64_t>::min() &&
		    lhs.upper - 1 >= (std::numeric_limits<int64_t>::max() + rhs.upper + underflow)) {
			return false;
		}
		lhs.upper = lhs.upper - (rhs.upper + underflow);
	}
	lhs.lower -= rhs.lower;
	if (lhs.upper == std::numeric_limits<int64_t>::min() && lhs.lower == 0) {
		return false;
	}
	return true;
}

hugeint_t Hugeint::Add(hugeint_t lhs, hugeint_t rhs) {
	if (!AddInPlace(lhs, rhs)) {
		throw OutOfRangeException("Overflow in HUGEINT addition");
	}
	return lhs;
}

hugeint_t Hugeint::Subtract(hugeint_t lhs, hugeint_t rhs) {
	if (!SubtractInPlace(lhs, rhs)) {
		throw OutOfRangeException("Underflow in HUGEINT addition");
	}
	return lhs;
}

//===--------------------------------------------------------------------===//
// Hugeint Cast/Conversion
//===--------------------------------------------------------------------===//
template <class DST, bool SIGNED = true>
bool HugeintTryCastInteger(hugeint_t input, DST &result) {
	switch (input.upper) {
	case 0:
		// positive number: check if the positive number is in range
		if (input.lower <= uint64_t(NumericLimits<DST>::Maximum())) {
			result = DST(input.lower);
			return true;
		}
		break;
	case -1:
		if (!SIGNED) {
			return false;
		}
		// negative number: check if the negative number is in range
		if (input.lower >= NumericLimits<uint64_t>::Maximum() - uint64_t(NumericLimits<DST>::Maximum())) {
			result = -DST(NumericLimits<uint64_t>::Maximum() - input.lower) - 1;
			return true;
		}
		break;
	default:
		break;
	}
	return false;
}

template <>
bool Hugeint::TryCast(hugeint_t input, int8_t &result) {
	return HugeintTryCastInteger<int8_t>(input, result);
}

template <>
bool Hugeint::TryCast(hugeint_t input, int16_t &result) {
	return HugeintTryCastInteger<int16_t>(input, result);
}

template <>
bool Hugeint::TryCast(hugeint_t input, int32_t &result) {
	return HugeintTryCastInteger<int32_t>(input, result);
}

template <>
bool Hugeint::TryCast(hugeint_t input, int64_t &result) {
	return HugeintTryCastInteger<int64_t>(input, result);
}

template <>
bool Hugeint::TryCast(hugeint_t input, uint8_t &result) {
	return HugeintTryCastInteger<uint8_t, false>(input, result);
}

template <>
bool Hugeint::TryCast(hugeint_t input, uint16_t &result) {
	return HugeintTryCastInteger<uint16_t, false>(input, result);
}

template <>
bool Hugeint::TryCast(hugeint_t input, uint32_t &result) {
	return HugeintTryCastInteger<uint32_t, false>(input, result);
}

template <>
bool Hugeint::TryCast(hugeint_t input, uint64_t &result) {
	return HugeintTryCastInteger<uint64_t, false>(input, result);
}

template <>
bool Hugeint::TryCast(hugeint_t input, hugeint_t &result) {
	result = input;
	return true;
}

template <>
bool Hugeint::TryCast(hugeint_t input, float &result) {
	double dbl_result;
	Hugeint::TryCast(input, dbl_result);
	result = (float)dbl_result;
	return true;
}

template <class REAL_T>
bool CastBigintToFloating(hugeint_t input, REAL_T &result) {
	switch (input.upper) {
	case -1:
		// special case for upper = -1 to avoid rounding issues in small negative numbers
		result = -REAL_T(NumericLimits<uint64_t>::Maximum() - input.lower) - 1;
		break;
	default:
		result = REAL_T(input.lower) + REAL_T(input.upper) * REAL_T(NumericLimits<uint64_t>::Maximum());
		break;
	}
	return true;
}

template <>
bool Hugeint::TryCast(hugeint_t input, double &result) {
	return CastBigintToFloating<double>(input, result);
}

template <>
bool Hugeint::TryCast(hugeint_t input, long double &result) {
	return CastBigintToFloating<long double>(input, result);
}

template <class DST>
hugeint_t HugeintConvertInteger(DST input) {
	hugeint_t result;
	result.lower = (uint64_t)input;
	result.upper = (input < 0) * -1;
	return result;
}

template <>
bool Hugeint::TryConvert(int8_t value, hugeint_t &result) {
	result = HugeintConvertInteger<int8_t>(value);
	return true;
}

template <>
bool Hugeint::TryConvert(int16_t value, hugeint_t &result) {
	result = HugeintConvertInteger<int16_t>(value);
	return true;
}

template <>
bool Hugeint::TryConvert(int32_t value, hugeint_t &result) {
	result = HugeintConvertInteger<int32_t>(value);
	return true;
}

template <>
bool Hugeint::TryConvert(int64_t value, hugeint_t &result) {
	result = HugeintConvertInteger<int64_t>(value);
	return true;
}
template <>
bool Hugeint::TryConvert(uint8_t value, hugeint_t &result) {
	result = HugeintConvertInteger<uint8_t>(value);
	return true;
}
template <>
bool Hugeint::TryConvert(uint16_t value, hugeint_t &result) {
	result = HugeintConvertInteger<uint16_t>(value);
	return true;
}
template <>
bool Hugeint::TryConvert(uint32_t value, hugeint_t &result) {
	result = HugeintConvertInteger<uint32_t>(value);
	return true;
}
template <>
bool Hugeint::TryConvert(uint64_t value, hugeint_t &result) {
	result = HugeintConvertInteger<uint64_t>(value);
	return true;
}

template <>
bool Hugeint::TryConvert(float value, hugeint_t &result) {
	return Hugeint::TryConvert(double(value), result);
}

template <class REAL_T>
bool ConvertFloatingToBigint(REAL_T value, hugeint_t &result) {
	if (!Value::IsFinite<REAL_T>(value)) {
		return false;
	}
	if (value <= -170141183460469231731687303715884105728.0 || value >= 170141183460469231731687303715884105727.0) {
		return false;
	}
	bool negative = value < 0;
	if (negative) {
		value = -value;
	}
	result.lower = (uint64_t)fmod(value, REAL_T(NumericLimits<uint64_t>::Maximum()));
	result.upper = (uint64_t)(value / REAL_T(NumericLimits<uint64_t>::Maximum()));
	if (negative) {
		Hugeint::NegateInPlace(result);
	}
	return true;
}

template <>
bool Hugeint::TryConvert(double value, hugeint_t &result) {
	return ConvertFloatingToBigint<double>(value, result);
}

template <>
bool Hugeint::TryConvert(long double value, hugeint_t &result) {
	return ConvertFloatingToBigint<long double>(value, result);
}

//===--------------------------------------------------------------------===//
// hugeint_t operators
//===--------------------------------------------------------------------===//
hugeint_t::hugeint_t(int64_t value) {
	auto result = Hugeint::Convert(value);
	this->lower = result.lower;
	this->upper = result.upper;
}

bool hugeint_t::operator==(const hugeint_t &rhs) const {
	return Hugeint::Equals(*this, rhs);
}

bool hugeint_t::operator!=(const hugeint_t &rhs) const {
	return Hugeint::NotEquals(*this, rhs);
}

bool hugeint_t::operator<(const hugeint_t &rhs) const {
	return Hugeint::LessThan(*this, rhs);
}

bool hugeint_t::operator<=(const hugeint_t &rhs) const {
	return Hugeint::LessThanEquals(*this, rhs);
}

bool hugeint_t::operator>(const hugeint_t &rhs) const {
	return Hugeint::GreaterThan(*this, rhs);
}

bool hugeint_t::operator>=(const hugeint_t &rhs) const {
	return Hugeint::GreaterThanEquals(*this, rhs);
}

hugeint_t hugeint_t::operator+(const hugeint_t &rhs) const {
	return Hugeint::Add(*this, rhs);
}

hugeint_t hugeint_t::operator-(const hugeint_t &rhs) const {
	return Hugeint::Subtract(*this, rhs);
}

hugeint_t hugeint_t::operator*(const hugeint_t &rhs) const {
	return Hugeint::Multiply(*this, rhs);
}

hugeint_t hugeint_t::operator/(const hugeint_t &rhs) const {
	return Hugeint::Divide(*this, rhs);
}

hugeint_t hugeint_t::operator%(const hugeint_t &rhs) const {
	return Hugeint::Modulo(*this, rhs);
}

hugeint_t hugeint_t::operator-() const {
	return Hugeint::Negate(*this);
}

hugeint_t hugeint_t::operator>>(const hugeint_t &rhs) const {
	hugeint_t result;
	uint64_t shift = rhs.lower;
	if (rhs.upper != 0 || shift >= 128) {
		return hugeint_t(0);
	} else if (shift == 0) {
		return *this;
	} else if (shift == 64) {
		result.upper = (upper < 0) ? -1 : 0;
		result.lower = upper;
	} else if (shift < 64) {
		// perform lower shift in unsigned integer, and mask away the most significant bit
		result.lower = (uint64_t(upper) << (64 - shift)) | (lower >> shift);
		result.upper = upper >> shift;
	} else {
		D_ASSERT(shift < 128);
		result.lower = upper >> (shift - 64);
		result.upper = (upper < 0) ? -1 : 0;
	}
	return result;
}

hugeint_t hugeint_t::operator<<(const hugeint_t &rhs) const {
	if (upper < 0) {
		return hugeint_t(0);
	}
	hugeint_t result;
	uint64_t shift = rhs.lower;
	if (rhs.upper != 0 || shift >= 128) {
		return hugeint_t(0);
	} else if (shift == 64) {
		result.upper = lower;
		result.lower = 0;
	} else if (shift == 0) {
		return *this;
	} else if (shift < 64) {
		// perform upper shift in unsigned integer, and mask away the most significant bit
		uint64_t upper_shift = ((uint64_t(upper) << shift) + (lower >> (64 - shift))) & 0x7FFFFFFFFFFFFFFF;
		result.lower = lower << shift;
		result.upper = upper_shift;
	} else {
		D_ASSERT(shift < 128);
		result.lower = 0;
		result.upper = (lower << (shift - 64)) & 0x7FFFFFFFFFFFFFFF;
	}
	return result;
}

hugeint_t hugeint_t::operator&(const hugeint_t &rhs) const {
	hugeint_t result;
	result.lower = lower & rhs.lower;
	result.upper = upper & rhs.upper;
	return result;
}

hugeint_t hugeint_t::operator|(const hugeint_t &rhs) const {
	hugeint_t result;
	result.lower = lower | rhs.lower;
	result.upper = upper | rhs.upper;
	return result;
}

hugeint_t hugeint_t::operator^(const hugeint_t &rhs) const {
	hugeint_t result;
	result.lower = lower ^ rhs.lower;
	result.upper = upper ^ rhs.upper;
	return result;
}

hugeint_t hugeint_t::operator~() const {
	hugeint_t result;
	result.lower = ~lower;
	result.upper = ~upper;
	return result;
}

hugeint_t &hugeint_t::operator+=(const hugeint_t &rhs) {
	Hugeint::AddInPlace(*this, rhs);
	return *this;
}
hugeint_t &hugeint_t::operator-=(const hugeint_t &rhs) {
	Hugeint::SubtractInPlace(*this, rhs);
	return *this;
}
hugeint_t &hugeint_t::operator*=(const hugeint_t &rhs) {
	*this = Hugeint::Multiply(*this, rhs);
	return *this;
}
hugeint_t &hugeint_t::operator/=(const hugeint_t &rhs) {
	*this = Hugeint::Divide(*this, rhs);
	return *this;
}
hugeint_t &hugeint_t::operator%=(const hugeint_t &rhs) {
	*this = Hugeint::Modulo(*this, rhs);
	return *this;
}
hugeint_t &hugeint_t::operator>>=(const hugeint_t &rhs) {
	*this = *this >> rhs;
	return *this;
}
hugeint_t &hugeint_t::operator<<=(const hugeint_t &rhs) {
	*this = *this << rhs;
	return *this;
}
hugeint_t &hugeint_t::operator&=(const hugeint_t &rhs) {
	lower &= rhs.lower;
	upper &= rhs.upper;
	return *this;
}
hugeint_t &hugeint_t::operator|=(const hugeint_t &rhs) {
	lower |= rhs.lower;
	upper |= rhs.upper;
	return *this;
}
hugeint_t &hugeint_t::operator^=(const hugeint_t &rhs) {
	lower ^= rhs.lower;
	upper ^= rhs.upper;
	return *this;
}

string hugeint_t::ToString() const {
	return Hugeint::ToString(*this);
}

} // namespace duckdb






namespace duckdb {

HyperLogLog::HyperLogLog() : hll(nullptr) {
	hll = duckdb_hll::hll_create();
	// Insert into a dense hll can be vectorized, sparse cannot, so we immediately convert
	duckdb_hll::hllSparseToDense((duckdb_hll::robj *)hll);
}

HyperLogLog::HyperLogLog(void *hll) : hll(hll) {
}

HyperLogLog::~HyperLogLog() {
	duckdb_hll::hll_destroy((duckdb_hll::robj *)hll);
}

void HyperLogLog::Add(data_ptr_t element, idx_t size) {
	if (duckdb_hll::hll_add((duckdb_hll::robj *)hll, element, size) == HLL_C_ERR) {
		throw InternalException("Could not add to HLL?");
	}
}

idx_t HyperLogLog::Count() const {
	// exception from size_t ban
	size_t result;

	if (duckdb_hll::hll_count((duckdb_hll::robj *)hll, &result) != HLL_C_OK) {
		throw InternalException("Could not count HLL?");
	}
	return result;
}

unique_ptr<HyperLogLog> HyperLogLog::Merge(HyperLogLog &other) {
	duckdb_hll::robj *hlls[2];
	hlls[0] = (duckdb_hll::robj *)hll;
	hlls[1] = (duckdb_hll::robj *)other.hll;
	auto new_hll = duckdb_hll::hll_merge(hlls, 2);
	if (!new_hll) {
		throw InternalException("Could not merge HLLs");
	}
	return unique_ptr<HyperLogLog>(new HyperLogLog((void *)new_hll));
}

HyperLogLog *HyperLogLog::MergePointer(HyperLogLog &other) {
	duckdb_hll::robj *hlls[2];
	hlls[0] = (duckdb_hll::robj *)hll;
	hlls[1] = (duckdb_hll::robj *)other.hll;
	auto new_hll = duckdb_hll::hll_merge(hlls, 2);
	if (!new_hll) {
		throw Exception("Could not merge HLLs");
	}
	return new HyperLogLog((void *)new_hll);
}

unique_ptr<HyperLogLog> HyperLogLog::Merge(HyperLogLog logs[], idx_t count) {
	auto hlls_uptr = unique_ptr<duckdb_hll::robj *[]> {
		new duckdb_hll::robj *[count]
	};
	auto hlls = hlls_uptr.get();
	for (idx_t i = 0; i < count; i++) {
		hlls[i] = (duckdb_hll::robj *)logs[i].hll;
	}
	auto new_hll = duckdb_hll::hll_merge(hlls, count);
	if (!new_hll) {
		throw InternalException("Could not merge HLLs");
	}
	return unique_ptr<HyperLogLog>(new HyperLogLog((void *)new_hll));
}

idx_t HyperLogLog::GetSize() {
	return duckdb_hll::get_size();
}

data_ptr_t HyperLogLog::GetPtr() const {
	return (data_ptr_t)((duckdb_hll::robj *)hll)->ptr;
}

unique_ptr<HyperLogLog> HyperLogLog::Copy() {
	auto result = make_unique<HyperLogLog>();
	lock_guard<mutex> guard(lock);
	memcpy(result->GetPtr(), GetPtr(), GetSize());
	D_ASSERT(result->Count() == Count());
	return result;
}

void HyperLogLog::Serialize(FieldWriter &writer) const {
	writer.WriteField<HLLStorageType>(HLLStorageType::UNCOMPRESSED);
	writer.WriteBlob(GetPtr(), GetSize());
}

unique_ptr<HyperLogLog> HyperLogLog::Deserialize(FieldReader &reader) {
	auto result = make_unique<HyperLogLog>();
	auto storage_type = reader.ReadRequired<HLLStorageType>();
	switch (storage_type) {
	case HLLStorageType::UNCOMPRESSED:
		reader.ReadBlob(result->GetPtr(), GetSize());
		break;
	default:
		throw SerializationException("Unknown HyperLogLog storage type!");
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Vectorized HLL implementation
//===--------------------------------------------------------------------===//
//! Taken from https://nullprogram.com/blog/2018/07/31/
template <class T>
inline uint64_t TemplatedHash(const T &elem) {
	uint64_t x = elem;
	x ^= x >> 30;
	x *= UINT64_C(0xbf58476d1ce4e5b9);
	x ^= x >> 27;
	x *= UINT64_C(0x94d049bb133111eb);
	x ^= x >> 31;
	return x;
}

template <>
inline uint64_t TemplatedHash(const hugeint_t &elem) {
	return TemplatedHash<uint64_t>(Load<uint64_t>((data_ptr_t)&elem.upper)) ^ TemplatedHash<uint64_t>(elem.lower);
}

template <idx_t rest>
inline void CreateIntegerRecursive(const data_ptr_t &data, uint64_t &x) {
	x ^= (uint64_t)data[rest - 1] << ((rest - 1) * 8);
	return CreateIntegerRecursive<rest - 1>(data, x);
}

template <>
inline void CreateIntegerRecursive<1>(const data_ptr_t &data, uint64_t &x) {
	x ^= (uint64_t)data[0];
}

inline uint64_t HashOtherSize(const data_ptr_t &data, const idx_t &len) {
	uint64_t x = 0;
	switch (len & 7) {
	case 7:
		CreateIntegerRecursive<7>(data, x);
		break;
	case 6:
		CreateIntegerRecursive<6>(data, x);
		break;
	case 5:
		CreateIntegerRecursive<5>(data, x);
		break;
	case 4:
		CreateIntegerRecursive<4>(data, x);
		break;
	case 3:
		CreateIntegerRecursive<3>(data, x);
		break;
	case 2:
		CreateIntegerRecursive<2>(data, x);
		break;
	case 1:
		CreateIntegerRecursive<1>(data, x);
		break;
	case 0:
		break;
	}
	return TemplatedHash<uint64_t>(x);
}

template <>
inline uint64_t TemplatedHash(const string_t &elem) {
	data_ptr_t data = (data_ptr_t)elem.GetDataUnsafe();
	const auto &len = elem.GetSize();
	uint64_t h = 0;
	for (idx_t i = 0; i + sizeof(uint64_t) <= len; i += sizeof(uint64_t)) {
		h ^= TemplatedHash<uint64_t>(Load<uint64_t>(data));
		data += sizeof(uint64_t);
	}
	switch (len & (sizeof(uint64_t) - 1)) {
	case 4:
		h ^= TemplatedHash<uint32_t>(Load<uint32_t>(data));
		break;
	case 2:
		h ^= TemplatedHash<uint16_t>(Load<uint16_t>(data));
		break;
	case 1:
		h ^= TemplatedHash<uint8_t>(Load<uint8_t>(data));
		break;
	default:
		h ^= HashOtherSize(data, len);
	}
	return h;
}

template <class T>
void TemplatedComputeHashes(VectorData &vdata, const idx_t &count, uint64_t hashes[]) {
	T *data = (T *)vdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		if (vdata.validity.RowIsValid(idx)) {
			hashes[i] = TemplatedHash<T>(data[idx]);
		} else {
			hashes[i] = 0;
		}
	}
}

static void ComputeHashes(VectorData &vdata, const LogicalType &type, uint64_t hashes[], idx_t count) {
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
	case PhysicalType::UINT8:
		return TemplatedComputeHashes<uint8_t>(vdata, count, hashes);
	case PhysicalType::INT16:
	case PhysicalType::UINT16:
		return TemplatedComputeHashes<uint16_t>(vdata, count, hashes);
	case PhysicalType::INT32:
	case PhysicalType::UINT32:
	case PhysicalType::FLOAT:
		return TemplatedComputeHashes<uint32_t>(vdata, count, hashes);
	case PhysicalType::INT64:
	case PhysicalType::UINT64:
	case PhysicalType::DOUBLE:
		return TemplatedComputeHashes<uint64_t>(vdata, count, hashes);
	case PhysicalType::INT128:
	case PhysicalType::INTERVAL:
		static_assert(sizeof(hugeint_t) == sizeof(interval_t), "ComputeHashes assumes these are the same size!");
		return TemplatedComputeHashes<hugeint_t>(vdata, count, hashes);
	case PhysicalType::VARCHAR:
		return TemplatedComputeHashes<string_t>(vdata, count, hashes);
	default:
		throw InternalException("Unimplemented type for HyperLogLog::ComputeHashes");
	}
}

//! Taken from https://stackoverflow.com/a/72088344
static inline uint8_t CountTrailingZeros(uint64_t &x) {
	static constexpr const uint64_t DEBRUIJN = 0x03f79d71b4cb0a89;
	static constexpr const uint8_t LOOKUP[] = {0,  47, 1,  56, 48, 27, 2,  60, 57, 49, 41, 37, 28, 16, 3,  61,
	                                           54, 58, 35, 52, 50, 42, 21, 44, 38, 32, 29, 23, 17, 11, 4,  62,
	                                           46, 55, 26, 59, 40, 36, 15, 53, 34, 51, 20, 43, 31, 22, 10, 45,
	                                           25, 39, 14, 33, 19, 30, 9,  24, 13, 18, 8,  12, 7,  6,  5,  63};
	return LOOKUP[(DEBRUIJN * (x ^ (x - 1))) >> 58];
}

static inline void ComputeIndexAndCount(uint64_t &hash, uint8_t &prefix) {
	uint64_t index = hash & ((1 << 12) - 1); /* Register index. */
	hash >>= 12;                             /* Remove bits used to address the register. */
	hash |= ((uint64_t)1 << (64 - 12));      /* Make sure the count will be <= Q+1. */

	prefix = CountTrailingZeros(hash) + 1; /* Add 1 since we count the "00000...1" pattern. */
	hash = index;
}

void HyperLogLog::ProcessEntries(VectorData &vdata, const LogicalType &type, uint64_t hashes[], uint8_t counts[],
                                 idx_t count) {
	ComputeHashes(vdata, type, hashes, count);
	for (idx_t i = 0; i < count; i++) {
		ComputeIndexAndCount(hashes[i], counts[i]);
	}
}

void HyperLogLog::AddToLogs(VectorData &vdata, idx_t count, uint64_t indices[], uint8_t counts[], HyperLogLog **logs[],
                            const SelectionVector *log_sel) {
	AddToLogsInternal(vdata, count, indices, counts, (void ****)logs, log_sel);
}

void HyperLogLog::AddToLog(VectorData &vdata, idx_t count, uint64_t indices[], uint8_t counts[]) {
	lock_guard<mutex> guard(lock);
	AddToSingleLogInternal(vdata, count, indices, counts, hll);
}

} // namespace duckdb













namespace duckdb {

bool Interval::FromString(const string &str, interval_t &result) {
	string error_message;
	return Interval::FromCString(str.c_str(), str.size(), result, &error_message, false);
}

template <class T>
void IntervalTryAddition(T &target, int64_t input, int64_t multiplier) {
	int64_t addition;
	if (!TryMultiplyOperator::Operation<int64_t, int64_t, int64_t>(input, multiplier, addition)) {
		throw OutOfRangeException("interval value is out of range");
	}
	T addition_base = Cast::Operation<int64_t, T>(addition);
	if (!TryAddOperator::Operation<T, T, T>(target, addition_base, target)) {
		throw OutOfRangeException("interval value is out of range");
	}
}

bool Interval::FromCString(const char *str, idx_t len, interval_t &result, string *error_message, bool strict) {
	idx_t pos = 0;
	idx_t start_pos;
	bool negative;
	bool found_any = false;
	int64_t number;
	DatePartSpecifier specifier;
	string specifier_str;

	result.days = 0;
	result.micros = 0;
	result.months = 0;

	if (len == 0) {
		return false;
	}

	switch (str[pos]) {
	case '@':
		pos++;
		goto standard_interval;
	case 'P':
	case 'p':
		pos++;
		goto posix_interval;
	default:
		goto standard_interval;
	}
standard_interval:
	// start parsing a standard interval (e.g. 2 years 3 months...)
	for (; pos < len; pos++) {
		char c = str[pos];
		if (c == ' ' || c == '\t' || c == '\n') {
			// skip spaces
			continue;
		} else if (c >= '0' && c <= '9') {
			// start parsing a positive number
			negative = false;
			goto interval_parse_number;
		} else if (c == '-') {
			// negative number
			negative = true;
			pos++;
			goto interval_parse_number;
		} else if (c == 'a' || c == 'A') {
			// parse the word "ago" as the final specifier
			goto interval_parse_ago;
		} else {
			// unrecognized character, expected a number or end of string
			return false;
		}
	}
	goto end_of_string;
interval_parse_number:
	start_pos = pos;
	for (; pos < len; pos++) {
		char c = str[pos];
		if (c >= '0' && c <= '9') {
			// the number continues
			continue;
		} else if (c == ':') {
			// colon: we are parsing a time
			goto interval_parse_time;
		} else {
			if (pos == start_pos) {
				return false;
			}
			// finished the number, parse it from the string
			string_t nr_string(str + start_pos, pos - start_pos);
			number = Cast::Operation<string_t, int64_t>(nr_string);
			if (negative) {
				number = -number;
			}
			goto interval_parse_identifier;
		}
	}
	goto end_of_string;
interval_parse_time : {
	// parse the remainder of the time as a Time type
	dtime_t time;
	idx_t pos;
	if (!Time::TryConvertTime(str + start_pos, len - start_pos, pos, time)) {
		return false;
	}
	result.micros += time.micros;
	found_any = true;
	goto end_of_string;
}
interval_parse_identifier:
	for (; pos < len; pos++) {
		char c = str[pos];
		if (c == ' ' || c == '\t' || c == '\n') {
			// skip spaces at the start
			continue;
		} else {
			break;
		}
	}
	// now parse the identifier
	start_pos = pos;
	for (; pos < len; pos++) {
		char c = str[pos];
		if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) {
			// keep parsing the string
			continue;
		} else {
			break;
		}
	}
	specifier_str = string(str + start_pos, pos - start_pos);
	if (!TryGetDatePartSpecifier(specifier_str, specifier)) {
		HandleCastError::AssignError(StringUtil::Format("extract specifier \"%s\" not recognized", specifier_str),
		                             error_message);
		return false;
	}
	// add the specifier to the interval
	switch (specifier) {
	case DatePartSpecifier::MILLENNIUM:
		IntervalTryAddition<int32_t>(result.months, number, MONTHS_PER_MILLENIUM);
		break;
	case DatePartSpecifier::CENTURY:
		IntervalTryAddition<int32_t>(result.months, number, MONTHS_PER_CENTURY);
		break;
	case DatePartSpecifier::DECADE:
		IntervalTryAddition<int32_t>(result.months, number, MONTHS_PER_DECADE);
		break;
	case DatePartSpecifier::YEAR:
		IntervalTryAddition<int32_t>(result.months, number, MONTHS_PER_YEAR);
		break;
	case DatePartSpecifier::QUARTER:
		IntervalTryAddition<int32_t>(result.months, number, MONTHS_PER_QUARTER);
		break;
	case DatePartSpecifier::MONTH:
		IntervalTryAddition<int32_t>(result.months, number, 1);
		break;
	case DatePartSpecifier::DAY:
		IntervalTryAddition<int32_t>(result.days, number, 1);
		break;
	case DatePartSpecifier::WEEK:
		IntervalTryAddition<int32_t>(result.days, number, DAYS_PER_WEEK);
		break;
	case DatePartSpecifier::MICROSECONDS:
		IntervalTryAddition<int64_t>(result.micros, number, 1);
		break;
	case DatePartSpecifier::MILLISECONDS:
		IntervalTryAddition<int64_t>(result.micros, number, MICROS_PER_MSEC);
		break;
	case DatePartSpecifier::SECOND:
		IntervalTryAddition<int64_t>(result.micros, number, MICROS_PER_SEC);
		break;
	case DatePartSpecifier::MINUTE:
		IntervalTryAddition<int64_t>(result.micros, number, MICROS_PER_MINUTE);
		break;
	case DatePartSpecifier::HOUR:
		IntervalTryAddition<int64_t>(result.micros, number, MICROS_PER_HOUR);
		break;
	default:
		HandleCastError::AssignError(
		    StringUtil::Format("extract specifier \"%s\" not supported for interval", specifier_str), error_message);
		return false;
	}
	found_any = true;
	goto standard_interval;
interval_parse_ago:
	D_ASSERT(str[pos] == 'a' || str[pos] == 'A');
	// parse the "ago" string at the end of the interval
	if (len - pos < 3) {
		return false;
	}
	pos++;
	if (!(str[pos] == 'g' || str[pos] == 'G')) {
		return false;
	}
	pos++;
	if (!(str[pos] == 'o' || str[pos] == 'O')) {
		return false;
	}
	pos++;
	// parse any trailing whitespace
	for (; pos < len; pos++) {
		char c = str[pos];
		if (c == ' ' || c == '\t' || c == '\n') {
			continue;
		} else {
			return false;
		}
	}
	// invert all the values
	result.months = -result.months;
	result.days = -result.days;
	result.micros = -result.micros;
	goto end_of_string;
end_of_string:
	if (!found_any) {
		// end of string and no identifiers were found: cannot convert empty interval
		return false;
	}
	return true;
posix_interval:
	return false;
}

string Interval::ToString(const interval_t &interval) {
	char buffer[70];
	idx_t length = IntervalToStringCast::Format(interval, buffer);
	return string(buffer, length);
}

int64_t Interval::GetMilli(const interval_t &val) {
	int64_t milli_month, milli_day, milli;
	if (!TryMultiplyOperator::Operation((int64_t)val.months, Interval::MICROS_PER_MONTH / 1000, milli_month)) {
		throw ConversionException("Could not convert Interval to Milliseconds");
	}
	if (!TryMultiplyOperator::Operation((int64_t)val.days, Interval::MICROS_PER_DAY / 1000, milli_day)) {
		throw ConversionException("Could not convert Interval to Milliseconds");
	}
	milli = val.micros / 1000;
	if (!TryAddOperator::Operation<int64_t, int64_t, int64_t>(milli, milli_month, milli)) {
		throw ConversionException("Could not convert Interval to Milliseconds");
	}
	if (!TryAddOperator::Operation<int64_t, int64_t, int64_t>(milli, milli_day, milli)) {
		throw ConversionException("Could not convert Interval to Milliseconds");
	}
	return milli;
}

int64_t Interval::GetMicro(const interval_t &val) {
	int64_t micro_month, micro_day, micro_total;
	micro_total = val.micros;
	if (!TryMultiplyOperator::Operation((int64_t)val.months, MICROS_PER_MONTH, micro_month)) {
		throw ConversionException("Could not convert Month to Microseconds");
	}
	if (!TryMultiplyOperator::Operation((int64_t)val.days, MICROS_PER_DAY, micro_day)) {
		throw ConversionException("Could not convert Day to Microseconds");
	}
	if (!TryAddOperator::Operation<int64_t, int64_t, int64_t>(micro_total, micro_month, micro_total)) {
		throw ConversionException("Could not convert Interval to Microseconds");
	}
	if (!TryAddOperator::Operation<int64_t, int64_t, int64_t>(micro_total, micro_day, micro_total)) {
		throw ConversionException("Could not convert Interval to Microseconds");
	}

	return micro_total;
}

int64_t Interval::GetNanoseconds(const interval_t &val) {
	int64_t nano;
	const auto micro_total = GetMicro(val);
	if (!TryMultiplyOperator::Operation(micro_total, NANOS_PER_MICRO, nano)) {
		throw ConversionException("Could not convert Interval to Nanoseconds");
	}

	return nano;
}

interval_t Interval::GetAge(timestamp_t timestamp_1, timestamp_t timestamp_2) {
	D_ASSERT(Timestamp::IsFinite(timestamp_1) && Timestamp::IsFinite(timestamp_2));
	date_t date1, date2;
	dtime_t time1, time2;

	Timestamp::Convert(timestamp_1, date1, time1);
	Timestamp::Convert(timestamp_2, date2, time2);

	// and from date extract the years, months and days
	int32_t year1, month1, day1;
	int32_t year2, month2, day2;
	Date::Convert(date1, year1, month1, day1);
	Date::Convert(date2, year2, month2, day2);
	// finally perform the differences
	auto year_diff = year1 - year2;
	auto month_diff = month1 - month2;
	auto day_diff = day1 - day2;

	// and from time extract hours, minutes, seconds and milliseconds
	int32_t hour1, min1, sec1, micros1;
	int32_t hour2, min2, sec2, micros2;
	Time::Convert(time1, hour1, min1, sec1, micros1);
	Time::Convert(time2, hour2, min2, sec2, micros2);
	// finally perform the differences
	auto hour_diff = hour1 - hour2;
	auto min_diff = min1 - min2;
	auto sec_diff = sec1 - sec2;
	auto micros_diff = micros1 - micros2;

	// flip sign if necessary
	bool sign_flipped = false;
	if (timestamp_1 < timestamp_2) {
		year_diff = -year_diff;
		month_diff = -month_diff;
		day_diff = -day_diff;
		hour_diff = -hour_diff;
		min_diff = -min_diff;
		sec_diff = -sec_diff;
		micros_diff = -micros_diff;
		sign_flipped = true;
	}
	// now propagate any negative field into the next higher field
	while (micros_diff < 0) {
		micros_diff += MICROS_PER_SEC;
		sec_diff--;
	}
	while (sec_diff < 0) {
		sec_diff += SECS_PER_MINUTE;
		min_diff--;
	}
	while (min_diff < 0) {
		min_diff += MINS_PER_HOUR;
		hour_diff--;
	}
	while (hour_diff < 0) {
		hour_diff += HOURS_PER_DAY;
		day_diff--;
	}
	while (day_diff < 0) {
		if (timestamp_1 < timestamp_2) {
			day_diff += Date::IsLeapYear(year1) ? Date::LEAP_DAYS[month1] : Date::NORMAL_DAYS[month1];
			month_diff--;
		} else {
			day_diff += Date::IsLeapYear(year2) ? Date::LEAP_DAYS[month2] : Date::NORMAL_DAYS[month2];
			month_diff--;
		}
	}
	while (month_diff < 0) {
		month_diff += MONTHS_PER_YEAR;
		year_diff--;
	}

	// recover sign if necessary
	if (sign_flipped) {
		year_diff = -year_diff;
		month_diff = -month_diff;
		day_diff = -day_diff;
		hour_diff = -hour_diff;
		min_diff = -min_diff;
		sec_diff = -sec_diff;
		micros_diff = -micros_diff;
	}
	interval_t interval;
	interval.months = year_diff * MONTHS_PER_YEAR + month_diff;
	interval.days = day_diff;
	interval.micros = Time::FromTime(hour_diff, min_diff, sec_diff, micros_diff).micros;

	return interval;
}

interval_t Interval::GetDifference(timestamp_t timestamp_1, timestamp_t timestamp_2) {
	if (!Timestamp::IsFinite(timestamp_1) || !Timestamp::IsFinite(timestamp_2)) {
		throw InvalidInputException("Cannot subtract infinite timestamps");
	}
	const auto us_1 = Timestamp::GetEpochMicroSeconds(timestamp_1);
	const auto us_2 = Timestamp::GetEpochMicroSeconds(timestamp_2);
	int64_t delta_us;
	if (!TrySubtractOperator::Operation(us_1, us_2, delta_us)) {
		throw ConversionException("Timestamp difference is out of bounds");
	}
	return FromMicro(delta_us);
}

interval_t Interval::FromMicro(int64_t delta_us) {
	interval_t result;
	result.months = 0;
	result.days = delta_us / Interval::MICROS_PER_DAY;
	result.micros = delta_us % Interval::MICROS_PER_DAY;

	return result;
}

static void NormalizeIntervalEntries(interval_t input, int64_t &months, int64_t &days, int64_t &micros) {
	int64_t extra_months_d = input.days / Interval::DAYS_PER_MONTH;
	int64_t extra_months_micros = input.micros / Interval::MICROS_PER_MONTH;
	input.days -= extra_months_d * Interval::DAYS_PER_MONTH;
	input.micros -= extra_months_micros * Interval::MICROS_PER_MONTH;

	int64_t extra_days_micros = input.micros / Interval::MICROS_PER_DAY;
	input.micros -= extra_days_micros * Interval::MICROS_PER_DAY;

	months = input.months + extra_months_d + extra_months_micros;
	days = input.days + extra_days_micros;
	micros = input.micros;
}

bool Interval::Equals(interval_t left, interval_t right) {
	return left.months == right.months && left.days == right.days && left.micros == right.micros;
}

bool Interval::GreaterThan(interval_t left, interval_t right) {
	int64_t lmonths, ldays, lmicros;
	int64_t rmonths, rdays, rmicros;
	NormalizeIntervalEntries(left, lmonths, ldays, lmicros);
	NormalizeIntervalEntries(right, rmonths, rdays, rmicros);

	if (lmonths > rmonths) {
		return true;
	} else if (lmonths < rmonths) {
		return false;
	}
	if (ldays > rdays) {
		return true;
	} else if (ldays < rdays) {
		return false;
	}
	return lmicros > rmicros;
}

bool Interval::GreaterThanEquals(interval_t left, interval_t right) {
	return GreaterThan(left, right) || Equals(left, right);
}

date_t Interval::Add(date_t left, interval_t right) {
	if (!Date::IsFinite(left)) {
		return left;
	}
	date_t result;
	if (right.months != 0) {
		int32_t year, month, day;
		Date::Convert(left, year, month, day);
		int32_t year_diff = right.months / Interval::MONTHS_PER_YEAR;
		year += year_diff;
		month += right.months - year_diff * Interval::MONTHS_PER_YEAR;
		if (month > Interval::MONTHS_PER_YEAR) {
			year++;
			month -= Interval::MONTHS_PER_YEAR;
		} else if (month <= 0) {
			year--;
			month += Interval::MONTHS_PER_YEAR;
		}
		day = MinValue<int32_t>(day, Date::MonthDays(year, month));
		result = Date::FromDate(year, month, day);
	} else {
		result = left;
	}
	if (right.days != 0) {
		if (!TryAddOperator::Operation(result.days, right.days, result.days)) {
			throw OutOfRangeException("Date out of range");
		}
	}
	if (right.micros != 0) {
		if (!TryAddOperator::Operation(result.days, int32_t(right.micros / Interval::MICROS_PER_DAY), result.days)) {
			throw OutOfRangeException("Date out of range");
		}
	}
	if (!Date::IsFinite(result)) {
		throw OutOfRangeException("Date out of range");
	}
	return result;
}

dtime_t Interval::Add(dtime_t left, interval_t right, date_t &date) {
	int64_t diff = right.micros - ((right.micros / Interval::MICROS_PER_DAY) * Interval::MICROS_PER_DAY);
	left += diff;
	if (left.micros >= Interval::MICROS_PER_DAY) {
		left.micros -= Interval::MICROS_PER_DAY;
		date.days++;
	} else if (left.micros < 0) {
		left.micros += Interval::MICROS_PER_DAY;
		date.days--;
	}
	return left;
}

timestamp_t Interval::Add(timestamp_t left, interval_t right) {
	if (!Timestamp::IsFinite(left)) {
		return left;
	}
	date_t date;
	dtime_t time;
	Timestamp::Convert(left, date, time);
	auto new_date = Interval::Add(date, right);
	auto new_time = Interval::Add(time, right, new_date);
	return Timestamp::FromDatetime(new_date, new_time);
}

} // namespace duckdb


namespace duckdb {

RowDataCollection::RowDataCollection(BufferManager &buffer_manager, idx_t block_capacity, idx_t entry_size,
                                     bool keep_pinned)
    : buffer_manager(buffer_manager), count(0), block_capacity(block_capacity), entry_size(entry_size),
      keep_pinned(keep_pinned) {
	D_ASSERT(block_capacity * entry_size >= Storage::BLOCK_SIZE);
}

idx_t RowDataCollection::AppendToBlock(RowDataBlock &block, BufferHandle &handle,
                                       vector<BlockAppendEntry> &append_entries, idx_t remaining, idx_t entry_sizes[]) {
	idx_t append_count = 0;
	data_ptr_t dataptr;
	if (entry_sizes) {
		D_ASSERT(entry_size == 1);
		// compute how many entries fit if entry size is variable
		dataptr = handle.Ptr() + block.byte_offset;
		for (idx_t i = 0; i < remaining; i++) {
			if (block.byte_offset + entry_sizes[i] > block.capacity) {
				if (block.count == 0 && append_count == 0 && entry_sizes[i] > block.capacity) {
					// special case: single entry is bigger than block capacity
					// resize current block to fit the entry, append it, and move to the next block
					block.capacity = entry_sizes[i];
					buffer_manager.ReAllocate(block.block, block.capacity);
					dataptr = handle.Ptr();
					append_count++;
					block.byte_offset += entry_sizes[i];
				}
				break;
			}
			append_count++;
			block.byte_offset += entry_sizes[i];
		}
	} else {
		append_count = MinValue<idx_t>(remaining, block.capacity - block.count);
		dataptr = handle.Ptr() + block.count * entry_size;
	}
	append_entries.emplace_back(dataptr, append_count);
	block.count += append_count;
	return append_count;
}

vector<BufferHandle> RowDataCollection::Build(idx_t added_count, data_ptr_t key_locations[], idx_t entry_sizes[],
                                              const SelectionVector *sel) {
	vector<BufferHandle> handles;
	vector<BlockAppendEntry> append_entries;

	// first allocate space of where to serialize the keys and payload columns
	idx_t remaining = added_count;
	{
		// first append to the last block (if any)
		lock_guard<mutex> append_lock(rdc_lock);
		count += added_count;

		if (!blocks.empty()) {
			auto &last_block = blocks.back();
			if (last_block.count < last_block.capacity) {
				// last block has space: pin the buffer of this block
				auto handle = buffer_manager.Pin(last_block.block);
				// now append to the block
				idx_t append_count = AppendToBlock(last_block, handle, append_entries, remaining, entry_sizes);
				remaining -= append_count;
				handles.push_back(move(handle));
			}
		}
		while (remaining > 0) {
			// now for the remaining data, allocate new buffers to store the data and append there
			RowDataBlock new_block(buffer_manager, block_capacity, entry_size);
			auto handle = buffer_manager.Pin(new_block.block);

			// offset the entry sizes array if we have added entries already
			idx_t *offset_entry_sizes = entry_sizes ? entry_sizes + added_count - remaining : nullptr;

			idx_t append_count = AppendToBlock(new_block, handle, append_entries, remaining, offset_entry_sizes);
			D_ASSERT(new_block.count > 0);
			remaining -= append_count;

			blocks.push_back(move(new_block));
			if (keep_pinned) {
				pinned_blocks.push_back(move(handle));
			} else {
				handles.push_back(move(handle));
			}
		}
	}
	// now set up the key_locations based on the append entries
	idx_t append_idx = 0;
	for (auto &append_entry : append_entries) {
		idx_t next = append_idx + append_entry.count;
		if (entry_sizes) {
			for (; append_idx < next; append_idx++) {
				key_locations[append_idx] = append_entry.baseptr;
				append_entry.baseptr += entry_sizes[append_idx];
			}
		} else {
			for (; append_idx < next; append_idx++) {
				auto idx = sel->get_index(append_idx);
				key_locations[idx] = append_entry.baseptr;
				append_entry.baseptr += entry_size;
			}
		}
	}
	// return the unique pointers to the handles because they must stay pinned
	return handles;
}

void RowDataCollection::Merge(RowDataCollection &other) {
	RowDataCollection temp(buffer_manager, Storage::BLOCK_SIZE, 1);
	{
		//	One lock at a time to avoid deadlocks
		lock_guard<mutex> read_lock(other.rdc_lock);
		temp.count = other.count;
		temp.block_capacity = other.block_capacity;
		temp.entry_size = other.entry_size;
		temp.blocks = move(other.blocks);
		other.count = 0;
	}

	lock_guard<mutex> write_lock(rdc_lock);
	count += temp.count;
	block_capacity = MaxValue(block_capacity, temp.block_capacity);
	entry_size = MaxValue(entry_size, temp.entry_size);
	for (auto &block : temp.blocks) {
		blocks.emplace_back(move(block));
	}
	for (auto &handle : temp.pinned_blocks) {
		pinned_blocks.emplace_back(move(handle));
	}
}

} // namespace duckdb
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/row_layout.cpp
//
//
//===----------------------------------------------------------------------===//





namespace duckdb {

RowLayout::RowLayout()
    : flag_width(0), data_width(0), aggr_width(0), row_width(0), all_constant(true), heap_pointer_offset(0) {
}

void RowLayout::Initialize(vector<LogicalType> types_p, Aggregates aggregates_p, bool align) {
	offsets.clear();
	types = move(types_p);

	// Null mask at the front - 1 bit per value.
	flag_width = ValidityBytes::ValidityMaskSize(types.size());
	row_width = flag_width;

	// Whether all columns are constant size.
	for (const auto &type : types) {
		all_constant = all_constant && TypeIsConstantSize(type.InternalType());
	}

	// This enables pointer swizzling for out-of-core computation.
	if (!all_constant) {
		// When unswizzled the pointer lives here.
		// When swizzled, the pointer is replaced by an offset.
		heap_pointer_offset = row_width;
		// The 8 byte pointer will be replaced with an 8 byte idx_t when swizzled.
		// However, this cannot be sizeof(data_ptr_t), since 32 bit builds use 4 byte pointers.
		row_width += sizeof(idx_t);
	}

	// Data columns. No alignment required.
	for (const auto &type : types) {
		offsets.push_back(row_width);
		const auto internal_type = type.InternalType();
		if (TypeIsConstantSize(internal_type) || internal_type == PhysicalType::VARCHAR) {
			row_width += GetTypeIdSize(type.InternalType());
		} else {
			// Variable size types use pointers to the actual data (can be swizzled).
			// Again, we would use sizeof(data_ptr_t), but this is not guaranteed to be equal to sizeof(idx_t).
			row_width += sizeof(idx_t);
		}
	}

	// Alignment padding for aggregates
#ifndef DUCKDB_ALLOW_UNDEFINED
	if (align) {
		row_width = AlignValue(row_width);
	}
#endif
	data_width = row_width - flag_width;

	// Aggregate fields.
	aggregates = move(aggregates_p);
	for (auto &aggregate : aggregates) {
		offsets.push_back(row_width);
		row_width += aggregate.payload_size;
#ifndef DUCKDB_ALLOW_UNDEFINED
		D_ASSERT(aggregate.payload_size == AlignValue(aggregate.payload_size));
#endif
	}
	aggr_width = row_width - data_width - flag_width;

	// Alignment padding for the next row
#ifndef DUCKDB_ALLOW_UNDEFINED
	if (align) {
		row_width = AlignValue(row_width);
	}
#endif
}

void RowLayout::Initialize(vector<LogicalType> types_p, bool align) {
	Initialize(move(types_p), Aggregates(), align);
}

void RowLayout::Initialize(Aggregates aggregates_p, bool align) {
	Initialize(vector<LogicalType>(), move(aggregates_p), align);
}

} // namespace duckdb




namespace duckdb {

SelectionData::SelectionData(idx_t count) {
	owned_data = unique_ptr<sel_t[]>(new sel_t[count]);
#ifdef DEBUG
	for (idx_t i = 0; i < count; i++) {
		owned_data[i] = std::numeric_limits<sel_t>::max();
	}
#endif
}

// LCOV_EXCL_START
string SelectionVector::ToString(idx_t count) const {
	string result = "Selection Vector (" + to_string(count) + ") [";
	for (idx_t i = 0; i < count; i++) {
		if (i != 0) {
			result += ", ";
		}
		result += to_string(get_index(i));
	}
	result += "]";
	return result;
}

void SelectionVector::Print(idx_t count) const {
	Printer::Print(ToString(count));
}
// LCOV_EXCL_STOP

buffer_ptr<SelectionData> SelectionVector::Slice(const SelectionVector &sel, idx_t count) const {
	auto data = make_buffer<SelectionData>(count);
	auto result_ptr = data->owned_data.get();
	// for every element, we perform result[i] = target[new[i]]
	for (idx_t i = 0; i < count; i++) {
		auto new_idx = sel.get_index(i);
		auto idx = this->get_index(new_idx);
		result_ptr[i] = idx;
	}
	return data;
}

} // namespace duckdb







#include <cstring>

namespace duckdb {

StringHeap::StringHeap() : allocator(Allocator::DefaultAllocator()) {
}

void StringHeap::Destroy() {
	allocator.Destroy();
}

void StringHeap::Move(StringHeap &other) {
	other.allocator.Move(allocator);
}

string_t StringHeap::AddString(const char *data, idx_t len) {
	D_ASSERT(Utf8Proc::Analyze(data, len) != UnicodeType::INVALID);
	return AddBlob(data, len);
}

string_t StringHeap::AddString(const char *data) {
	return AddString(data, strlen(data));
}

string_t StringHeap::AddString(const string &data) {
	return AddString(data.c_str(), data.size());
}

string_t StringHeap::AddString(const string_t &data) {
	return AddString(data.GetDataUnsafe(), data.GetSize());
}

string_t StringHeap::AddBlob(const char *data, idx_t len) {
	auto insert_string = EmptyString(len);
	auto insert_pos = insert_string.GetDataWriteable();
	memcpy(insert_pos, data, len);
	insert_string.Finalize();
	return insert_string;
}

string_t StringHeap::AddBlob(const string_t &data) {
	return AddBlob(data.GetDataUnsafe(), data.GetSize());
}

string_t StringHeap::EmptyString(idx_t len) {
	D_ASSERT(len >= string_t::INLINE_LENGTH);
	auto insert_pos = (const char *)allocator.Allocate(len);
	return string_t(insert_pos, len);
}

} // namespace duckdb





namespace duckdb {

void string_t::Verify() {
	auto dataptr = GetDataUnsafe();
	(void)dataptr;
	D_ASSERT(dataptr);

#ifdef DEBUG
	auto utf_type = Utf8Proc::Analyze(dataptr, GetSize());
	D_ASSERT(utf_type != UnicodeType::INVALID);
#endif

	// verify that the prefix contains the first four characters of the string
	for (idx_t i = 0; i < MinValue<uint32_t>(PREFIX_LENGTH, GetSize()); i++) {
		D_ASSERT(GetPrefix()[i] == dataptr[i]);
	}
	// verify that for strings with length < INLINE_LENGTH, the rest of the string is zero
	for (idx_t i = GetSize(); i < INLINE_LENGTH; i++) {
		D_ASSERT(GetDataUnsafe()[i] == '\0');
	}
}

void string_t::VerifyNull() {
	for (idx_t i = 0; i < GetSize(); i++) {
		D_ASSERT(GetDataUnsafe()[i] != '\0');
	}
}

} // namespace duckdb








#include <cstring>
#include <sstream>
#include <cctype>

namespace duckdb {

static_assert(sizeof(dtime_t) == sizeof(int64_t), "dtime_t was padded");

// string format is hh:mm:ss.microsecondsZ
// microseconds and Z are optional
// ISO 8601

bool Time::TryConvertInternal(const char *buf, idx_t len, idx_t &pos, dtime_t &result, bool strict) {
	int32_t hour = -1, min = -1, sec = -1, micros = -1;
	pos = 0;

	if (len == 0) {
		return false;
	}

	int sep;

	// skip leading spaces
	while (pos < len && StringUtil::CharacterIsSpace(buf[pos])) {
		pos++;
	}

	if (pos >= len) {
		return false;
	}

	if (!StringUtil::CharacterIsDigit(buf[pos])) {
		return false;
	}

	if (!Date::ParseDoubleDigit(buf, len, pos, hour)) {
		return false;
	}
	if (hour < 0 || hour >= 24) {
		return false;
	}

	if (pos >= len) {
		return false;
	}

	// fetch the separator
	sep = buf[pos++];
	if (sep != ':') {
		// invalid separator
		return false;
	}

	if (!Date::ParseDoubleDigit(buf, len, pos, min)) {
		return false;
	}
	if (min < 0 || min >= 60) {
		return false;
	}

	if (pos >= len) {
		return false;
	}

	if (buf[pos++] != sep) {
		return false;
	}

	if (!Date::ParseDoubleDigit(buf, len, pos, sec)) {
		return false;
	}
	if (sec < 0 || sec >= 60) {
		return false;
	}

	micros = 0;
	if (pos < len && buf[pos] == '.') {
		pos++;
		// we expect some microseconds
		int32_t mult = 100000;
		for (; pos < len && StringUtil::CharacterIsDigit(buf[pos]); pos++, mult /= 10) {
			if (mult > 0) {
				micros += (buf[pos] - '0') * mult;
			}
		}
	}

	// in strict mode, check remaining string for non-space characters
	if (strict) {
		// skip trailing spaces
		while (pos < len && StringUtil::CharacterIsSpace(buf[pos])) {
			pos++;
		}
		// check position. if end was not reached, non-space chars remaining
		if (pos < len) {
			return false;
		}
	}

	result = Time::FromTime(hour, min, sec, micros);
	return true;
}

bool Time::TryConvertTime(const char *buf, idx_t len, idx_t &pos, dtime_t &result, bool strict) {
	if (!Time::TryConvertInternal(buf, len, pos, result, strict)) {
		if (!strict) {
			// last chance, check if we can parse as timestamp
			timestamp_t timestamp;
			if (Timestamp::TryConvertTimestamp(buf, len, timestamp)) {
				result = Timestamp::GetTime(timestamp);
				return true;
			}
		}
		return false;
	}
	return true;
}

string Time::ConversionError(const string &str) {
	return StringUtil::Format("time field value out of range: \"%s\", "
	                          "expected format is ([YYYY-MM-DD ]HH:MM:SS[.MS])",
	                          str);
}

string Time::ConversionError(string_t str) {
	return Time::ConversionError(str.GetString());
}

dtime_t Time::FromCString(const char *buf, idx_t len, bool strict) {
	dtime_t result;
	idx_t pos;
	if (!Time::TryConvertTime(buf, len, pos, result, strict)) {
		throw ConversionException(ConversionError(string(buf, len)));
	}
	return result;
}

dtime_t Time::FromString(const string &str, bool strict) {
	return Time::FromCString(str.c_str(), str.size(), strict);
}

string Time::ToString(dtime_t time) {
	int32_t time_units[4];
	Time::Convert(time, time_units[0], time_units[1], time_units[2], time_units[3]);

	char micro_buffer[6];
	auto length = TimeToStringCast::Length(time_units, micro_buffer);
	auto buffer = unique_ptr<char[]>(new char[length]);
	TimeToStringCast::Format(buffer.get(), length, time_units, micro_buffer);
	return string(buffer.get(), length);
}

string Time::ToUTCOffset(int hour_offset, int minute_offset) {
	dtime_t time((hour_offset * Interval::MINS_PER_HOUR + minute_offset) * Interval::MICROS_PER_MINUTE);

	char buffer[1 + 2 + 1 + 2];
	idx_t length = 0;
	buffer[length++] = (time.micros < 0 ? '-' : '+');
	time.micros = std::abs(time.micros);

	int32_t time_units[4];
	Time::Convert(time, time_units[0], time_units[1], time_units[2], time_units[3]);

	TimeToStringCast::FormatTwoDigits(buffer + length, time_units[0]);
	length += 2;
	if (time_units[1]) {
		buffer[length++] = ':';
		TimeToStringCast::FormatTwoDigits(buffer + length, time_units[1]);
		length += 2;
	}

	return string(buffer, length);
}

dtime_t Time::FromTime(int32_t hour, int32_t minute, int32_t second, int32_t microseconds) {
	int64_t result;
	result = hour;                                             // hours
	result = result * Interval::MINS_PER_HOUR + minute;        // hours -> minutes
	result = result * Interval::SECS_PER_MINUTE + second;      // minutes -> seconds
	result = result * Interval::MICROS_PER_SEC + microseconds; // seconds -> microseconds
	return dtime_t(result);
}

// LCOV_EXCL_START
#ifdef DEBUG
static bool AssertValidTime(int32_t hour, int32_t minute, int32_t second, int32_t microseconds) {
	if (hour < 0 || hour >= 24) {
		return false;
	}
	if (minute < 0 || minute >= 60) {
		return false;
	}
	if (second < 0 || second > 60) {
		return false;
	}
	if (microseconds < 0 || microseconds > 1000000) {
		return false;
	}
	return true;
}
#endif
// LCOV_EXCL_STOP

void Time::Convert(dtime_t dtime, int32_t &hour, int32_t &min, int32_t &sec, int32_t &micros) {
	int64_t time = dtime.micros;
	hour = int32_t(time / Interval::MICROS_PER_HOUR);
	time -= int64_t(hour) * Interval::MICROS_PER_HOUR;
	min = int32_t(time / Interval::MICROS_PER_MINUTE);
	time -= int64_t(min) * Interval::MICROS_PER_MINUTE;
	sec = int32_t(time / Interval::MICROS_PER_SEC);
	time -= int64_t(sec) * Interval::MICROS_PER_SEC;
	micros = int32_t(time);
#ifdef DEBUG
	D_ASSERT(AssertValidTime(hour, min, sec, micros));
#endif
}

} // namespace duckdb











#include <ctime>

namespace duckdb {

static_assert(sizeof(timestamp_t) == sizeof(int64_t), "timestamp_t was padded");

// timestamp/datetime uses 64 bits, high 32 bits for date and low 32 bits for time
// string format is YYYY-MM-DDThh:mm:ssZ
// T may be a space
// Z is optional
// ISO 8601
bool Timestamp::TryConvertTimestamp(const char *str, idx_t len, timestamp_t &result) {
	idx_t pos;
	date_t date;
	dtime_t time;
	if (!Date::TryConvertDate(str, len, pos, date)) {
		return false;
	}
	if (pos == len) {
		// no time: only a date or special
		if (date == date_t::infinity()) {
			result = timestamp_t::infinity();
			return true;
		} else if (date == date_t::ninfinity()) {
			result = timestamp_t::ninfinity();
			return true;
		}
		return Timestamp::TryFromDatetime(date, dtime_t(0), result);
	}
	// try to parse a time field
	if (str[pos] == ' ' || str[pos] == 'T') {
		pos++;
	}
	idx_t time_pos = 0;
	if (!Time::TryConvertTime(str + pos, len - pos, time_pos, time)) {
		return false;
	}
	pos += time_pos;
	if (!Timestamp::TryFromDatetime(date, time, result)) {
		return false;
	}
	if (pos < len) {
		// skip a "Z" at the end (as per the ISO8601 specs)
		if (str[pos] == 'Z') {
			pos++;
		}
		int hour_offset, minute_offset;
		if (Timestamp::TryParseUTCOffset(str, pos, len, hour_offset, minute_offset)) {
			result -= hour_offset * Interval::MICROS_PER_HOUR + minute_offset * Interval::MICROS_PER_MINUTE;
		}

		// skip any spaces at the end
		while (pos < len && StringUtil::CharacterIsSpace(str[pos])) {
			pos++;
		}
		if (pos < len) {
			return false;
		}
	}
	return true;
}

string Timestamp::ConversionError(const string &str) {
	return StringUtil::Format("timestamp field value out of range: \"%s\", "
	                          "expected format is (YYYY-MM-DD HH:MM:SS[.MS])",
	                          str);
}

string Timestamp::ConversionError(string_t str) {
	return Timestamp::ConversionError(str.GetString());
}

timestamp_t Timestamp::FromCString(const char *str, idx_t len) {
	timestamp_t result;
	if (!Timestamp::TryConvertTimestamp(str, len, result)) {
		throw ConversionException(Timestamp::ConversionError(string(str, len)));
	}
	return result;
}

bool Timestamp::TryParseUTCOffset(const char *str, idx_t &pos, idx_t len, int &hour_offset, int &minute_offset) {
	minute_offset = 0;
	idx_t curpos = pos;
	// parse the next 3 characters
	if (curpos + 3 > len) {
		// no characters left to parse
		return false;
	}
	char sign_char = str[curpos];
	if (sign_char != '+' && sign_char != '-') {
		// expected either + or -
		return false;
	}
	curpos++;
	if (!StringUtil::CharacterIsDigit(str[curpos]) || !StringUtil::CharacterIsDigit(str[curpos + 1])) {
		// expected +HH or -HH
		return false;
	}
	hour_offset = (str[curpos] - '0') * 10 + (str[curpos + 1] - '0');
	if (sign_char == '-') {
		hour_offset = -hour_offset;
	}
	curpos += 2;

	// optional minute specifier: expected either "MM" or ":MM"
	if (curpos >= len) {
		// done, nothing left
		pos = curpos;
		return true;
	}
	if (str[curpos] == ':') {
		curpos++;
	}
	if (curpos + 2 > len || !StringUtil::CharacterIsDigit(str[curpos]) ||
	    !StringUtil::CharacterIsDigit(str[curpos + 1])) {
		// no MM specifier
		pos = curpos;
		return true;
	}
	// we have an MM specifier: parse it
	minute_offset = (str[curpos] - '0') * 10 + (str[curpos + 1] - '0');
	if (sign_char == '-') {
		minute_offset = -minute_offset;
	}
	pos = curpos + 2;
	return true;
}

timestamp_t Timestamp::FromString(const string &str) {
	return Timestamp::FromCString(str.c_str(), str.size());
}

string Timestamp::ToString(timestamp_t timestamp) {
	if (timestamp == timestamp_t::infinity()) {
		return Date::PINF;
	} else if (timestamp == timestamp_t::ninfinity()) {
		return Date::NINF;
	}
	date_t date;
	dtime_t time;
	Timestamp::Convert(timestamp, date, time);
	return Date::ToString(date) + " " + Time::ToString(time);
}

date_t Timestamp::GetDate(timestamp_t timestamp) {
	if (timestamp == timestamp_t::infinity()) {
		return date_t::infinity();
	} else if (timestamp == timestamp_t::ninfinity()) {
		return date_t::ninfinity();
	}
	return date_t((timestamp.value + (timestamp.value < 0)) / Interval::MICROS_PER_DAY - (timestamp.value < 0));
}

dtime_t Timestamp::GetTime(timestamp_t timestamp) {
	if (!IsFinite(timestamp)) {
		throw ConversionException("Can't get TIME of infinite TIMESTAMP");
	}
	date_t date = Timestamp::GetDate(timestamp);
	return dtime_t(timestamp.value - (int64_t(date.days) * int64_t(Interval::MICROS_PER_DAY)));
}

bool Timestamp::TryFromDatetime(date_t date, dtime_t time, timestamp_t &result) {
	if (!TryMultiplyOperator::Operation<int64_t, int64_t, int64_t>(date.days, Interval::MICROS_PER_DAY, result.value)) {
		return false;
	}
	if (!TryAddOperator::Operation<int64_t, int64_t, int64_t>(result.value, time.micros, result.value)) {
		return false;
	}
	return Timestamp::IsFinite(result);
}

timestamp_t Timestamp::FromDatetime(date_t date, dtime_t time) {
	timestamp_t result;
	if (!TryFromDatetime(date, time, result)) {
		throw Exception("Overflow exception in date/time -> timestamp conversion");
	}
	return result;
}

void Timestamp::Convert(timestamp_t timestamp, date_t &out_date, dtime_t &out_time) {
	out_date = GetDate(timestamp);
	int64_t days_micros;
	if (!TryMultiplyOperator::Operation<int64_t, int64_t, int64_t>(out_date.days, Interval::MICROS_PER_DAY,
	                                                               days_micros)) {
		throw ConversionException("Date out of range in timestamp conversion");
	}
	out_time = dtime_t(timestamp.value - days_micros);
	D_ASSERT(timestamp == Timestamp::FromDatetime(out_date, out_time));
}

timestamp_t Timestamp::GetCurrentTimestamp() {
	auto now = system_clock::now();
	auto epoch_ms = duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
	return Timestamp::FromEpochMs(epoch_ms);
}

timestamp_t Timestamp::FromEpochSeconds(int64_t sec) {
	int64_t result;
	if (!TryMultiplyOperator::Operation(sec, Interval::MICROS_PER_SEC, result)) {
		throw ConversionException("Could not convert Timestamp(S) to Timestamp(US)");
	}
	return timestamp_t(result);
}

timestamp_t Timestamp::FromEpochMs(int64_t ms) {
	int64_t result;
	if (!TryMultiplyOperator::Operation(ms, Interval::MICROS_PER_MSEC, result)) {
		throw ConversionException("Could not convert Timestamp(MS) to Timestamp(US)");
	}
	return timestamp_t(result);
}

timestamp_t Timestamp::FromEpochMicroSeconds(int64_t micros) {
	return timestamp_t(micros);
}

timestamp_t Timestamp::FromEpochNanoSeconds(int64_t ns) {
	return timestamp_t(ns / 1000);
}

int64_t Timestamp::GetEpochSeconds(timestamp_t timestamp) {
	return timestamp.value / Interval::MICROS_PER_SEC;
}

int64_t Timestamp::GetEpochMs(timestamp_t timestamp) {
	return timestamp.value / Interval::MICROS_PER_MSEC;
}

int64_t Timestamp::GetEpochMicroSeconds(timestamp_t timestamp) {
	return timestamp.value;
}

int64_t Timestamp::GetEpochNanoSeconds(timestamp_t timestamp) {
	int64_t result;
	int64_t ns_in_us = 1000;
	if (!TryMultiplyOperator::Operation(timestamp.value, ns_in_us, result)) {
		throw ConversionException("Could not convert Timestamp(US) to Timestamp(NS)");
	}
	return result;
}

} // namespace duckdb



namespace duckdb {

bool UUID::FromString(string str, hugeint_t &result) {
	auto hex2char = [](char ch) -> unsigned char {
		if (ch >= '0' && ch <= '9') {
			return ch - '0';
		}
		if (ch >= 'a' && ch <= 'f') {
			return 10 + ch - 'a';
		}
		if (ch >= 'A' && ch <= 'F') {
			return 10 + ch - 'A';
		}
		return 0;
	};
	auto is_hex = [](char ch) -> bool {
		return (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F');
	};

	if (str.empty()) {
		return false;
	}
	int has_braces = 0;
	if (str.front() == '{') {
		has_braces = 1;
	}
	if (has_braces && str.back() != '}') {
		return false;
	}

	result.lower = 0;
	result.upper = 0;
	size_t count = 0;
	for (size_t i = has_braces; i < str.size() - has_braces; ++i) {
		if (str[i] == '-') {
			continue;
		}
		if (count >= 32 || !is_hex(str[i])) {
			return false;
		}
		if (count >= 16) {
			result.lower = (result.lower << 4) | hex2char(str[i]);
		} else {
			result.upper = (result.upper << 4) | hex2char(str[i]);
		}
		count++;
	}
	// Flip the first bit to make `order by uuid` same as `order by uuid::varchar`
	result.upper ^= (int64_t(1) << 63);
	return count == 32;
}

void UUID::ToString(hugeint_t input, char *buf) {
	auto byte_to_hex = [](char byte_val, char *buf, idx_t &pos) {
		static char const HEX_DIGITS[] = "0123456789abcdef";
		buf[pos++] = HEX_DIGITS[(byte_val >> 4) & 0xf];
		buf[pos++] = HEX_DIGITS[byte_val & 0xf];
	};

	// Flip back before convert to string
	int64_t upper = input.upper ^ (int64_t(1) << 63);
	idx_t pos = 0;
	byte_to_hex(upper >> 56 & 0xFF, buf, pos);
	byte_to_hex(upper >> 48 & 0xFF, buf, pos);
	byte_to_hex(upper >> 40 & 0xFF, buf, pos);
	byte_to_hex(upper >> 32 & 0xFF, buf, pos);
	buf[pos++] = '-';
	byte_to_hex(upper >> 24 & 0xFF, buf, pos);
	byte_to_hex(upper >> 16 & 0xFF, buf, pos);
	buf[pos++] = '-';
	byte_to_hex(upper >> 8 & 0xFF, buf, pos);
	byte_to_hex(upper & 0xFF, buf, pos);
	buf[pos++] = '-';
	byte_to_hex(input.lower >> 56 & 0xFF, buf, pos);
	byte_to_hex(input.lower >> 48 & 0xFF, buf, pos);
	buf[pos++] = '-';
	byte_to_hex(input.lower >> 40 & 0xFF, buf, pos);
	byte_to_hex(input.lower >> 32 & 0xFF, buf, pos);
	byte_to_hex(input.lower >> 24 & 0xFF, buf, pos);
	byte_to_hex(input.lower >> 16 & 0xFF, buf, pos);
	byte_to_hex(input.lower >> 8 & 0xFF, buf, pos);
	byte_to_hex(input.lower & 0xFF, buf, pos);
}

hugeint_t UUID::GenerateRandomUUID(RandomEngine &engine) {
	uint8_t bytes[16];
	for (int i = 0; i < 16; i += 4) {
		*reinterpret_cast<uint32_t *>(bytes + i) = engine.NextRandomInteger();
	}
	// variant must be 10xxxxxx
	bytes[8] &= 0xBF;
	bytes[8] |= 0x80;
	// version must be 0100xxxx
	bytes[6] &= 0x4F;
	bytes[6] |= 0x40;

	hugeint_t result;
	result.upper = 0;
	result.upper |= ((int64_t)bytes[0] << 56);
	result.upper |= ((int64_t)bytes[1] << 48);
	result.upper |= ((int64_t)bytes[3] << 40);
	result.upper |= ((int64_t)bytes[4] << 32);
	result.upper |= ((int64_t)bytes[5] << 24);
	result.upper |= ((int64_t)bytes[6] << 16);
	result.upper |= ((int64_t)bytes[7] << 8);
	result.upper |= bytes[8];
	result.lower = 0;
	result.lower |= ((uint64_t)bytes[8] << 56);
	result.lower |= ((uint64_t)bytes[9] << 48);
	result.lower |= ((uint64_t)bytes[10] << 40);
	result.lower |= ((uint64_t)bytes[11] << 32);
	result.lower |= ((uint64_t)bytes[12] << 24);
	result.lower |= ((uint64_t)bytes[13] << 16);
	result.lower |= ((uint64_t)bytes[14] << 8);
	result.lower |= bytes[15];
	return result;
}

hugeint_t UUID::GenerateRandomUUID() {
	RandomEngine engine;
	return GenerateRandomUUID(engine);
}

} // namespace duckdb


namespace duckdb {

ValidityData::ValidityData(idx_t count) : TemplatedValidityData(count) {
}
ValidityData::ValidityData(const ValidityMask &original, idx_t count)
    : TemplatedValidityData(original.GetData(), count) {
}

void ValidityMask::Combine(const ValidityMask &other, idx_t count) {
	if (other.AllValid()) {
		// X & 1 = X
		return;
	}
	if (AllValid()) {
		// 1 & Y = Y
		Initialize(other);
		return;
	}
	if (validity_mask == other.validity_mask) {
		// X & X == X
		return;
	}
	// have to merge
	// create a new validity mask that contains the combined mask
	auto owned_data = move(validity_data);
	auto data = GetData();
	auto other_data = other.GetData();

	Initialize(count);
	auto result_data = GetData();

	auto entry_count = ValidityData::EntryCount(count);
	for (idx_t entry_idx = 0; entry_idx < entry_count; entry_idx++) {
		result_data[entry_idx] = data[entry_idx] & other_data[entry_idx];
	}
}

// LCOV_EXCL_START
string ValidityMask::ToString(idx_t count) const {
	string result = "Validity Mask (" + to_string(count) + ") [";
	for (idx_t i = 0; i < count; i++) {
		result += RowIsValid(i) ? "." : "X";
	}
	result += "]";
	return result;
}
// LCOV_EXCL_STOP

void ValidityMask::Resize(idx_t old_size, idx_t new_size) {
	if (validity_mask) {
		auto new_size_count = EntryCount(new_size);
		auto old_size_count = EntryCount(old_size);
		auto new_owned_data = unique_ptr<validity_t[]>(new validity_t[new_size_count]);
		for (idx_t entry_idx = 0; entry_idx < old_size_count; entry_idx++) {
			new_owned_data[entry_idx] = validity_mask[entry_idx];
		}
		for (idx_t entry_idx = old_size_count; entry_idx < new_size_count; entry_idx++) {
			new_owned_data[entry_idx] = ValidityData::MAX_ENTRY;
		}
		validity_data->owned_data = move(new_owned_data);
		validity_mask = validity_data->owned_data.get();
	} else {
		Initialize(new_size);
	}
}

void ValidityMask::Slice(const ValidityMask &other, idx_t offset) {
	if (other.AllValid()) {
		validity_mask = nullptr;
		validity_data.reset();
		return;
	}
	if (offset == 0) {
		Initialize(other);
		return;
	}
	ValidityMask new_mask(STANDARD_VECTOR_SIZE);

// FIXME THIS NEEDS FIXING!
#if 1
	for (idx_t i = offset; i < STANDARD_VECTOR_SIZE; i++) {
		new_mask.Set(i - offset, other.RowIsValid(i));
	}
	Initialize(new_mask);
#else
	// first shift the "whole" units
	idx_t entire_units = offset / BITS_PER_VALUE;
	idx_t sub_units = offset - entire_units * BITS_PER_VALUE;
	if (entire_units > 0) {
		idx_t validity_idx;
		for (validity_idx = 0; validity_idx + entire_units < STANDARD_ENTRY_COUNT; validity_idx++) {
			new_mask.validity_mask[validity_idx] = other.validity_mask[validity_idx + entire_units];
		}
	}
	// now we shift the remaining sub units
	// this gets a bit more complicated because we have to shift over the borders of the entries
	// e.g. suppose we have 2 entries of length 4 and we left-shift by two
	// 0101|1010
	// a regular left-shift of both gets us:
	// 0100|1000
	// we then OR the overflow (right-shifted by BITS_PER_VALUE - offset) together to get the correct result
	// 0100|1000 ->
	// 0110|1000
	if (sub_units > 0) {
		idx_t validity_idx;
		for (validity_idx = 0; validity_idx + 1 < STANDARD_ENTRY_COUNT; validity_idx++) {
			new_mask.validity_mask[validity_idx] =
			    (other.validity_mask[validity_idx] >> sub_units) |
			    (other.validity_mask[validity_idx + 1] << (BITS_PER_VALUE - sub_units));
		}
		new_mask.validity_mask[validity_idx] >>= sub_units;
	}
#ifdef DEBUG
	for (idx_t i = offset; i < STANDARD_VECTOR_SIZE; i++) {
		D_ASSERT(new_mask.RowIsValid(i - offset) == other.RowIsValid(i));
	}
	Initialize(new_mask);
#endif
#endif
}

} // namespace duckdb





























#include <utility>
#include <cmath>

namespace duckdb {

Value::Value(LogicalType type) : type_(move(type)), is_null(true) {
}

Value::Value(int32_t val) : type_(LogicalType::INTEGER), is_null(false) {
	value_.integer = val;
}

Value::Value(int64_t val) : type_(LogicalType::BIGINT), is_null(false) {
	value_.bigint = val;
}

Value::Value(float val) : type_(LogicalType::FLOAT), is_null(false) {
	value_.float_ = val;
}

Value::Value(double val) : type_(LogicalType::DOUBLE), is_null(false) {
	value_.double_ = val;
}

Value::Value(const char *val) : Value(val ? string(val) : string()) {
}

Value::Value(std::nullptr_t val) : Value(LogicalType::VARCHAR) {
}

Value::Value(string_t val) : Value(string(val.GetDataUnsafe(), val.GetSize())) {
}

Value::Value(string val) : type_(LogicalType::VARCHAR), is_null(false), str_value(move(val)) {
	if (!Value::StringIsValid(str_value.c_str(), str_value.size())) {
		throw Exception("String value is not valid UTF8");
	}
}

Value::~Value() {
}

Value::Value(const Value &other)
    : type_(other.type_), is_null(other.is_null), value_(other.value_), str_value(other.str_value),
      struct_value(other.struct_value), list_value(other.list_value) {
}

Value::Value(Value &&other) noexcept
    : type_(move(other.type_)), is_null(other.is_null), value_(other.value_), str_value(move(other.str_value)),
      struct_value(move(other.struct_value)), list_value(move(other.list_value)) {
}

Value &Value::operator=(const Value &other) {
	type_ = other.type_;
	is_null = other.is_null;
	value_ = other.value_;
	str_value = other.str_value;
	struct_value = other.struct_value;
	list_value = other.list_value;
	return *this;
}

Value &Value::operator=(Value &&other) noexcept {
	type_ = move(other.type_);
	is_null = other.is_null;
	value_ = other.value_;
	str_value = move(other.str_value);
	struct_value = move(other.struct_value);
	list_value = move(other.list_value);
	return *this;
}

Value Value::MinimumValue(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return Value::BOOLEAN(false);
	case LogicalTypeId::TINYINT:
		return Value::TINYINT(NumericLimits<int8_t>::Minimum());
	case LogicalTypeId::SMALLINT:
		return Value::SMALLINT(NumericLimits<int16_t>::Minimum());
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::SQLNULL:
		return Value::INTEGER(NumericLimits<int32_t>::Minimum());
	case LogicalTypeId::BIGINT:
		return Value::BIGINT(NumericLimits<int64_t>::Minimum());
	case LogicalTypeId::HUGEINT:
		return Value::HUGEINT(NumericLimits<hugeint_t>::Minimum());
	case LogicalTypeId::UUID:
		return Value::UUID(NumericLimits<hugeint_t>::Minimum());
	case LogicalTypeId::UTINYINT:
		return Value::UTINYINT(NumericLimits<uint8_t>::Minimum());
	case LogicalTypeId::USMALLINT:
		return Value::USMALLINT(NumericLimits<uint16_t>::Minimum());
	case LogicalTypeId::UINTEGER:
		return Value::UINTEGER(NumericLimits<uint32_t>::Minimum());
	case LogicalTypeId::UBIGINT:
		return Value::UBIGINT(NumericLimits<uint64_t>::Minimum());
	case LogicalTypeId::DATE:
		return Value::DATE(Date::FromDate(Date::DATE_MIN_YEAR, Date::DATE_MIN_MONTH, Date::DATE_MIN_DAY));
	case LogicalTypeId::TIME:
		return Value::TIME(dtime_t(0));
	case LogicalTypeId::TIMESTAMP:
		return Value::TIMESTAMP(Date::FromDate(Timestamp::MIN_YEAR, Timestamp::MIN_MONTH, Timestamp::MIN_DAY),
		                        dtime_t(0));
	case LogicalTypeId::TIMESTAMP_SEC:
		return MinimumValue(LogicalType::TIMESTAMP).CastAs(LogicalType::TIMESTAMP_S);
	case LogicalTypeId::TIMESTAMP_MS:
		return MinimumValue(LogicalType::TIMESTAMP).CastAs(LogicalType::TIMESTAMP_MS);
	case LogicalTypeId::TIMESTAMP_NS:
		return Value::TIMESTAMPNS(timestamp_t(NumericLimits<int64_t>::Minimum()));
	case LogicalTypeId::TIME_TZ:
		return Value::TIMETZ(dtime_t(0));
	case LogicalTypeId::TIMESTAMP_TZ:
		return Value::TIMESTAMPTZ(Timestamp::FromDatetime(
		    Date::FromDate(Timestamp::MIN_YEAR, Timestamp::MIN_MONTH, Timestamp::MIN_DAY), dtime_t(0)));
	case LogicalTypeId::FLOAT:
		return Value::FLOAT(NumericLimits<float>::Minimum());
	case LogicalTypeId::DOUBLE:
		return Value::DOUBLE(NumericLimits<double>::Minimum());
	case LogicalTypeId::DECIMAL: {
		auto width = DecimalType::GetWidth(type);
		auto scale = DecimalType::GetScale(type);
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			return Value::DECIMAL(int16_t(-NumericHelper::POWERS_OF_TEN[width] + 1), width, scale);
		case PhysicalType::INT32:
			return Value::DECIMAL(int32_t(-NumericHelper::POWERS_OF_TEN[width] + 1), width, scale);
		case PhysicalType::INT64:
			return Value::DECIMAL(int64_t(-NumericHelper::POWERS_OF_TEN[width] + 1), width, scale);
		case PhysicalType::INT128:
			return Value::DECIMAL(-Hugeint::POWERS_OF_TEN[width] + 1, width, scale);
		default:
			throw InternalException("Unknown decimal type");
		}
	}
	case LogicalTypeId::ENUM:
		return Value::ENUM(0, type);
	default:
		throw InvalidTypeException(type, "MinimumValue requires numeric type");
	}
}

Value Value::MaximumValue(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return Value::BOOLEAN(true);
	case LogicalTypeId::TINYINT:
		return Value::TINYINT(NumericLimits<int8_t>::Maximum());
	case LogicalTypeId::SMALLINT:
		return Value::SMALLINT(NumericLimits<int16_t>::Maximum());
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::SQLNULL:
		return Value::INTEGER(NumericLimits<int32_t>::Maximum());
	case LogicalTypeId::BIGINT:
		return Value::BIGINT(NumericLimits<int64_t>::Maximum());
	case LogicalTypeId::HUGEINT:
		return Value::HUGEINT(NumericLimits<hugeint_t>::Maximum());
	case LogicalTypeId::UUID:
		return Value::UUID(NumericLimits<hugeint_t>::Maximum());
	case LogicalTypeId::UTINYINT:
		return Value::UTINYINT(NumericLimits<uint8_t>::Maximum());
	case LogicalTypeId::USMALLINT:
		return Value::USMALLINT(NumericLimits<uint16_t>::Maximum());
	case LogicalTypeId::UINTEGER:
		return Value::UINTEGER(NumericLimits<uint32_t>::Maximum());
	case LogicalTypeId::UBIGINT:
		return Value::UBIGINT(NumericLimits<uint64_t>::Maximum());
	case LogicalTypeId::DATE:
		return Value::DATE(Date::FromDate(Date::DATE_MAX_YEAR, Date::DATE_MAX_MONTH, Date::DATE_MAX_DAY));
	case LogicalTypeId::TIME:
		return Value::TIME(dtime_t(Interval::SECS_PER_DAY * Interval::MICROS_PER_SEC - 1));
	case LogicalTypeId::TIMESTAMP:
		return Value::TIMESTAMP(timestamp_t(NumericLimits<int64_t>::Maximum() - 1));
	case LogicalTypeId::TIMESTAMP_MS:
		return MaximumValue(LogicalType::TIMESTAMP).CastAs(LogicalType::TIMESTAMP_MS);
	case LogicalTypeId::TIMESTAMP_NS:
		return Value::TIMESTAMPNS(timestamp_t(NumericLimits<int64_t>::Maximum() - 1));
	case LogicalTypeId::TIMESTAMP_SEC:
		return MaximumValue(LogicalType::TIMESTAMP).CastAs(LogicalType::TIMESTAMP_S);
	case LogicalTypeId::TIME_TZ:
		return Value::TIMETZ(dtime_t(Interval::SECS_PER_DAY * Interval::MICROS_PER_SEC - 1));
	case LogicalTypeId::TIMESTAMP_TZ:
		return MaximumValue(LogicalType::TIMESTAMP);
	case LogicalTypeId::FLOAT:
		return Value::FLOAT(NumericLimits<float>::Maximum());
	case LogicalTypeId::DOUBLE:
		return Value::DOUBLE(NumericLimits<double>::Maximum());
	case LogicalTypeId::DECIMAL: {
		auto width = DecimalType::GetWidth(type);
		auto scale = DecimalType::GetScale(type);
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			return Value::DECIMAL(int16_t(NumericHelper::POWERS_OF_TEN[width] - 1), width, scale);
		case PhysicalType::INT32:
			return Value::DECIMAL(int32_t(NumericHelper::POWERS_OF_TEN[width] - 1), width, scale);
		case PhysicalType::INT64:
			return Value::DECIMAL(int64_t(NumericHelper::POWERS_OF_TEN[width] - 1), width, scale);
		case PhysicalType::INT128:
			return Value::DECIMAL(Hugeint::POWERS_OF_TEN[width] - 1, width, scale);
		default:
			throw InternalException("Unknown decimal type");
		}
	}
	case LogicalTypeId::ENUM:
		return Value::ENUM(EnumType::GetSize(type) - 1, type);
	default:
		throw InvalidTypeException(type, "MaximumValue requires numeric type");
	}
}

Value Value::BOOLEAN(int8_t value) {
	Value result(LogicalType::BOOLEAN);
	result.value_.boolean = value ? true : false;
	result.is_null = false;
	return result;
}

Value Value::TINYINT(int8_t value) {
	Value result(LogicalType::TINYINT);
	result.value_.tinyint = value;
	result.is_null = false;
	return result;
}

Value Value::SMALLINT(int16_t value) {
	Value result(LogicalType::SMALLINT);
	result.value_.smallint = value;
	result.is_null = false;
	return result;
}

Value Value::INTEGER(int32_t value) {
	Value result(LogicalType::INTEGER);
	result.value_.integer = value;
	result.is_null = false;
	return result;
}

Value Value::BIGINT(int64_t value) {
	Value result(LogicalType::BIGINT);
	result.value_.bigint = value;
	result.is_null = false;
	return result;
}

Value Value::HUGEINT(hugeint_t value) {
	Value result(LogicalType::HUGEINT);
	result.value_.hugeint = value;
	result.is_null = false;
	return result;
}

Value Value::UUID(hugeint_t value) {
	Value result(LogicalType::UUID);
	result.value_.hugeint = value;
	result.is_null = false;
	return result;
}

Value Value::UUID(const string &value) {
	Value result(LogicalType::UUID);
	result.value_.hugeint = UUID::FromString(value);
	result.is_null = false;
	return result;
}

Value Value::UTINYINT(uint8_t value) {
	Value result(LogicalType::UTINYINT);
	result.value_.utinyint = value;
	result.is_null = false;
	return result;
}

Value Value::USMALLINT(uint16_t value) {
	Value result(LogicalType::USMALLINT);
	result.value_.usmallint = value;
	result.is_null = false;
	return result;
}

Value Value::UINTEGER(uint32_t value) {
	Value result(LogicalType::UINTEGER);
	result.value_.uinteger = value;
	result.is_null = false;
	return result;
}

Value Value::UBIGINT(uint64_t value) {
	Value result(LogicalType::UBIGINT);
	result.value_.ubigint = value;
	result.is_null = false;
	return result;
}

bool Value::FloatIsFinite(float value) {
	return !(std::isnan(value) || std::isinf(value));
}

bool Value::DoubleIsFinite(double value) {
	return !(std::isnan(value) || std::isinf(value));
}

template <>
bool Value::IsNan(float input) {
	return std::isnan(input);
}

template <>
bool Value::IsNan(double input) {
	return std::isnan(input);
}

template <>
bool Value::IsFinite(float input) {
	return Value::FloatIsFinite(input);
}

template <>
bool Value::IsFinite(double input) {
	return Value::DoubleIsFinite(input);
}

template <>
bool Value::IsFinite(date_t input) {
	return Date::IsFinite(input);
}

template <>
bool Value::IsFinite(timestamp_t input) {
	return Timestamp::IsFinite(input);
}

bool Value::StringIsValid(const char *str, idx_t length) {
	auto utf_type = Utf8Proc::Analyze(str, length);
	return utf_type != UnicodeType::INVALID;
}

Value Value::DECIMAL(int16_t value, uint8_t width, uint8_t scale) {
	D_ASSERT(width <= Decimal::MAX_WIDTH_INT16);
	Value result(LogicalType::DECIMAL(width, scale));
	result.value_.smallint = value;
	result.is_null = false;
	return result;
}

Value Value::DECIMAL(int32_t value, uint8_t width, uint8_t scale) {
	D_ASSERT(width >= Decimal::MAX_WIDTH_INT16 && width <= Decimal::MAX_WIDTH_INT32);
	Value result(LogicalType::DECIMAL(width, scale));
	result.value_.integer = value;
	result.is_null = false;
	return result;
}

Value Value::DECIMAL(int64_t value, uint8_t width, uint8_t scale) {
	auto decimal_type = LogicalType::DECIMAL(width, scale);
	Value result(decimal_type);
	switch (decimal_type.InternalType()) {
	case PhysicalType::INT16:
		result.value_.smallint = value;
		break;
	case PhysicalType::INT32:
		result.value_.integer = value;
		break;
	case PhysicalType::INT64:
		result.value_.bigint = value;
		break;
	default:
		result.value_.hugeint = value;
		break;
	}
	result.type_.Verify();
	result.is_null = false;
	return result;
}

Value Value::DECIMAL(hugeint_t value, uint8_t width, uint8_t scale) {
	D_ASSERT(width >= Decimal::MAX_WIDTH_INT64 && width <= Decimal::MAX_WIDTH_INT128);
	Value result(LogicalType::DECIMAL(width, scale));
	result.value_.hugeint = value;
	result.is_null = false;
	return result;
}

Value Value::FLOAT(float value) {
	Value result(LogicalType::FLOAT);
	result.value_.float_ = value;
	result.is_null = false;
	return result;
}

Value Value::DOUBLE(double value) {
	Value result(LogicalType::DOUBLE);
	result.value_.double_ = value;
	result.is_null = false;
	return result;
}

Value Value::HASH(hash_t value) {
	Value result(LogicalType::HASH);
	result.value_.hash = value;
	result.is_null = false;
	return result;
}

Value Value::POINTER(uintptr_t value) {
	Value result(LogicalType::POINTER);
	result.value_.pointer = value;
	result.is_null = false;
	return result;
}

Value Value::DATE(date_t value) {
	Value result(LogicalType::DATE);
	result.value_.date = value;
	result.is_null = false;
	return result;
}

Value Value::DATE(int32_t year, int32_t month, int32_t day) {
	return Value::DATE(Date::FromDate(year, month, day));
}

Value Value::TIME(dtime_t value) {
	Value result(LogicalType::TIME);
	result.value_.time = value;
	result.is_null = false;
	return result;
}

Value Value::TIMETZ(dtime_t value) {
	Value result(LogicalType::TIME_TZ);
	result.value_.time = value;
	result.is_null = false;
	return result;
}

Value Value::TIME(int32_t hour, int32_t min, int32_t sec, int32_t micros) {
	return Value::TIME(Time::FromTime(hour, min, sec, micros));
}

Value Value::TIMESTAMP(timestamp_t value) {
	Value result(LogicalType::TIMESTAMP);
	result.value_.timestamp = value;
	result.is_null = false;
	return result;
}

Value Value::TIMESTAMPTZ(timestamp_t value) {
	Value result(LogicalType::TIMESTAMP_TZ);
	result.value_.timestamp = value;
	result.is_null = false;
	return result;
}

Value Value::TIMESTAMPNS(timestamp_t timestamp) {
	Value result(LogicalType::TIMESTAMP_NS);
	result.value_.timestamp = timestamp;
	result.is_null = false;
	return result;
}

Value Value::TIMESTAMPMS(timestamp_t timestamp) {
	Value result(LogicalType::TIMESTAMP_MS);
	result.value_.timestamp = timestamp;
	result.is_null = false;
	return result;
}

Value Value::TIMESTAMPSEC(timestamp_t timestamp) {
	Value result(LogicalType::TIMESTAMP_S);
	result.value_.timestamp = timestamp;
	result.is_null = false;
	return result;
}

Value Value::TIMESTAMP(date_t date, dtime_t time) {
	return Value::TIMESTAMP(Timestamp::FromDatetime(date, time));
}

Value Value::TIMESTAMP(int32_t year, int32_t month, int32_t day, int32_t hour, int32_t min, int32_t sec,
                       int32_t micros) {
	auto val = Value::TIMESTAMP(Date::FromDate(year, month, day), Time::FromTime(hour, min, sec, micros));
	val.type_ = LogicalType::TIMESTAMP;
	return val;
}

Value Value::STRUCT(child_list_t<Value> values) {
	Value result;
	child_list_t<LogicalType> child_types;
	for (auto &child : values) {
		child_types.push_back(make_pair(move(child.first), child.second.type()));
		result.struct_value.push_back(move(child.second));
	}
	result.type_ = LogicalType::STRUCT(move(child_types));

	result.is_null = false;
	return result;
}

Value Value::MAP(Value key, Value value) {
	Value result;
	child_list_t<LogicalType> child_types;
	child_types.push_back({"key", key.type()});
	child_types.push_back({"value", value.type()});

	result.type_ = LogicalType::MAP(move(child_types));

	result.struct_value.push_back(move(key));
	result.struct_value.push_back(move(value));
	result.is_null = false;
	return result;
}

Value Value::LIST(vector<Value> values) {
	if (values.empty()) {
		throw InternalException("Value::LIST requires a non-empty list of values. Use Value::EMPTYLIST instead.");
	}
#ifdef DEBUG
	for (idx_t i = 1; i < values.size(); i++) {
		D_ASSERT(values[i].type() == values[0].type());
	}
#endif
	Value result;
	result.type_ = LogicalType::LIST(values[0].type());
	result.list_value = move(values);
	result.is_null = false;
	return result;
}

Value Value::LIST(LogicalType child_type, vector<Value> values) {
	if (values.empty()) {
		return Value::EMPTYLIST(move(child_type));
	}
	for (auto &val : values) {
		val = val.CastAs(child_type);
	}
	return Value::LIST(move(values));
}

Value Value::EMPTYLIST(LogicalType child_type) {
	Value result;
	result.type_ = LogicalType::LIST(move(child_type));
	result.is_null = false;
	return result;
}

Value Value::BLOB(const_data_ptr_t data, idx_t len) {
	Value result(LogicalType::BLOB);
	result.is_null = false;
	result.str_value = string((const char *)data, len);
	return result;
}

Value Value::BLOB(const string &data) {
	Value result(LogicalType::BLOB);
	result.is_null = false;
	result.str_value = Blob::ToBlob(string_t(data));
	return result;
}

Value Value::JSON(const char *val) {
	auto result = Value(val);
	result.type_ = LogicalTypeId::JSON;
	return result;
}

Value Value::JSON(string_t val) {
	auto result = Value(val);
	result.type_ = LogicalTypeId::JSON;
	return result;
}

Value Value::JSON(string val) {
	auto result = Value(move(val));
	result.type_ = LogicalTypeId::JSON;
	return result;
}

Value Value::ENUM(uint64_t value, const LogicalType &original_type) {
	D_ASSERT(original_type.id() == LogicalTypeId::ENUM);
	Value result(original_type);
	switch (original_type.InternalType()) {
	case PhysicalType::UINT8:
		result.value_.utinyint = value;
		break;
	case PhysicalType::UINT16:
		result.value_.usmallint = value;
		break;
	case PhysicalType::UINT32:
		result.value_.uinteger = value;
		break;
	case PhysicalType::UINT64: //  DEDUP_POINTER_ENUM
		result.value_.ubigint = value;
		break;
	default:
		throw InternalException("Incorrect Physical Type for ENUM");
	}
	result.is_null = false;
	return result;
}

Value Value::INTERVAL(int32_t months, int32_t days, int64_t micros) {
	Value result(LogicalType::INTERVAL);
	result.is_null = false;
	result.value_.interval.months = months;
	result.value_.interval.days = days;
	result.value_.interval.micros = micros;
	return result;
}

Value Value::INTERVAL(interval_t interval) {
	return Value::INTERVAL(interval.months, interval.days, interval.micros);
}

//===--------------------------------------------------------------------===//
// CreateValue
//===--------------------------------------------------------------------===//
template <>
Value Value::CreateValue(bool value) {
	return Value::BOOLEAN(value);
}

template <>
Value Value::CreateValue(int8_t value) {
	return Value::TINYINT(value);
}

template <>
Value Value::CreateValue(int16_t value) {
	return Value::SMALLINT(value);
}

template <>
Value Value::CreateValue(int32_t value) {
	return Value::INTEGER(value);
}

template <>
Value Value::CreateValue(int64_t value) {
	return Value::BIGINT(value);
}

template <>
Value Value::CreateValue(uint8_t value) {
	return Value::UTINYINT(value);
}

template <>
Value Value::CreateValue(uint16_t value) {
	return Value::USMALLINT(value);
}

template <>
Value Value::CreateValue(uint32_t value) {
	return Value::UINTEGER(value);
}

template <>
Value Value::CreateValue(uint64_t value) {
	return Value::UBIGINT(value);
}

template <>
Value Value::CreateValue(hugeint_t value) {
	return Value::HUGEINT(value);
}

template <>
Value Value::CreateValue(date_t value) {
	return Value::DATE(value);
}

template <>
Value Value::CreateValue(dtime_t value) {
	return Value::TIME(value);
}

template <>
Value Value::CreateValue(timestamp_t value) {
	return Value::TIMESTAMP(value);
}

template <>
Value Value::CreateValue(const char *value) {
	return Value(string(value));
}

template <>
Value Value::CreateValue(string value) { // NOLINT: required for templating
	return Value::BLOB(value);
}

template <>
Value Value::CreateValue(string_t value) {
	return Value(value);
}

template <>
Value Value::CreateValue(float value) {
	return Value::FLOAT(value);
}

template <>
Value Value::CreateValue(double value) {
	return Value::DOUBLE(value);
}

template <>
Value Value::CreateValue(interval_t value) {
	return Value::INTERVAL(value);
}

template <>
Value Value::CreateValue(Value value) {
	return value;
}

//===--------------------------------------------------------------------===//
// GetValue
//===--------------------------------------------------------------------===//
template <class T>
T Value::GetValueInternal() const {
	if (IsNull()) {
		return NullValue<T>();
	}
	switch (type_.id()) {
	case LogicalTypeId::BOOLEAN:
		return Cast::Operation<bool, T>(value_.boolean);
	case LogicalTypeId::TINYINT:
		return Cast::Operation<int8_t, T>(value_.tinyint);
	case LogicalTypeId::SMALLINT:
		return Cast::Operation<int16_t, T>(value_.smallint);
	case LogicalTypeId::INTEGER:
		return Cast::Operation<int32_t, T>(value_.integer);
	case LogicalTypeId::BIGINT:
		return Cast::Operation<int64_t, T>(value_.bigint);
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UUID:
		return Cast::Operation<hugeint_t, T>(value_.hugeint);
	case LogicalTypeId::DATE:
		return Cast::Operation<date_t, T>(value_.date);
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
		return Cast::Operation<dtime_t, T>(value_.time);
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		return Cast::Operation<timestamp_t, T>(value_.timestamp);
	case LogicalTypeId::UTINYINT:
		return Cast::Operation<uint8_t, T>(value_.utinyint);
	case LogicalTypeId::USMALLINT:
		return Cast::Operation<uint16_t, T>(value_.usmallint);
	case LogicalTypeId::UINTEGER:
		return Cast::Operation<uint32_t, T>(value_.uinteger);
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::UBIGINT:
		return Cast::Operation<uint64_t, T>(value_.ubigint);
	case LogicalTypeId::FLOAT:
		return Cast::Operation<float, T>(value_.float_);
	case LogicalTypeId::DOUBLE:
		return Cast::Operation<double, T>(value_.double_);
	case LogicalTypeId::VARCHAR:
		return Cast::Operation<string_t, T>(str_value.c_str());
	case LogicalTypeId::INTERVAL:
		return Cast::Operation<interval_t, T>(value_.interval);
	case LogicalTypeId::DECIMAL:
		return CastAs(LogicalType::DOUBLE).GetValueInternal<T>();
	case LogicalTypeId::ENUM: {
		switch (type_.InternalType()) {
		case PhysicalType::UINT8:
			return Cast::Operation<uint8_t, T>(value_.utinyint);
		case PhysicalType::UINT16:
			return Cast::Operation<uint16_t, T>(value_.usmallint);
		case PhysicalType::UINT32:
			return Cast::Operation<uint32_t, T>(value_.uinteger);
		default:
			throw InternalException("Invalid Internal Type for ENUMs");
		}
	}

	default:
		throw NotImplementedException("Unimplemented type \"%s\" for GetValue()", type_.ToString());
	}
}

template <>
bool Value::GetValue() const {
	return GetValueInternal<int8_t>();
}
template <>
int8_t Value::GetValue() const {
	return GetValueInternal<int8_t>();
}
template <>
int16_t Value::GetValue() const {
	return GetValueInternal<int16_t>();
}
template <>
int32_t Value::GetValue() const {
	if (type_.id() == LogicalTypeId::DATE) {
		return value_.integer;
	}
	return GetValueInternal<int32_t>();
}
template <>
int64_t Value::GetValue() const {
	switch (type_.id()) {
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::TIMESTAMP_TZ:
		return value_.bigint;
	default:
		return GetValueInternal<int64_t>();
	}
}
template <>
hugeint_t Value::GetValue() const {
	return GetValueInternal<hugeint_t>();
}
template <>
uint8_t Value::GetValue() const {
	return GetValueInternal<uint8_t>();
}
template <>
uint16_t Value::GetValue() const {
	return GetValueInternal<uint16_t>();
}
template <>
uint32_t Value::GetValue() const {
	return GetValueInternal<uint32_t>();
}
template <>
uint64_t Value::GetValue() const {
	return GetValueInternal<uint64_t>();
}
template <>
string Value::GetValue() const {
	return ToString();
}
template <>
float Value::GetValue() const {
	return GetValueInternal<float>();
}
template <>
double Value::GetValue() const {
	return GetValueInternal<double>();
}
template <>
date_t Value::GetValue() const {
	return GetValueInternal<date_t>();
}
template <>
dtime_t Value::GetValue() const {
	return GetValueInternal<dtime_t>();
}
template <>
timestamp_t Value::GetValue() const {
	return GetValueInternal<timestamp_t>();
}

template <>
DUCKDB_API interval_t Value::GetValue() const {
	return GetValueInternal<interval_t>();
}

template <>
DUCKDB_API Value Value::GetValue() const {
	return Value(*this);
}

uintptr_t Value::GetPointer() const {
	D_ASSERT(type() == LogicalType::POINTER);
	return value_.pointer;
}

Value Value::Numeric(const LogicalType &type, int64_t value) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		D_ASSERT(value == 0 || value == 1);
		return Value::BOOLEAN(value ? 1 : 0);
	case LogicalTypeId::TINYINT:
		D_ASSERT(value >= NumericLimits<int8_t>::Minimum() && value <= NumericLimits<int8_t>::Maximum());
		return Value::TINYINT((int8_t)value);
	case LogicalTypeId::SMALLINT:
		D_ASSERT(value >= NumericLimits<int16_t>::Minimum() && value <= NumericLimits<int16_t>::Maximum());
		return Value::SMALLINT((int16_t)value);
	case LogicalTypeId::INTEGER:
		D_ASSERT(value >= NumericLimits<int32_t>::Minimum() && value <= NumericLimits<int32_t>::Maximum());
		return Value::INTEGER((int32_t)value);
	case LogicalTypeId::BIGINT:
		return Value::BIGINT(value);
	case LogicalTypeId::UTINYINT:
		D_ASSERT(value >= NumericLimits<uint8_t>::Minimum() && value <= NumericLimits<uint8_t>::Maximum());
		return Value::UTINYINT((uint8_t)value);
	case LogicalTypeId::USMALLINT:
		D_ASSERT(value >= NumericLimits<uint16_t>::Minimum() && value <= NumericLimits<uint16_t>::Maximum());
		return Value::USMALLINT((uint16_t)value);
	case LogicalTypeId::UINTEGER:
		D_ASSERT(value >= NumericLimits<uint32_t>::Minimum() && value <= NumericLimits<uint32_t>::Maximum());
		return Value::UINTEGER((uint32_t)value);
	case LogicalTypeId::UBIGINT:
		D_ASSERT(value >= 0);
		return Value::UBIGINT(value);
	case LogicalTypeId::HUGEINT:
		return Value::HUGEINT(value);
	case LogicalTypeId::DECIMAL:
		return Value::DECIMAL(value, DecimalType::GetWidth(type), DecimalType::GetScale(type));
	case LogicalTypeId::FLOAT:
		return Value((float)value);
	case LogicalTypeId::DOUBLE:
		return Value((double)value);
	case LogicalTypeId::POINTER:
		return Value::POINTER(value);
	case LogicalTypeId::DATE:
		D_ASSERT(value >= NumericLimits<int32_t>::Minimum() && value <= NumericLimits<int32_t>::Maximum());
		return Value::DATE(date_t(value));
	case LogicalTypeId::TIME:
		return Value::TIME(dtime_t(value));
	case LogicalTypeId::TIMESTAMP:
		return Value::TIMESTAMP(timestamp_t(value));
	case LogicalTypeId::TIMESTAMP_NS:
		return Value::TIMESTAMPNS(timestamp_t(value));
	case LogicalTypeId::TIMESTAMP_MS:
		return Value::TIMESTAMPMS(timestamp_t(value));
	case LogicalTypeId::TIMESTAMP_SEC:
		return Value::TIMESTAMPSEC(timestamp_t(value));
	case LogicalTypeId::TIME_TZ:
		return Value::TIMETZ(dtime_t(value));
	case LogicalTypeId::TIMESTAMP_TZ:
		return Value::TIMESTAMPTZ(timestamp_t(value));
	case LogicalTypeId::ENUM:
		switch (type.InternalType()) {
		case PhysicalType::UINT8:
			D_ASSERT(value >= NumericLimits<uint8_t>::Minimum() && value <= NumericLimits<uint8_t>::Maximum());
			return Value::UTINYINT((uint8_t)value);
		case PhysicalType::UINT16:
			D_ASSERT(value >= NumericLimits<uint16_t>::Minimum() && value <= NumericLimits<uint16_t>::Maximum());
			return Value::USMALLINT((uint16_t)value);
		case PhysicalType::UINT32:
			D_ASSERT(value >= NumericLimits<uint32_t>::Minimum() && value <= NumericLimits<uint32_t>::Maximum());
			return Value::UINTEGER((uint32_t)value);
		default:
			throw InternalException("Enum doesn't accept this physical type");
		}
	default:
		throw InvalidTypeException(type, "Numeric requires numeric type");
	}
}

Value Value::Numeric(const LogicalType &type, hugeint_t value) {
#ifdef DEBUG
	// perform a throwing cast to verify that the type fits
	Value::HUGEINT(value).CastAs(type);
#endif
	switch (type.id()) {
	case LogicalTypeId::HUGEINT:
		return Value::HUGEINT(value);
	case LogicalTypeId::UBIGINT:
		return Value::UBIGINT(Hugeint::Cast<uint64_t>(value));
	default:
		return Value::Numeric(type, Hugeint::Cast<int64_t>(value));
	}
}

//===--------------------------------------------------------------------===//
// GetValueUnsafe
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::BOOL);
	return value_.boolean;
}

template <>
int8_t Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::INT8 || type_.InternalType() == PhysicalType::BOOL);
	return value_.tinyint;
}

template <>
int16_t Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::INT16);
	return value_.smallint;
}

template <>
int32_t Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::INT32);
	return value_.integer;
}

template <>
int64_t Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::INT64);
	return value_.bigint;
}

template <>
hugeint_t Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::INT128);
	return value_.hugeint;
}

template <>
uint8_t Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::UINT8);
	return value_.utinyint;
}

template <>
uint16_t Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::UINT16);
	return value_.usmallint;
}

template <>
uint32_t Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::UINT32);
	return value_.uinteger;
}

template <>
uint64_t Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::UINT64);
	return value_.ubigint;
}

template <>
string Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::VARCHAR);
	return str_value;
}

template <>
DUCKDB_API string_t Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::VARCHAR);
	return string_t(str_value);
}

template <>
float Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::FLOAT);
	return value_.float_;
}

template <>
double Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::DOUBLE);
	return value_.double_;
}

template <>
date_t Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::INT32);
	return value_.date;
}

template <>
dtime_t Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::INT64);
	return value_.time;
}

template <>
timestamp_t Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::INT64);
	return value_.timestamp;
}

template <>
interval_t Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::INTERVAL);
	return value_.interval;
}

//===--------------------------------------------------------------------===//
// GetReferenceUnsafe
//===--------------------------------------------------------------------===//
template <>
int8_t &Value::GetReferenceUnsafe() {
	D_ASSERT(type_.InternalType() == PhysicalType::INT8 || type_.InternalType() == PhysicalType::BOOL);
	return value_.tinyint;
}

template <>
int16_t &Value::GetReferenceUnsafe() {
	D_ASSERT(type_.InternalType() == PhysicalType::INT16);
	return value_.smallint;
}

template <>
int32_t &Value::GetReferenceUnsafe() {
	D_ASSERT(type_.InternalType() == PhysicalType::INT32);
	return value_.integer;
}

template <>
int64_t &Value::GetReferenceUnsafe() {
	D_ASSERT(type_.InternalType() == PhysicalType::INT64);
	return value_.bigint;
}

template <>
hugeint_t &Value::GetReferenceUnsafe() {
	D_ASSERT(type_.InternalType() == PhysicalType::INT128);
	return value_.hugeint;
}

template <>
uint8_t &Value::GetReferenceUnsafe() {
	D_ASSERT(type_.InternalType() == PhysicalType::UINT8);
	return value_.utinyint;
}

template <>
uint16_t &Value::GetReferenceUnsafe() {
	D_ASSERT(type_.InternalType() == PhysicalType::UINT16);
	return value_.usmallint;
}

template <>
uint32_t &Value::GetReferenceUnsafe() {
	D_ASSERT(type_.InternalType() == PhysicalType::UINT32);
	return value_.uinteger;
}

template <>
uint64_t &Value::GetReferenceUnsafe() {
	D_ASSERT(type_.InternalType() == PhysicalType::UINT64);
	return value_.ubigint;
}

template <>
float &Value::GetReferenceUnsafe() {
	D_ASSERT(type_.InternalType() == PhysicalType::FLOAT);
	return value_.float_;
}

template <>
double &Value::GetReferenceUnsafe() {
	D_ASSERT(type_.InternalType() == PhysicalType::DOUBLE);
	return value_.double_;
}

template <>
date_t &Value::GetReferenceUnsafe() {
	D_ASSERT(type_.InternalType() == PhysicalType::INT32);
	return value_.date;
}

template <>
dtime_t &Value::GetReferenceUnsafe() {
	D_ASSERT(type_.InternalType() == PhysicalType::INT64);
	return value_.time;
}

template <>
timestamp_t &Value::GetReferenceUnsafe() {
	D_ASSERT(type_.InternalType() == PhysicalType::INT64);
	return value_.timestamp;
}

template <>
interval_t &Value::GetReferenceUnsafe() {
	D_ASSERT(type_.InternalType() == PhysicalType::INTERVAL);
	return value_.interval;
}

//===--------------------------------------------------------------------===//
// Hash
//===--------------------------------------------------------------------===//
hash_t Value::Hash() const {
	if (IsNull()) {
		return 0;
	}
	switch (type_.InternalType()) {
	case PhysicalType::BOOL:
		return duckdb::Hash(value_.boolean);
	case PhysicalType::INT8:
		return duckdb::Hash(value_.tinyint);
	case PhysicalType::INT16:
		return duckdb::Hash(value_.smallint);
	case PhysicalType::INT32:
		return duckdb::Hash(value_.integer);
	case PhysicalType::INT64:
		return duckdb::Hash(value_.bigint);
	case PhysicalType::UINT8:
		return duckdb::Hash(value_.utinyint);
	case PhysicalType::UINT16:
		return duckdb::Hash(value_.usmallint);
	case PhysicalType::UINT32:
		return duckdb::Hash(value_.uinteger);
	case PhysicalType::UINT64:
		return duckdb::Hash(value_.ubigint);
	case PhysicalType::INT128:
		return duckdb::Hash(value_.hugeint);
	case PhysicalType::FLOAT:
		return duckdb::Hash(value_.float_);
	case PhysicalType::DOUBLE:
		return duckdb::Hash(value_.double_);
	case PhysicalType::INTERVAL:
		return duckdb::Hash(value_.interval);
	case PhysicalType::VARCHAR:
		return duckdb::Hash(string_t(StringValue::Get(*this)));
	case PhysicalType::STRUCT: {
		auto &struct_children = StructValue::GetChildren(*this);
		hash_t hash = 0;
		for (auto &entry : struct_children) {
			hash ^= entry.Hash();
		}
		return hash;
	}
	case PhysicalType::LIST: {
		auto &list_children = ListValue::GetChildren(*this);
		hash_t hash = 0;
		for (auto &entry : list_children) {
			hash ^= entry.Hash();
		}
		return hash;
	}
	default:
		throw InternalException("Unimplemented type for value hash");
	}
}

string Value::ToString() const {
	if (IsNull()) {
		return "NULL";
	}
	switch (type_.id()) {
	case LogicalTypeId::BOOLEAN:
		return value_.boolean ? "True" : "False";
	case LogicalTypeId::TINYINT:
		return to_string(value_.tinyint);
	case LogicalTypeId::SMALLINT:
		return to_string(value_.smallint);
	case LogicalTypeId::INTEGER:
		return to_string(value_.integer);
	case LogicalTypeId::BIGINT:
		return to_string(value_.bigint);
	case LogicalTypeId::UTINYINT:
		return to_string(value_.utinyint);
	case LogicalTypeId::USMALLINT:
		return to_string(value_.usmallint);
	case LogicalTypeId::UINTEGER:
		return to_string(value_.uinteger);
	case LogicalTypeId::UBIGINT:
		return to_string(value_.ubigint);
	case LogicalTypeId::HUGEINT:
		return Hugeint::ToString(value_.hugeint);
	case LogicalTypeId::UUID:
		return UUID::ToString(value_.hugeint);
	case LogicalTypeId::FLOAT:
		return duckdb_fmt::format("{}", value_.float_);
	case LogicalTypeId::DOUBLE:
		return duckdb_fmt::format("{}", value_.double_);
	case LogicalTypeId::DECIMAL: {
		auto internal_type = type_.InternalType();
		auto scale = DecimalType::GetScale(type_);
		if (internal_type == PhysicalType::INT16) {
			return Decimal::ToString(value_.smallint, scale);
		} else if (internal_type == PhysicalType::INT32) {
			return Decimal::ToString(value_.integer, scale);
		} else if (internal_type == PhysicalType::INT64) {
			return Decimal::ToString(value_.bigint, scale);
		} else {
			D_ASSERT(internal_type == PhysicalType::INT128);
			return Decimal::ToString(value_.hugeint, scale);
		}
	}
	case LogicalTypeId::DATE:
		return Date::ToString(value_.date);
	case LogicalTypeId::TIME:
		return Time::ToString(value_.time);
	case LogicalTypeId::TIMESTAMP:
		return Timestamp::ToString(value_.timestamp);
	case LogicalTypeId::TIME_TZ:
		return Time::ToString(value_.time) + Time::ToUTCOffset(0, 0);
	case LogicalTypeId::TIMESTAMP_TZ: {
		// Infinite TSTZ values do not display offsets in PG.
		auto ret = Timestamp::ToString(value_.timestamp);
		if (Timestamp::IsFinite(value_.timestamp)) {
			ret += Time::ToUTCOffset(0, 0);
		}
		return ret;
	}
	case LogicalTypeId::TIMESTAMP_SEC:
		return Timestamp::ToString(Timestamp::FromEpochSeconds(value_.timestamp.value));
	case LogicalTypeId::TIMESTAMP_MS:
		return Timestamp::ToString(Timestamp::FromEpochMs(value_.timestamp.value));
	case LogicalTypeId::TIMESTAMP_NS:
		return Timestamp::ToString(Timestamp::FromEpochNanoSeconds(value_.timestamp.value));
	case LogicalTypeId::INTERVAL:
		return Interval::ToString(value_.interval);
	case LogicalTypeId::JSON:
	case LogicalTypeId::VARCHAR:
		return str_value;
	case LogicalTypeId::BLOB:
		return Blob::ToString(string_t(str_value));
	case LogicalTypeId::POINTER:
		return to_string(value_.pointer);
	case LogicalTypeId::STRUCT: {
		string ret = "{";
		auto &child_types = StructType::GetChildTypes(type_);
		for (size_t i = 0; i < struct_value.size(); i++) {
			auto &name = child_types[i].first;
			auto &child = struct_value[i];
			ret += "'" + name + "': " + child.ToString();
			if (i < struct_value.size() - 1) {
				ret += ", ";
			}
		}
		ret += "}";
		return ret;
	}
	case LogicalTypeId::LIST: {
		string ret = "[";
		for (size_t i = 0; i < list_value.size(); i++) {
			auto &child = list_value[i];
			ret += child.ToString();
			if (i < list_value.size() - 1) {
				ret += ", ";
			}
		}
		ret += "]";
		return ret;
	}
	case LogicalTypeId::MAP: {
		string ret = "{";
		auto &key_list = struct_value[0].list_value;
		auto &value_list = struct_value[1].list_value;
		for (size_t i = 0; i < key_list.size(); i++) {
			ret += key_list[i].ToString() + "=" + value_list[i].ToString();
			if (i < key_list.size() - 1) {
				ret += ", ";
			}
		}
		ret += "}";
		return ret;
	}
	case LogicalTypeId::ENUM: {
		auto &values_insert_order = EnumType::GetValuesInsertOrder(type_);
		uint64_t enum_idx;
		switch (type_.InternalType()) {
		case PhysicalType::UINT8:
			enum_idx = value_.utinyint;
			break;
		case PhysicalType::UINT16:
			enum_idx = value_.usmallint;
			break;
		case PhysicalType::UINT32:
			enum_idx = value_.uinteger;
			break;
		case PhysicalType::UINT64:
			return string((const char *)value_.bigint);
		default:
			throw InternalException("ENUM can only have unsigned integers as physical types");
		}
		return values_insert_order.GetValue(enum_idx).ToString();
	}
	default:
		throw NotImplementedException("Unimplemented type for printing: %s", type_.ToString());
	}
}

string Value::ToSQLString() const {
	if (IsNull()) {
		return ToString();
	}
	switch (type_.id()) {
	case LogicalTypeId::UUID:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::INTERVAL:
	case LogicalTypeId::BLOB:
		return "'" + ToString() + "'::" + type_.ToString();
	case LogicalTypeId::VARCHAR:
		return "'" + StringUtil::Replace(ToString(), "'", "''") + "'";
	case LogicalTypeId::STRUCT: {
		string ret = "{";
		auto &child_types = StructType::GetChildTypes(type_);
		for (size_t i = 0; i < struct_value.size(); i++) {
			auto &name = child_types[i].first;
			auto &child = struct_value[i];
			ret += "'" + name + "': " + child.ToSQLString();
			if (i < struct_value.size() - 1) {
				ret += ", ";
			}
		}
		ret += "}";
		return ret;
	}
	case LogicalTypeId::FLOAT:
		if (!FloatIsFinite(FloatValue::Get(*this))) {
			return "'" + ToString() + "'::" + type_.ToString();
		}
		return ToString();
	case LogicalTypeId::DOUBLE: {
		double val = DoubleValue::Get(*this);
		if (!DoubleIsFinite(val)) {
			if (!Value::IsNan(val)) {
				// to infinity and beyond
				return val < 0 ? "-1e1000" : "1e1000";
			}
			return "'" + ToString() + "'::" + type_.ToString();
		}
		return ToString();
	}
	case LogicalTypeId::LIST: {
		string ret = "[";
		for (size_t i = 0; i < list_value.size(); i++) {
			auto &child = list_value[i];
			ret += child.ToSQLString();
			if (i < list_value.size() - 1) {
				ret += ", ";
			}
		}
		ret += "]";
		return ret;
	}
	default:
		return ToString();
	}
}

//===--------------------------------------------------------------------===//
// Type-specific getters
//===--------------------------------------------------------------------===//
bool BooleanValue::Get(const Value &value) {
	return value.GetValueUnsafe<bool>();
}

int8_t TinyIntValue::Get(const Value &value) {
	return value.GetValueUnsafe<int8_t>();
}

int16_t SmallIntValue::Get(const Value &value) {
	return value.GetValueUnsafe<int16_t>();
}

int32_t IntegerValue::Get(const Value &value) {
	return value.GetValueUnsafe<int32_t>();
}

int64_t BigIntValue::Get(const Value &value) {
	return value.GetValueUnsafe<int64_t>();
}

hugeint_t HugeIntValue::Get(const Value &value) {
	return value.GetValueUnsafe<hugeint_t>();
}

uint8_t UTinyIntValue::Get(const Value &value) {
	return value.GetValueUnsafe<uint8_t>();
}

uint16_t USmallIntValue::Get(const Value &value) {
	return value.GetValueUnsafe<uint16_t>();
}

uint32_t UIntegerValue::Get(const Value &value) {
	return value.GetValueUnsafe<uint32_t>();
}

uint64_t UBigIntValue::Get(const Value &value) {
	return value.GetValueUnsafe<uint64_t>();
}

float FloatValue::Get(const Value &value) {
	return value.GetValueUnsafe<float>();
}

double DoubleValue::Get(const Value &value) {
	return value.GetValueUnsafe<double>();
}

const string &StringValue::Get(const Value &value) {
	D_ASSERT(value.type().InternalType() == PhysicalType::VARCHAR);
	return value.str_value;
}

date_t DateValue::Get(const Value &value) {
	return value.GetValueUnsafe<date_t>();
}

dtime_t TimeValue::Get(const Value &value) {
	return value.GetValueUnsafe<dtime_t>();
}

timestamp_t TimestampValue::Get(const Value &value) {
	return value.GetValueUnsafe<timestamp_t>();
}

interval_t IntervalValue::Get(const Value &value) {
	return value.GetValueUnsafe<interval_t>();
}

const vector<Value> &StructValue::GetChildren(const Value &value) {
	D_ASSERT(value.type().InternalType() == PhysicalType::STRUCT);
	return value.struct_value;
}

const vector<Value> &ListValue::GetChildren(const Value &value) {
	D_ASSERT(value.type().InternalType() == PhysicalType::LIST);
	return value.list_value;
}

hugeint_t IntegralValue::Get(const Value &value) {
	switch (value.type().InternalType()) {
	case PhysicalType::INT8:
		return TinyIntValue::Get(value);
	case PhysicalType::INT16:
		return SmallIntValue::Get(value);
	case PhysicalType::INT32:
		return IntegerValue::Get(value);
	case PhysicalType::INT64:
		return BigIntValue::Get(value);
	case PhysicalType::INT128:
		return HugeIntValue::Get(value);
	case PhysicalType::UINT8:
		return UTinyIntValue::Get(value);
	case PhysicalType::UINT16:
		return USmallIntValue::Get(value);
	case PhysicalType::UINT32:
		return UIntegerValue::Get(value);
	case PhysicalType::UINT64:
		return UBigIntValue::Get(value);
	default:
		throw InternalException("Invalid internal type \"%s\" for IntegralValue::Get", value.type().ToString());
	}
}

//===--------------------------------------------------------------------===//
// Comparison Operators
//===--------------------------------------------------------------------===//
bool Value::operator==(const Value &rhs) const {
	return ValueOperations::Equals(*this, rhs);
}

bool Value::operator!=(const Value &rhs) const {
	return ValueOperations::NotEquals(*this, rhs);
}

bool Value::operator<(const Value &rhs) const {
	return ValueOperations::LessThan(*this, rhs);
}

bool Value::operator>(const Value &rhs) const {
	return ValueOperations::GreaterThan(*this, rhs);
}

bool Value::operator<=(const Value &rhs) const {
	return ValueOperations::LessThanEquals(*this, rhs);
}

bool Value::operator>=(const Value &rhs) const {
	return ValueOperations::GreaterThanEquals(*this, rhs);
}

bool Value::operator==(const int64_t &rhs) const {
	return *this == Value::Numeric(type_, rhs);
}

bool Value::operator!=(const int64_t &rhs) const {
	return *this != Value::Numeric(type_, rhs);
}

bool Value::operator<(const int64_t &rhs) const {
	return *this < Value::Numeric(type_, rhs);
}

bool Value::operator>(const int64_t &rhs) const {
	return *this > Value::Numeric(type_, rhs);
}

bool Value::operator<=(const int64_t &rhs) const {
	return *this <= Value::Numeric(type_, rhs);
}

bool Value::operator>=(const int64_t &rhs) const {
	return *this >= Value::Numeric(type_, rhs);
}

bool Value::TryCastAs(const LogicalType &target_type, Value &new_value, string *error_message, bool strict) const {
	if (type_ == target_type) {
		new_value = Copy();
		return true;
	}
	Vector input(*this);
	Vector result(target_type);
	if (!VectorOperations::TryCast(input, result, 1, error_message, strict)) {
		return false;
	}
	new_value = result.GetValue(0);
	return true;
}

Value Value::CastAs(const LogicalType &target_type, bool strict) const {
	Value new_value;
	string error_message;
	if (!TryCastAs(target_type, new_value, &error_message, strict)) {
		throw InvalidInputException("Failed to cast value: %s", error_message);
	}
	return new_value;
}

bool Value::TryCastAs(const LogicalType &target_type, bool strict) {
	Value new_value;
	string error_message;
	if (!TryCastAs(target_type, new_value, &error_message, strict)) {
		return false;
	}
	type_ = target_type;
	is_null = new_value.is_null;
	value_ = new_value.value_;
	str_value = new_value.str_value;
	struct_value = new_value.struct_value;
	list_value = new_value.list_value;
	return true;
}

void Value::Serialize(Serializer &main_serializer) const {
	FieldWriter writer(main_serializer);
	writer.WriteSerializable(type_);
	writer.WriteField<bool>(IsNull());
	if (!IsNull()) {
		auto &serializer = writer.GetSerializer();
		switch (type_.InternalType()) {
		case PhysicalType::BOOL:
			serializer.Write<int8_t>(value_.boolean);
			break;
		case PhysicalType::INT8:
			serializer.Write<int8_t>(value_.tinyint);
			break;
		case PhysicalType::INT16:
			serializer.Write<int16_t>(value_.smallint);
			break;
		case PhysicalType::INT32:
			serializer.Write<int32_t>(value_.integer);
			break;
		case PhysicalType::INT64:
			serializer.Write<int64_t>(value_.bigint);
			break;
		case PhysicalType::UINT8:
			serializer.Write<uint8_t>(value_.utinyint);
			break;
		case PhysicalType::UINT16:
			serializer.Write<uint16_t>(value_.usmallint);
			break;
		case PhysicalType::UINT32:
			serializer.Write<uint32_t>(value_.uinteger);
			break;
		case PhysicalType::UINT64:
			serializer.Write<uint64_t>(value_.ubigint);
			break;
		case PhysicalType::INT128:
			serializer.Write<hugeint_t>(value_.hugeint);
			break;
		case PhysicalType::FLOAT:
			serializer.Write<float>(value_.float_);
			break;
		case PhysicalType::DOUBLE:
			serializer.Write<double>(value_.double_);
			break;
		case PhysicalType::INTERVAL:
			serializer.Write<interval_t>(value_.interval);
			break;
		case PhysicalType::VARCHAR:
			serializer.WriteString(str_value);
			break;
		default: {
			Vector v(*this);
			v.Serialize(1, serializer);
			break;
		}
		}
	}
	writer.Finalize();
}

Value Value::Deserialize(Deserializer &main_source) {
	FieldReader reader(main_source);
	auto type = reader.ReadRequiredSerializable<LogicalType, LogicalType>();
	auto is_null = reader.ReadRequired<bool>();
	Value new_value = Value(type);
	if (is_null) {
		reader.Finalize();
		return new_value;
	}
	new_value.is_null = false;
	auto &source = reader.GetSource();
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		new_value.value_.boolean = source.Read<int8_t>();
		break;
	case PhysicalType::INT8:
		new_value.value_.tinyint = source.Read<int8_t>();
		break;
	case PhysicalType::INT16:
		new_value.value_.smallint = source.Read<int16_t>();
		break;
	case PhysicalType::INT32:
		new_value.value_.integer = source.Read<int32_t>();
		break;
	case PhysicalType::INT64:
		new_value.value_.bigint = source.Read<int64_t>();
		break;
	case PhysicalType::UINT8:
		new_value.value_.utinyint = source.Read<uint8_t>();
		break;
	case PhysicalType::UINT16:
		new_value.value_.usmallint = source.Read<uint16_t>();
		break;
	case PhysicalType::UINT32:
		new_value.value_.uinteger = source.Read<uint32_t>();
		break;
	case PhysicalType::UINT64:
		new_value.value_.ubigint = source.Read<uint64_t>();
		break;
	case PhysicalType::INT128:
		new_value.value_.hugeint = source.Read<hugeint_t>();
		break;
	case PhysicalType::FLOAT:
		new_value.value_.float_ = source.Read<float>();
		break;
	case PhysicalType::DOUBLE:
		new_value.value_.double_ = source.Read<double>();
		break;
	case PhysicalType::INTERVAL:
		new_value.value_.interval = source.Read<interval_t>();
		break;
	case PhysicalType::VARCHAR:
		new_value.str_value = source.Read<string>();
		break;
	default: {
		Vector v(type);
		v.Deserialize(1, source);
		new_value = v.GetValue(0);
		break;
	}
	}
	reader.Finalize();
	return new_value;
}

void Value::Print() const {
	Printer::Print(ToString());
}

bool Value::NotDistinctFrom(const Value &lvalue, const Value &rvalue) {
	return ValueOperations::NotDistinctFrom(lvalue, rvalue);
}

bool Value::ValuesAreEqual(const Value &result_value, const Value &value) {
	if (result_value.IsNull() != value.IsNull()) {
		return false;
	}
	if (result_value.IsNull() && value.IsNull()) {
		// NULL = NULL in checking code
		return true;
	}
	switch (value.type_.id()) {
	case LogicalTypeId::FLOAT: {
		auto other = result_value.CastAs(LogicalType::FLOAT);
		float ldecimal = value.value_.float_;
		float rdecimal = other.value_.float_;
		return ApproxEqual(ldecimal, rdecimal);
	}
	case LogicalTypeId::DOUBLE: {
		auto other = result_value.CastAs(LogicalType::DOUBLE);
		double ldecimal = value.value_.double_;
		double rdecimal = other.value_.double_;
		return ApproxEqual(ldecimal, rdecimal);
	}
	case LogicalTypeId::VARCHAR: {
		auto other = result_value.CastAs(LogicalType::VARCHAR);
		// some results might contain padding spaces, e.g. when rendering
		// VARCHAR(10) and the string only has 6 characters, they will be padded
		// with spaces to 10 in the rendering. We don't do that here yet as we
		// are looking at internal structures. So just ignore any extra spaces
		// on the right
		string left = other.str_value;
		string right = value.str_value;
		StringUtil::RTrim(left);
		StringUtil::RTrim(right);
		return left == right;
	}
	default:
		if (result_value.type_.id() == LogicalTypeId::FLOAT || result_value.type_.id() == LogicalTypeId::DOUBLE) {
			return Value::ValuesAreEqual(value, result_value);
		}
		return value == result_value;
	}
}

} // namespace duckdb
















#include <cstring> // strlen() on Solaris

namespace duckdb {

Vector::Vector(LogicalType type_p, bool create_data, bool zero_data, idx_t capacity)
    : vector_type(VectorType::FLAT_VECTOR), type(move(type_p)), data(nullptr) {
	if (create_data) {
		Initialize(zero_data, capacity);
	}
}

Vector::Vector(LogicalType type_p, idx_t capacity) : Vector(move(type_p), true, false, capacity) {
}

Vector::Vector(LogicalType type_p, data_ptr_t dataptr)
    : vector_type(VectorType::FLAT_VECTOR), type(move(type_p)), data(dataptr) {
	if (dataptr && type.id() == LogicalTypeId::INVALID) {
		throw InternalException("Cannot create a vector of type INVALID!");
	}
}

Vector::Vector(const VectorCache &cache) : type(cache.GetType()) {
	ResetFromCache(cache);
}

Vector::Vector(Vector &other) : type(other.type) {
	Reference(other);
}

Vector::Vector(Vector &other, const SelectionVector &sel, idx_t count) : type(other.type) {
	Slice(other, sel, count);
}

Vector::Vector(Vector &other, idx_t offset) : type(other.type) {
	Slice(other, offset);
}

Vector::Vector(const Value &value) : type(value.type()) {
	Reference(value);
}

Vector::Vector(Vector &&other) noexcept
    : vector_type(other.vector_type), type(move(other.type)), data(other.data), validity(move(other.validity)),
      buffer(move(other.buffer)), auxiliary(move(other.auxiliary)) {
}

void Vector::Reference(const Value &value) {
	D_ASSERT(GetType().id() == value.type().id());
	this->vector_type = VectorType::CONSTANT_VECTOR;
	buffer = VectorBuffer::CreateConstantVector(value.type());
	auto internal_type = value.type().InternalType();
	if (internal_type == PhysicalType::STRUCT) {
		auto struct_buffer = make_unique<VectorStructBuffer>();
		auto &child_types = StructType::GetChildTypes(value.type());
		auto &child_vectors = struct_buffer->GetChildren();
		auto &value_children = StructValue::GetChildren(value);
		for (idx_t i = 0; i < child_types.size(); i++) {
			auto vector = make_unique<Vector>(value.IsNull() ? Value(child_types[i].second) : value_children[i]);
			child_vectors.push_back(move(vector));
		}
		auxiliary = move(struct_buffer);
		if (value.IsNull()) {
			SetValue(0, value);
		}
	} else if (internal_type == PhysicalType::LIST) {
		auto list_buffer = make_unique<VectorListBuffer>(value.type());
		auxiliary = move(list_buffer);
		data = buffer->GetData();
		SetValue(0, value);
	} else {
		auxiliary.reset();
		data = buffer->GetData();
		SetValue(0, value);
	}
}

void Vector::Reference(Vector &other) {
	D_ASSERT(other.GetType() == GetType());
	Reinterpret(other);
}

void Vector::ReferenceAndSetType(Vector &other) {
	type = other.GetType();
	Reference(other);
}

void Vector::Reinterpret(Vector &other) {
	vector_type = other.vector_type;
	AssignSharedPointer(buffer, other.buffer);
	AssignSharedPointer(auxiliary, other.auxiliary);
	data = other.data;
	validity = other.validity;
}

void Vector::ResetFromCache(const VectorCache &cache) {
	cache.ResetFromCache(*this);
}

void Vector::Slice(Vector &other, idx_t offset) {
	if (other.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		Reference(other);
		return;
	}
	D_ASSERT(other.GetVectorType() == VectorType::FLAT_VECTOR);

	auto internal_type = GetType().InternalType();
	if (internal_type == PhysicalType::STRUCT) {
		Vector new_vector(GetType());
		auto &entries = StructVector::GetEntries(new_vector);
		auto &other_entries = StructVector::GetEntries(other);
		D_ASSERT(entries.size() == other_entries.size());
		for (idx_t i = 0; i < entries.size(); i++) {
			entries[i]->Slice(*other_entries[i], offset);
		}
		if (offset > 0) {
			new_vector.validity.Slice(other.validity, offset);
		} else {
			new_vector.validity = other.validity;
		}
		Reference(new_vector);
	} else {
		Reference(other);
		if (offset > 0) {
			data = data + GetTypeIdSize(internal_type) * offset;
			validity.Slice(other.validity, offset);
		}
	}
}

void Vector::Slice(Vector &other, const SelectionVector &sel, idx_t count) {
	Reference(other);
	Slice(sel, count);
}

void Vector::Slice(const SelectionVector &sel, idx_t count) {
	if (GetVectorType() == VectorType::CONSTANT_VECTOR) {
		// dictionary on a constant is just a constant
		return;
	}
	if (GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		// already a dictionary, slice the current dictionary
		auto &current_sel = DictionaryVector::SelVector(*this);
		auto sliced_dictionary = current_sel.Slice(sel, count);
		buffer = make_buffer<DictionaryBuffer>(move(sliced_dictionary));
		if (GetType().InternalType() == PhysicalType::STRUCT) {
			auto &child_vector = DictionaryVector::Child(*this);

			Vector new_child(child_vector);
			new_child.auxiliary = make_buffer<VectorStructBuffer>(new_child, sel, count);
			auxiliary = make_buffer<VectorChildBuffer>(move(new_child));
		}
		return;
	}
	Vector child_vector(*this);
	auto internal_type = GetType().InternalType();
	if (internal_type == PhysicalType::STRUCT) {
		child_vector.auxiliary = make_buffer<VectorStructBuffer>(*this, sel, count);
	}
	auto child_ref = make_buffer<VectorChildBuffer>(move(child_vector));
	auto dict_buffer = make_buffer<DictionaryBuffer>(sel);
	vector_type = VectorType::DICTIONARY_VECTOR;
	buffer = move(dict_buffer);
	auxiliary = move(child_ref);
}

void Vector::Slice(const SelectionVector &sel, idx_t count, SelCache &cache) {
	if (GetVectorType() == VectorType::DICTIONARY_VECTOR && GetType().InternalType() != PhysicalType::STRUCT) {
		// dictionary vector: need to merge dictionaries
		// check if we have a cached entry
		auto &current_sel = DictionaryVector::SelVector(*this);
		auto target_data = current_sel.data();
		auto entry = cache.cache.find(target_data);
		if (entry != cache.cache.end()) {
			// cached entry exists: use that
			this->buffer = make_buffer<DictionaryBuffer>(((DictionaryBuffer &)*entry->second).GetSelVector());
			vector_type = VectorType::DICTIONARY_VECTOR;
		} else {
			Slice(sel, count);
			cache.cache[target_data] = this->buffer;
		}
	} else {
		Slice(sel, count);
	}
}

void Vector::Initialize(bool zero_data, idx_t capacity) {
	auxiliary.reset();
	validity.Reset();
	auto &type = GetType();
	auto internal_type = type.InternalType();
	if (internal_type == PhysicalType::STRUCT) {
		auto struct_buffer = make_unique<VectorStructBuffer>(type, capacity);
		auxiliary = move(struct_buffer);
	} else if (internal_type == PhysicalType::LIST) {
		auto list_buffer = make_unique<VectorListBuffer>(type, capacity);
		auxiliary = move(list_buffer);
	}
	auto type_size = GetTypeIdSize(internal_type);
	if (type_size > 0) {
		buffer = VectorBuffer::CreateStandardVector(type, capacity);
		data = buffer->GetData();
		if (zero_data) {
			memset(data, 0, capacity * type_size);
		}
	}
}

struct DataArrays {
	Vector &vec;
	data_ptr_t data;
	VectorBuffer *buffer;
	idx_t type_size;
	bool is_nested;
	DataArrays(Vector &vec, data_ptr_t data, VectorBuffer *buffer, idx_t type_size, bool is_nested)
	    : vec(vec), data(data), buffer(buffer), type_size(type_size), is_nested(is_nested) {};
};

void FindChildren(std::vector<DataArrays> &to_resize, VectorBuffer &auxiliary) {
	if (auxiliary.GetBufferType() == VectorBufferType::LIST_BUFFER) {
		auto &buffer = (VectorListBuffer &)auxiliary;
		auto &child = buffer.GetChild();
		auto data = child.GetData();
		if (!data) {
			//! Nested type
			DataArrays arrays(child, data, child.GetBuffer().get(), GetTypeIdSize(child.GetType().InternalType()),
			                  true);
			to_resize.emplace_back(arrays);
			FindChildren(to_resize, *child.GetAuxiliary());
		} else {
			DataArrays arrays(child, data, child.GetBuffer().get(), GetTypeIdSize(child.GetType().InternalType()),
			                  false);
			to_resize.emplace_back(arrays);
		}
	} else if (auxiliary.GetBufferType() == VectorBufferType::STRUCT_BUFFER) {
		auto &buffer = (VectorStructBuffer &)auxiliary;
		auto &children = buffer.GetChildren();
		for (auto &child : children) {
			auto data = child->GetData();
			if (!data) {
				//! Nested type
				DataArrays arrays(*child, data, child->GetBuffer().get(),
				                  GetTypeIdSize(child->GetType().InternalType()), true);
				to_resize.emplace_back(arrays);
				FindChildren(to_resize, *child->GetAuxiliary());
			} else {
				DataArrays arrays(*child, data, child->GetBuffer().get(),
				                  GetTypeIdSize(child->GetType().InternalType()), false);
				to_resize.emplace_back(arrays);
			}
		}
	}
}
void Vector::Resize(idx_t cur_size, idx_t new_size) {
	std::vector<DataArrays> to_resize;
	if (!buffer) {
		buffer = make_unique<VectorBuffer>(0);
	}
	if (!data) {
		//! this is a nested structure
		DataArrays arrays(*this, data, buffer.get(), GetTypeIdSize(GetType().InternalType()), true);
		to_resize.emplace_back(arrays);
		FindChildren(to_resize, *auxiliary);
	} else {
		DataArrays arrays(*this, data, buffer.get(), GetTypeIdSize(GetType().InternalType()), false);
		to_resize.emplace_back(arrays);
	}
	for (auto &data_to_resize : to_resize) {
		if (!data_to_resize.is_nested) {
			auto new_data = unique_ptr<data_t[]>(new data_t[new_size * data_to_resize.type_size]);
			memcpy(new_data.get(), data_to_resize.data, cur_size * data_to_resize.type_size * sizeof(data_t));
			data_to_resize.buffer->SetData(move(new_data));
			data_to_resize.vec.data = data_to_resize.buffer->GetData();
		}
		data_to_resize.vec.validity.Resize(cur_size, new_size);
	}
}

void Vector::SetValue(idx_t index, const Value &val) {
	if (GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		// dictionary: apply dictionary and forward to child
		auto &sel_vector = DictionaryVector::SelVector(*this);
		auto &child = DictionaryVector::Child(*this);
		return child.SetValue(sel_vector.get_index(index), val);
	}
	if (val.type().InternalType() != GetType().InternalType()) {
		SetValue(index, val.CastAs(GetType()));
		return;
	}

	validity.EnsureWritable();
	validity.Set(index, !val.IsNull());
	if (val.IsNull() && GetType().InternalType() != PhysicalType::STRUCT) {
		// for structs we still need to set the child-entries to NULL
		// so we do not bail out yet
		return;
	}

	switch (GetType().InternalType()) {
	case PhysicalType::BOOL:
		((bool *)data)[index] = val.GetValueUnsafe<bool>();
		break;
	case PhysicalType::INT8:
		((int8_t *)data)[index] = val.GetValueUnsafe<int8_t>();
		break;
	case PhysicalType::INT16:
		((int16_t *)data)[index] = val.GetValueUnsafe<int16_t>();
		break;
	case PhysicalType::INT32:
		((int32_t *)data)[index] = val.GetValueUnsafe<int32_t>();
		break;
	case PhysicalType::INT64:
		((int64_t *)data)[index] = val.GetValueUnsafe<int64_t>();
		break;
	case PhysicalType::INT128:
		((hugeint_t *)data)[index] = val.GetValueUnsafe<hugeint_t>();
		break;
	case PhysicalType::UINT8:
		((uint8_t *)data)[index] = val.GetValueUnsafe<uint8_t>();
		break;
	case PhysicalType::UINT16:
		((uint16_t *)data)[index] = val.GetValueUnsafe<uint16_t>();
		break;
	case PhysicalType::UINT32:
		((uint32_t *)data)[index] = val.GetValueUnsafe<uint32_t>();
		break;
	case PhysicalType::UINT64:
		((uint64_t *)data)[index] = val.GetValueUnsafe<uint64_t>();
		break;
	case PhysicalType::FLOAT:
		((float *)data)[index] = val.GetValueUnsafe<float>();
		break;
	case PhysicalType::DOUBLE:
		((double *)data)[index] = val.GetValueUnsafe<double>();
		break;
	case PhysicalType::INTERVAL:
		((interval_t *)data)[index] = val.GetValueUnsafe<interval_t>();
		break;
	case PhysicalType::VARCHAR:
		((string_t *)data)[index] = StringVector::AddStringOrBlob(*this, StringValue::Get(val));
		break;
	case PhysicalType::STRUCT: {
		D_ASSERT(GetVectorType() == VectorType::CONSTANT_VECTOR || GetVectorType() == VectorType::FLAT_VECTOR);

		auto &children = StructVector::GetEntries(*this);
		auto &val_children = StructValue::GetChildren(val);
		D_ASSERT(val.IsNull() || children.size() == val_children.size());
		for (size_t i = 0; i < children.size(); i++) {
			auto &vec_child = children[i];
			if (!val.IsNull()) {
				auto &struct_child = val_children[i];
				vec_child->SetValue(index, struct_child);
			} else {
				vec_child->SetValue(index, Value());
			}
		}
		break;
	}
	case PhysicalType::LIST: {
		auto offset = ListVector::GetListSize(*this);
		auto &val_children = ListValue::GetChildren(val);
		if (!val_children.empty()) {
			for (idx_t i = 0; i < val_children.size(); i++) {
				ListVector::PushBack(*this, val_children[i]);
			}
		}
		//! now set the pointer
		auto &entry = ((list_entry_t *)data)[index];
		entry.length = val_children.size();
		entry.offset = offset;
		break;
	}
	default:
		throw InternalException("Unimplemented type for Vector::SetValue");
	}
}

Value Vector::GetValue(const Vector &v_p, idx_t index_p) {
	const Vector *vector = &v_p;
	idx_t index = index_p;
	bool finished = false;
	while (!finished) {
		switch (vector->GetVectorType()) {
		case VectorType::CONSTANT_VECTOR:
			index = 0;
			finished = true;
			break;
		case VectorType::FLAT_VECTOR:
			finished = true;
			break;
			// dictionary: apply dictionary and forward to child
		case VectorType::DICTIONARY_VECTOR: {
			auto &sel_vector = DictionaryVector::SelVector(*vector);
			auto &child = DictionaryVector::Child(*vector);
			vector = &child;
			index = sel_vector.get_index(index);
			break;
		}
		case VectorType::SEQUENCE_VECTOR: {
			int64_t start, increment;
			SequenceVector::GetSequence(*vector, start, increment);
			return Value::Numeric(vector->GetType(), start + increment * index);
		}
		default:
			throw InternalException("Unimplemented vector type for Vector::GetValue");
		}
	}
	auto data = vector->data;
	auto &validity = vector->validity;
	auto &type = vector->GetType();

	if (!validity.RowIsValid(index)) {
		return Value(vector->GetType());
	}
	switch (vector->GetType().id()) {
	case LogicalTypeId::BOOLEAN:
		return Value::BOOLEAN(((bool *)data)[index]);
	case LogicalTypeId::TINYINT:
		return Value::TINYINT(((int8_t *)data)[index]);
	case LogicalTypeId::SMALLINT:
		return Value::SMALLINT(((int16_t *)data)[index]);
	case LogicalTypeId::INTEGER:
		return Value::INTEGER(((int32_t *)data)[index]);
	case LogicalTypeId::DATE:
		return Value::DATE(((date_t *)data)[index]);
	case LogicalTypeId::TIME:
		return Value::TIME(((dtime_t *)data)[index]);
	case LogicalTypeId::TIME_TZ:
		return Value::TIMETZ(((dtime_t *)data)[index]);
	case LogicalTypeId::BIGINT:
		return Value::BIGINT(((int64_t *)data)[index]);
	case LogicalTypeId::UTINYINT:
		return Value::UTINYINT(((uint8_t *)data)[index]);
	case LogicalTypeId::USMALLINT:
		return Value::USMALLINT(((uint16_t *)data)[index]);
	case LogicalTypeId::UINTEGER:
		return Value::UINTEGER(((uint32_t *)data)[index]);
	case LogicalTypeId::UBIGINT:
		return Value::UBIGINT(((uint64_t *)data)[index]);
	case LogicalTypeId::TIMESTAMP:
		return Value::TIMESTAMP(((timestamp_t *)data)[index]);
	case LogicalTypeId::TIMESTAMP_NS:
		return Value::TIMESTAMPNS(((timestamp_t *)data)[index]);
	case LogicalTypeId::TIMESTAMP_MS:
		return Value::TIMESTAMPMS(((timestamp_t *)data)[index]);
	case LogicalTypeId::TIMESTAMP_SEC:
		return Value::TIMESTAMPSEC(((timestamp_t *)data)[index]);
	case LogicalTypeId::TIMESTAMP_TZ:
		return Value::TIMESTAMPTZ(((timestamp_t *)data)[index]);
	case LogicalTypeId::HUGEINT:
		return Value::HUGEINT(((hugeint_t *)data)[index]);
	case LogicalTypeId::UUID:
		return Value::UUID(((hugeint_t *)data)[index]);
	case LogicalTypeId::DECIMAL: {
		auto width = DecimalType::GetWidth(type);
		auto scale = DecimalType::GetScale(type);
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			return Value::DECIMAL(((int16_t *)data)[index], width, scale);
		case PhysicalType::INT32:
			return Value::DECIMAL(((int32_t *)data)[index], width, scale);
		case PhysicalType::INT64:
			return Value::DECIMAL(((int64_t *)data)[index], width, scale);
		case PhysicalType::INT128:
			return Value::DECIMAL(((hugeint_t *)data)[index], width, scale);
		default:
			throw InternalException("Widths bigger than 38 are not supported");
		}
	}
	case LogicalTypeId::ENUM: {
		switch (type.InternalType()) {
		case PhysicalType::UINT8:
			return Value::ENUM(((uint8_t *)data)[index], type);
		case PhysicalType::UINT16:
			return Value::ENUM(((uint16_t *)data)[index], type);
		case PhysicalType::UINT32:
			return Value::ENUM(((uint32_t *)data)[index], type);
		case PhysicalType::UINT64: //  DEDUP_POINTER_ENUM
			return Value::ENUM(((uint64_t *)data)[index], type);
		default:
			throw InternalException("ENUM can only have unsigned integers as physical types");
		}
	}
	case LogicalTypeId::POINTER:
		return Value::POINTER(((uintptr_t *)data)[index]);
	case LogicalTypeId::FLOAT:
		return Value::FLOAT(((float *)data)[index]);
	case LogicalTypeId::DOUBLE:
		return Value::DOUBLE(((double *)data)[index]);
	case LogicalTypeId::INTERVAL:
		return Value::INTERVAL(((interval_t *)data)[index]);
	case LogicalTypeId::VARCHAR: {
		auto str = ((string_t *)data)[index];
		return Value(str.GetString());
	}
	case LogicalTypeId::JSON: {
		auto str = ((string_t *)data)[index];
		return Value::JSON(str.GetString());
	}
	case LogicalTypeId::AGGREGATE_STATE:
	case LogicalTypeId::BLOB: {
		auto str = ((string_t *)data)[index];
		return Value::BLOB((const_data_ptr_t)str.GetDataUnsafe(), str.GetSize());
	}
	case LogicalTypeId::MAP: {
		auto &child_entries = StructVector::GetEntries(*vector);
		Value key = child_entries[0]->GetValue(index);
		Value value = child_entries[1]->GetValue(index);
		return Value::MAP(move(key), move(value));
	}
	case LogicalTypeId::STRUCT: {
		// we can derive the value schema from the vector schema
		auto &child_entries = StructVector::GetEntries(*vector);
		child_list_t<Value> children;
		for (idx_t child_idx = 0; child_idx < child_entries.size(); child_idx++) {
			auto &struct_child = child_entries[child_idx];
			children.push_back(make_pair(StructType::GetChildName(type, child_idx), struct_child->GetValue(index_p)));
		}
		return Value::STRUCT(move(children));
	}
	case LogicalTypeId::LIST: {
		auto offlen = ((list_entry_t *)data)[index];
		auto &child_vec = ListVector::GetEntry(*vector);
		std::vector<Value> children;
		for (idx_t i = offlen.offset; i < offlen.offset + offlen.length; i++) {
			children.push_back(child_vec.GetValue(i));
		}
		return Value::LIST(ListType::GetChildType(type), move(children));
	}
	default:
		throw InternalException("Unimplemented type for value access");
	}
}

Value Vector::GetValue(idx_t index) const {
	return GetValue(*this, index);
}

// LCOV_EXCL_START
string VectorTypeToString(VectorType type) {
	switch (type) {
	case VectorType::FLAT_VECTOR:
		return "FLAT";
	case VectorType::SEQUENCE_VECTOR:
		return "SEQUENCE";
	case VectorType::DICTIONARY_VECTOR:
		return "DICTIONARY";
	case VectorType::CONSTANT_VECTOR:
		return "CONSTANT";
	default:
		return "UNKNOWN";
	}
}

string Vector::ToString(idx_t count) const {
	string retval =
	    VectorTypeToString(GetVectorType()) + " " + GetType().ToString() + ": " + to_string(count) + " = [ ";
	switch (GetVectorType()) {
	case VectorType::FLAT_VECTOR:
	case VectorType::DICTIONARY_VECTOR:
		for (idx_t i = 0; i < count; i++) {
			retval += GetValue(i).ToString() + (i == count - 1 ? "" : ", ");
		}
		break;
	case VectorType::CONSTANT_VECTOR:
		retval += GetValue(0).ToString();
		break;
	case VectorType::SEQUENCE_VECTOR: {
		int64_t start, increment;
		SequenceVector::GetSequence(*this, start, increment);
		for (idx_t i = 0; i < count; i++) {
			retval += to_string(start + increment * i) + (i == count - 1 ? "" : ", ");
		}
		break;
	}
	default:
		retval += "UNKNOWN VECTOR TYPE";
		break;
	}
	retval += "]";
	return retval;
}

void Vector::Print(idx_t count) {
	Printer::Print(ToString(count));
}

string Vector::ToString() const {
	string retval = VectorTypeToString(GetVectorType()) + " " + GetType().ToString() + ": (UNKNOWN COUNT) [ ";
	switch (GetVectorType()) {
	case VectorType::FLAT_VECTOR:
	case VectorType::DICTIONARY_VECTOR:
		break;
	case VectorType::CONSTANT_VECTOR:
		retval += GetValue(0).ToString();
		break;
	case VectorType::SEQUENCE_VECTOR: {
		break;
	}
	default:
		retval += "UNKNOWN VECTOR TYPE";
		break;
	}
	retval += "]";
	return retval;
}

void Vector::Print() {
	Printer::Print(ToString());
}
// LCOV_EXCL_STOP

template <class T>
static void TemplatedFlattenConstantVector(data_ptr_t data, data_ptr_t old_data, idx_t count) {
	auto constant = Load<T>(old_data);
	auto output = (T *)data;
	for (idx_t i = 0; i < count; i++) {
		output[i] = constant;
	}
}

void Vector::Normalify(idx_t count) {
	switch (GetVectorType()) {
	case VectorType::FLAT_VECTOR:
		// already a flat vector
		break;
	case VectorType::DICTIONARY_VECTOR: {
		// create a new flat vector of this type
		Vector other(GetType(), count);
		// now copy the data of this vector to the other vector, removing the selection vector in the process
		VectorOperations::Copy(*this, other, count, 0, 0);
		// create a reference to the data in the other vector
		this->Reference(other);
		break;
	}
	case VectorType::CONSTANT_VECTOR: {
		bool is_null = ConstantVector::IsNull(*this);
		// allocate a new buffer for the vector
		auto old_buffer = move(buffer);
		auto old_data = data;
		buffer = VectorBuffer::CreateStandardVector(type, MaxValue<idx_t>(STANDARD_VECTOR_SIZE, count));
		data = buffer->GetData();
		vector_type = VectorType::FLAT_VECTOR;
		if (is_null) {
			// constant NULL, set nullmask
			validity.EnsureWritable();
			validity.SetAllInvalid(count);
			return;
		}
		// non-null constant: have to repeat the constant
		switch (GetType().InternalType()) {
		case PhysicalType::BOOL:
			TemplatedFlattenConstantVector<bool>(data, old_data, count);
			break;
		case PhysicalType::INT8:
			TemplatedFlattenConstantVector<int8_t>(data, old_data, count);
			break;
		case PhysicalType::INT16:
			TemplatedFlattenConstantVector<int16_t>(data, old_data, count);
			break;
		case PhysicalType::INT32:
			TemplatedFlattenConstantVector<int32_t>(data, old_data, count);
			break;
		case PhysicalType::INT64:
			TemplatedFlattenConstantVector<int64_t>(data, old_data, count);
			break;
		case PhysicalType::UINT8:
			TemplatedFlattenConstantVector<uint8_t>(data, old_data, count);
			break;
		case PhysicalType::UINT16:
			TemplatedFlattenConstantVector<uint16_t>(data, old_data, count);
			break;
		case PhysicalType::UINT32:
			TemplatedFlattenConstantVector<uint32_t>(data, old_data, count);
			break;
		case PhysicalType::UINT64:
			TemplatedFlattenConstantVector<uint64_t>(data, old_data, count);
			break;
		case PhysicalType::INT128:
			TemplatedFlattenConstantVector<hugeint_t>(data, old_data, count);
			break;
		case PhysicalType::FLOAT:
			TemplatedFlattenConstantVector<float>(data, old_data, count);
			break;
		case PhysicalType::DOUBLE:
			TemplatedFlattenConstantVector<double>(data, old_data, count);
			break;
		case PhysicalType::INTERVAL:
			TemplatedFlattenConstantVector<interval_t>(data, old_data, count);
			break;
		case PhysicalType::VARCHAR:
			TemplatedFlattenConstantVector<string_t>(data, old_data, count);
			break;
		case PhysicalType::LIST: {
			TemplatedFlattenConstantVector<list_entry_t>(data, old_data, count);
			break;
		}
		case PhysicalType::STRUCT: {
			auto normalified_buffer = make_unique<VectorStructBuffer>();

			auto &new_children = normalified_buffer->GetChildren();

			auto &child_entries = StructVector::GetEntries(*this);
			for (auto &child : child_entries) {
				D_ASSERT(child->GetVectorType() == VectorType::CONSTANT_VECTOR);
				auto vector = make_unique<Vector>(*child);
				vector->Normalify(count);
				new_children.push_back(move(vector));
			}
			auxiliary = move(normalified_buffer);
		} break;
		default:
			throw InternalException("Unimplemented type for VectorOperations::Normalify");
		}
		break;
	}
	case VectorType::SEQUENCE_VECTOR: {
		int64_t start, increment;
		SequenceVector::GetSequence(*this, start, increment);

		buffer = VectorBuffer::CreateStandardVector(GetType());
		data = buffer->GetData();
		VectorOperations::GenerateSequence(*this, count, start, increment);
		break;
	}
	default:
		throw InternalException("Unimplemented type for normalify");
	}
}

void Vector::Normalify(const SelectionVector &sel, idx_t count) {
	switch (GetVectorType()) {
	case VectorType::FLAT_VECTOR:
		// already a flat vector
		break;
	case VectorType::SEQUENCE_VECTOR: {
		int64_t start, increment;
		SequenceVector::GetSequence(*this, start, increment);

		buffer = VectorBuffer::CreateStandardVector(GetType());
		data = buffer->GetData();
		VectorOperations::GenerateSequence(*this, count, sel, start, increment);
		break;
	}
	default:
		throw InternalException("Unimplemented type for normalify with selection vector");
	}
}

void Vector::Orrify(idx_t count, VectorData &data) {
	switch (GetVectorType()) {
	case VectorType::DICTIONARY_VECTOR: {
		auto &sel = DictionaryVector::SelVector(*this);
		auto &child = DictionaryVector::Child(*this);
		if (child.GetVectorType() == VectorType::FLAT_VECTOR) {
			data.sel = &sel;
			data.data = FlatVector::GetData(child);
			data.validity = FlatVector::Validity(child);
		} else {
			// dictionary with non-flat child: create a new reference to the child and normalify it
			Vector child_vector(child);
			child_vector.Normalify(sel, count);
			auto new_aux = make_buffer<VectorChildBuffer>(move(child_vector));

			data.sel = &sel;
			data.data = FlatVector::GetData(new_aux->data);
			data.validity = FlatVector::Validity(new_aux->data);
			this->auxiliary = move(new_aux);
		}
		break;
	}
	case VectorType::CONSTANT_VECTOR:
		data.sel = ConstantVector::ZeroSelectionVector(count, data.owned_sel);
		data.data = ConstantVector::GetData(*this);
		data.validity = ConstantVector::Validity(*this);
		break;
	default:
		Normalify(count);
		data.sel = FlatVector::IncrementalSelectionVector();
		data.data = FlatVector::GetData(*this);
		data.validity = FlatVector::Validity(*this);
		break;
	}
}

void Vector::Sequence(int64_t start, int64_t increment) {
	this->vector_type = VectorType::SEQUENCE_VECTOR;
	this->buffer = make_buffer<VectorBuffer>(sizeof(int64_t) * 2);
	auto data = (int64_t *)buffer->GetData();
	data[0] = start;
	data[1] = increment;
	validity.Reset();
	auxiliary.reset();
}

void Vector::Serialize(idx_t count, Serializer &serializer) {
	auto &type = GetType();

	VectorData vdata;
	Orrify(count, vdata);

	const auto write_validity = (count > 0) && !vdata.validity.AllValid();
	serializer.Write<bool>(write_validity);
	if (write_validity) {
		ValidityMask flat_mask(count);
		for (idx_t i = 0; i < count; ++i) {
			auto row_idx = vdata.sel->get_index(i);
			flat_mask.Set(i, vdata.validity.RowIsValid(row_idx));
		}
		serializer.WriteData((const_data_ptr_t)flat_mask.GetData(), flat_mask.ValidityMaskSize(count));
	}
	if (TypeIsConstantSize(type.InternalType())) {
		// constant size type: simple copy
		idx_t write_size = GetTypeIdSize(type.InternalType()) * count;
		auto ptr = unique_ptr<data_t[]>(new data_t[write_size]);
		VectorOperations::WriteToStorage(*this, count, ptr.get());
		serializer.WriteData(ptr.get(), write_size);
	} else {
		switch (type.InternalType()) {
		case PhysicalType::VARCHAR: {
			auto strings = (string_t *)vdata.data;
			for (idx_t i = 0; i < count; i++) {
				auto idx = vdata.sel->get_index(i);
				auto source = !vdata.validity.RowIsValid(idx) ? NullValue<string_t>() : strings[idx];
				serializer.WriteStringLen((const_data_ptr_t)source.GetDataUnsafe(), source.GetSize());
			}
			break;
		}
		case PhysicalType::STRUCT: {
			Normalify(count);
			auto &entries = StructVector::GetEntries(*this);
			for (auto &entry : entries) {
				entry->Serialize(count, serializer);
			}
			break;
		}
		case PhysicalType::LIST: {
			auto &child = ListVector::GetEntry(*this);
			auto list_size = ListVector::GetListSize(*this);

			// serialize the list entries in a flat array
			auto data = unique_ptr<list_entry_t[]>(new list_entry_t[count]);
			auto source_array = (list_entry_t *)vdata.data;
			for (idx_t i = 0; i < count; i++) {
				auto idx = vdata.sel->get_index(i);
				auto source = source_array[idx];
				data[i].offset = source.offset;
				data[i].length = source.length;
			}

			// write the list size
			serializer.Write<idx_t>(list_size);
			serializer.WriteData((data_ptr_t)data.get(), count * sizeof(list_entry_t));

			child.Serialize(list_size, serializer);
			break;
		}
		default:
			throw InternalException("Unimplemented variable width type for Vector::Serialize!");
		}
	}
}

void Vector::Deserialize(idx_t count, Deserializer &source) {
	auto &type = GetType();

	auto &validity = FlatVector::Validity(*this);
	validity.Reset();
	const auto has_validity = source.Read<bool>();
	if (has_validity) {
		validity.Initialize(count);
		source.ReadData((data_ptr_t)validity.GetData(), validity.ValidityMaskSize(count));
	}

	if (TypeIsConstantSize(type.InternalType())) {
		// constant size type: read fixed amount of data from
		auto column_size = GetTypeIdSize(type.InternalType()) * count;
		auto ptr = unique_ptr<data_t[]>(new data_t[column_size]);
		source.ReadData(ptr.get(), column_size);

		VectorOperations::ReadFromStorage(ptr.get(), count, *this);
	} else {
		switch (type.InternalType()) {
		case PhysicalType::VARCHAR: {
			auto strings = FlatVector::GetData<string_t>(*this);
			for (idx_t i = 0; i < count; i++) {
				// read the strings
				auto str = source.Read<string>();
				// now add the string to the StringHeap of the vector
				// and write the pointer into the vector
				if (validity.RowIsValid(i)) {
					strings[i] = StringVector::AddStringOrBlob(*this, str);
				}
			}
			break;
		}
		case PhysicalType::STRUCT: {
			auto &entries = StructVector::GetEntries(*this);
			for (auto &entry : entries) {
				entry->Deserialize(count, source);
			}
			break;
		}
		case PhysicalType::LIST: {
			// read the list size
			auto list_size = source.Read<idx_t>();
			ListVector::Reserve(*this, list_size);
			ListVector::SetListSize(*this, list_size);

			// read the list entry
			auto list_entries = FlatVector::GetData(*this);
			source.ReadData(list_entries, count * sizeof(list_entry_t));

			// deserialize the child vector
			auto &child = ListVector::GetEntry(*this);
			child.Deserialize(list_size, source);

			break;
		}
		default:
			throw InternalException("Unimplemented variable width type for Vector::Deserialize!");
		}
	}
}

void Vector::SetVectorType(VectorType vector_type_p) {
	this->vector_type = vector_type_p;
	if (TypeIsConstantSize(GetType().InternalType()) &&
	    (GetVectorType() == VectorType::CONSTANT_VECTOR || GetVectorType() == VectorType::FLAT_VECTOR)) {
		auxiliary.reset();
	}
	if (vector_type == VectorType::CONSTANT_VECTOR && GetType().InternalType() == PhysicalType::STRUCT) {
		auto &entries = StructVector::GetEntries(*this);
		for (auto &entry : entries) {
			entry->SetVectorType(vector_type);
		}
	}
}

void Vector::UTFVerify(const SelectionVector &sel, idx_t count) {
#ifdef DEBUG
	if (count == 0) {
		return;
	}
	if (GetType().InternalType() == PhysicalType::VARCHAR) {
		// we just touch all the strings and let the sanitizer figure out if any
		// of them are deallocated/corrupt
		switch (GetVectorType()) {
		case VectorType::CONSTANT_VECTOR: {
			auto string = ConstantVector::GetData<string_t>(*this);
			if (!ConstantVector::IsNull(*this)) {
				string->Verify();
			}
			break;
		}
		case VectorType::FLAT_VECTOR: {
			auto strings = FlatVector::GetData<string_t>(*this);
			for (idx_t i = 0; i < count; i++) {
				auto oidx = sel.get_index(i);
				if (validity.RowIsValid(oidx)) {
					strings[oidx].Verify();
				}
			}
			break;
		}
		default:
			break;
		}
	}
#endif
}

void Vector::UTFVerify(idx_t count) {
	auto flat_sel = FlatVector::IncrementalSelectionVector();

	UTFVerify(*flat_sel, count);
}

void Vector::VerifyMap(Vector &vector_p, const SelectionVector &sel_p, idx_t count) {
#ifdef DEBUG
	D_ASSERT(vector_p.GetType().id() == LogicalTypeId::MAP);
	auto valid_check = CheckMapValidity(vector_p, count, sel_p);
	D_ASSERT(valid_check == MapInvalidReason::VALID);
#endif // DEBUG
}

void Vector::Verify(Vector &vector_p, const SelectionVector &sel_p, idx_t count) {
#ifdef DEBUG
	if (count == 0) {
		return;
	}
	Vector *vector = &vector_p;
	const SelectionVector *sel = &sel_p;
	SelectionVector owned_sel;
	auto &type = vector->GetType();
	auto vtype = vector->GetVectorType();
	if (vector->GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(*vector);
		D_ASSERT(child.GetVectorType() != VectorType::DICTIONARY_VECTOR);
		auto &dict_sel = DictionaryVector::SelVector(*vector);
		// merge the selection vectors and verify the child
		auto new_buffer = dict_sel.Slice(*sel, count);
		owned_sel.Initialize(new_buffer);
		sel = &owned_sel;
		vector = &child;
		vtype = vector->GetVectorType();
	}
	if (TypeIsConstantSize(type.InternalType()) &&
	    (vtype == VectorType::CONSTANT_VECTOR || vtype == VectorType::FLAT_VECTOR)) {
		D_ASSERT(!vector->auxiliary);
	}
	if (type.id() == LogicalTypeId::VARCHAR || type.id() == LogicalTypeId::JSON) {
		// verify that there are no '\0' bytes in string values
		switch (vtype) {
		case VectorType::FLAT_VECTOR: {
			auto &validity = FlatVector::Validity(*vector);
			auto strings = FlatVector::GetData<string_t>(*vector);
			for (idx_t i = 0; i < count; i++) {
				auto oidx = sel->get_index(i);
				if (validity.RowIsValid(oidx)) {
					strings[oidx].VerifyNull();
				}
			}
			break;
		}
		default:
			break;
		}
	}

	if (type.InternalType() == PhysicalType::STRUCT) {
		auto &child_types = StructType::GetChildTypes(type);
		D_ASSERT(!child_types.empty());
		// create a selection vector of the non-null entries of the struct vector
		auto &children = StructVector::GetEntries(*vector);
		D_ASSERT(child_types.size() == children.size());
		for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
			D_ASSERT(children[child_idx]->GetType() == child_types[child_idx].second);
			Vector::Verify(*children[child_idx], sel_p, count);
			if (vtype == VectorType::CONSTANT_VECTOR) {
				D_ASSERT(children[child_idx]->GetVectorType() == VectorType::CONSTANT_VECTOR);
				if (ConstantVector::IsNull(*vector)) {
					D_ASSERT(ConstantVector::IsNull(*children[child_idx]));
				}
			}
			if (vtype != VectorType::FLAT_VECTOR) {
				continue;
			}
			ValidityMask *child_validity;
			SelectionVector owned_child_sel;
			const SelectionVector *child_sel = &owned_child_sel;
			if (children[child_idx]->GetVectorType() == VectorType::FLAT_VECTOR) {
				child_sel = FlatVector::IncrementalSelectionVector();
				child_validity = &FlatVector::Validity(*children[child_idx]);
			} else if (children[child_idx]->GetVectorType() == VectorType::DICTIONARY_VECTOR) {
				auto &child = DictionaryVector::Child(*children[child_idx]);
				if (child.GetVectorType() != VectorType::FLAT_VECTOR) {
					continue;
				}
				child_validity = &FlatVector::Validity(child);
				child_sel = &DictionaryVector::SelVector(*children[child_idx]);
			} else if (children[child_idx]->GetVectorType() == VectorType::CONSTANT_VECTOR) {
				child_sel = ConstantVector::ZeroSelectionVector(count, owned_child_sel);
				child_validity = &ConstantVector::Validity(*children[child_idx]);
			} else {
				continue;
			}
			// for any NULL entry in the struct, the child should be NULL as well
			auto &validity = FlatVector::Validity(*vector);
			for (idx_t i = 0; i < count; i++) {
				auto index = sel->get_index(i);
				if (!validity.RowIsValid(index)) {
					auto child_index = child_sel->get_index(sel_p.get_index(i));
					D_ASSERT(!child_validity->RowIsValid(child_index));
				}
			}
		}
		if (vector->GetType().id() == LogicalTypeId::MAP) {
			VerifyMap(*vector, *sel, count);
		}
	}

	if (type.InternalType() == PhysicalType::LIST) {
		if (vtype == VectorType::CONSTANT_VECTOR) {
			if (!ConstantVector::IsNull(*vector)) {
				auto &child = ListVector::GetEntry(*vector);
				SelectionVector child_sel(ListVector::GetListSize(*vector));
				idx_t child_count = 0;
				auto le = ConstantVector::GetData<list_entry_t>(*vector);
				D_ASSERT(le->offset + le->length <= ListVector::GetListSize(*vector));
				for (idx_t k = 0; k < le->length; k++) {
					child_sel.set_index(child_count++, le->offset + k);
				}
				Vector::Verify(child, child_sel, child_count);
			}
		} else if (vtype == VectorType::FLAT_VECTOR) {
			auto &validity = FlatVector::Validity(*vector);
			auto &child = ListVector::GetEntry(*vector);
			auto child_size = ListVector::GetListSize(*vector);
			auto list_data = FlatVector::GetData<list_entry_t>(*vector);
			idx_t total_size = 0;
			for (idx_t i = 0; i < count; i++) {
				auto idx = sel->get_index(i);
				auto &le = list_data[idx];
				if (validity.RowIsValid(idx)) {
					D_ASSERT(le.offset + le.length <= child_size);
					total_size += le.length;
				}
			}
			SelectionVector child_sel(total_size);
			idx_t child_count = 0;
			for (idx_t i = 0; i < count; i++) {
				auto idx = sel->get_index(i);
				auto &le = list_data[idx];
				if (validity.RowIsValid(idx)) {
					D_ASSERT(le.offset + le.length <= child_size);
					for (idx_t k = 0; k < le.length; k++) {
						child_sel.set_index(child_count++, le.offset + k);
					}
				}
			}
			Vector::Verify(child, child_sel, child_count);
		}
	}
#endif
}

void Vector::Verify(idx_t count) {
	auto flat_sel = FlatVector::IncrementalSelectionVector();
	Verify(*this, *flat_sel, count);
}

void FlatVector::SetNull(Vector &vector, idx_t idx, bool is_null) {
	D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR);
	vector.validity.Set(idx, !is_null);
	if (is_null && vector.GetType().InternalType() == PhysicalType::STRUCT) {
		// set all child entries to null as well
		auto &entries = StructVector::GetEntries(vector);
		for (auto &entry : entries) {
			FlatVector::SetNull(*entry, idx, is_null);
		}
	}
}

void ConstantVector::SetNull(Vector &vector, bool is_null) {
	D_ASSERT(vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
	vector.validity.Set(0, !is_null);
	if (is_null && vector.GetType().InternalType() == PhysicalType::STRUCT) {
		// set all child entries to null as well
		auto &entries = StructVector::GetEntries(vector);
		for (auto &entry : entries) {
			entry->SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(*entry, is_null);
		}
	}
}

const SelectionVector *ConstantVector::ZeroSelectionVector(idx_t count, SelectionVector &owned_sel) {
	if (count <= STANDARD_VECTOR_SIZE) {
		return ConstantVector::ZeroSelectionVector();
	}
	owned_sel.Initialize(count);
	for (idx_t i = 0; i < count; i++) {
		owned_sel.set_index(i, 0);
	}
	return &owned_sel;
}

void ConstantVector::Reference(Vector &vector, Vector &source, idx_t position, idx_t count) {
	auto &source_type = source.GetType();
	switch (source_type.InternalType()) {
	case PhysicalType::LIST: {
		// retrieve the list entry from the source vector
		VectorData vdata;
		source.Orrify(count, vdata);

		auto list_index = vdata.sel->get_index(position);
		if (!vdata.validity.RowIsValid(list_index)) {
			// list is null: create null value
			Value null_value(source_type);
			vector.Reference(null_value);
			break;
		}

		auto list_data = (list_entry_t *)vdata.data;
		auto list_entry = list_data[list_index];

		// add the list entry as the first element of "vector"
		// FIXME: we only need to allocate space for 1 tuple here
		auto target_data = FlatVector::GetData<list_entry_t>(vector);
		target_data[0] = list_entry;

		// create a reference to the child list of the source vector
		auto &child = ListVector::GetEntry(vector);
		child.Reference(ListVector::GetEntry(source));

		ListVector::SetListSize(vector, ListVector::GetListSize(source));
		vector.SetVectorType(VectorType::CONSTANT_VECTOR);
		break;
	}
	case PhysicalType::STRUCT: {
		VectorData vdata;
		source.Orrify(count, vdata);

		auto struct_index = vdata.sel->get_index(position);
		if (!vdata.validity.RowIsValid(struct_index)) {
			// null struct: create null value
			Value null_value(source_type);
			vector.Reference(null_value);
			break;
		}

		// struct: pass constant reference into child entries
		auto &source_entries = StructVector::GetEntries(source);
		auto &target_entries = StructVector::GetEntries(vector);
		for (idx_t i = 0; i < source_entries.size(); i++) {
			ConstantVector::Reference(*target_entries[i], *source_entries[i], position, count);
		}
		vector.SetVectorType(VectorType::CONSTANT_VECTOR);
		break;
	}
	default:
		// default behavior: get a value from the vector and reference it
		// this is not that expensive for scalar types
		auto value = source.GetValue(position);
		vector.Reference(value);
		D_ASSERT(vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
		break;
	}
}

string_t StringVector::AddString(Vector &vector, const char *data, idx_t len) {
	return StringVector::AddString(vector, string_t(data, len));
}

string_t StringVector::AddStringOrBlob(Vector &vector, const char *data, idx_t len) {
	return StringVector::AddStringOrBlob(vector, string_t(data, len));
}

string_t StringVector::AddString(Vector &vector, const char *data) {
	return StringVector::AddString(vector, string_t(data, strlen(data)));
}

string_t StringVector::AddString(Vector &vector, const string &data) {
	return StringVector::AddString(vector, string_t(data.c_str(), data.size()));
}

string_t StringVector::AddString(Vector &vector, string_t data) {
	D_ASSERT(vector.GetType().id() == LogicalTypeId::VARCHAR || vector.GetType().id() == LogicalTypeId::JSON);
	if (data.IsInlined()) {
		// string will be inlined: no need to store in string heap
		return data;
	}
	if (!vector.auxiliary) {
		vector.auxiliary = make_buffer<VectorStringBuffer>();
	}
	D_ASSERT(vector.auxiliary->GetBufferType() == VectorBufferType::STRING_BUFFER);
	auto &string_buffer = (VectorStringBuffer &)*vector.auxiliary;
	return string_buffer.AddString(data);
}

string_t StringVector::AddStringOrBlob(Vector &vector, string_t data) {
	D_ASSERT(vector.GetType().InternalType() == PhysicalType::VARCHAR);
	if (data.IsInlined()) {
		// string will be inlined: no need to store in string heap
		return data;
	}
	if (!vector.auxiliary) {
		vector.auxiliary = make_buffer<VectorStringBuffer>();
	}
	D_ASSERT(vector.auxiliary->GetBufferType() == VectorBufferType::STRING_BUFFER);
	auto &string_buffer = (VectorStringBuffer &)*vector.auxiliary;
	return string_buffer.AddBlob(data);
}

string_t StringVector::EmptyString(Vector &vector, idx_t len) {
	D_ASSERT(vector.GetType().InternalType() == PhysicalType::VARCHAR);
	if (len < string_t::INLINE_LENGTH) {
		return string_t(len);
	}
	if (!vector.auxiliary) {
		vector.auxiliary = make_buffer<VectorStringBuffer>();
	}
	D_ASSERT(vector.auxiliary->GetBufferType() == VectorBufferType::STRING_BUFFER);
	auto &string_buffer = (VectorStringBuffer &)*vector.auxiliary;
	return string_buffer.EmptyString(len);
}

void StringVector::AddHandle(Vector &vector, BufferHandle handle) {
	D_ASSERT(vector.GetType().InternalType() == PhysicalType::VARCHAR);
	if (!vector.auxiliary) {
		vector.auxiliary = make_buffer<VectorStringBuffer>();
	}
	auto &string_buffer = (VectorStringBuffer &)*vector.auxiliary;
	string_buffer.AddHeapReference(make_buffer<ManagedVectorBuffer>(move(handle)));
}

void StringVector::AddBuffer(Vector &vector, buffer_ptr<VectorBuffer> buffer) {
	D_ASSERT(vector.GetType().InternalType() == PhysicalType::VARCHAR);
	D_ASSERT(buffer.get() != vector.auxiliary.get());
	if (!vector.auxiliary) {
		vector.auxiliary = make_buffer<VectorStringBuffer>();
	}
	auto &string_buffer = (VectorStringBuffer &)*vector.auxiliary;
	string_buffer.AddHeapReference(move(buffer));
}

void StringVector::AddHeapReference(Vector &vector, Vector &other) {
	D_ASSERT(vector.GetType().InternalType() == PhysicalType::VARCHAR);
	D_ASSERT(other.GetType().InternalType() == PhysicalType::VARCHAR);

	if (other.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		StringVector::AddHeapReference(vector, DictionaryVector::Child(other));
		return;
	}
	if (!other.auxiliary) {
		return;
	}
	StringVector::AddBuffer(vector, other.auxiliary);
}

vector<unique_ptr<Vector>> &StructVector::GetEntries(Vector &vector) {
	D_ASSERT(vector.GetType().id() == LogicalTypeId::STRUCT || vector.GetType().id() == LogicalTypeId::MAP);
	if (vector.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(vector);
		return StructVector::GetEntries(child);
	}
	D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR ||
	         vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
	D_ASSERT(vector.auxiliary);
	D_ASSERT(vector.auxiliary->GetBufferType() == VectorBufferType::STRUCT_BUFFER);
	return ((VectorStructBuffer *)vector.auxiliary.get())->GetChildren();
}

const vector<unique_ptr<Vector>> &StructVector::GetEntries(const Vector &vector) {
	return GetEntries((Vector &)vector);
}

const Vector &ListVector::GetEntry(const Vector &vector) {
	D_ASSERT(vector.GetType().id() == LogicalTypeId::LIST);
	if (vector.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(vector);
		return ListVector::GetEntry(child);
	}
	D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR ||
	         vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
	D_ASSERT(vector.auxiliary);
	D_ASSERT(vector.auxiliary->GetBufferType() == VectorBufferType::LIST_BUFFER);
	return ((VectorListBuffer *)vector.auxiliary.get())->GetChild();
}

Vector &ListVector::GetEntry(Vector &vector) {
	const Vector &cvector = vector;
	return const_cast<Vector &>(ListVector::GetEntry(cvector));
}

void ListVector::Reserve(Vector &vector, idx_t required_capacity) {
	D_ASSERT(vector.GetType().id() == LogicalTypeId::LIST);
	D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR ||
	         vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
	D_ASSERT(vector.auxiliary);
	D_ASSERT(vector.auxiliary->GetBufferType() == VectorBufferType::LIST_BUFFER);
	auto &child_buffer = *((VectorListBuffer *)vector.auxiliary.get());
	child_buffer.Reserve(required_capacity);
}

template <class T>
void TemplatedSearchInMap(Vector &list, T key, vector<idx_t> &offsets, bool is_key_null, idx_t offset, idx_t length) {
	auto &list_vector = ListVector::GetEntry(list);
	VectorData vector_data;
	list_vector.Orrify(ListVector::GetListSize(list), vector_data);
	auto data = (T *)vector_data.data;
	auto validity_mask = vector_data.validity;

	if (is_key_null) {
		for (idx_t i = offset; i < offset + length; i++) {
			if (!validity_mask.RowIsValid(i)) {
				offsets.push_back(i);
			}
		}
	} else {
		for (idx_t i = offset; i < offset + length; i++) {
			if (!validity_mask.RowIsValid(i)) {
				continue;
			}
			if (key == data[i]) {
				offsets.push_back(i);
			}
		}
	}
}

template <class T>
void TemplatedSearchInMap(Vector &list, const Value &key, vector<idx_t> &offsets, bool is_key_null, idx_t offset,
                          idx_t length) {
	TemplatedSearchInMap<T>(list, key.template GetValueUnsafe<T>(), offsets, is_key_null, offset, length);
}

void SearchStringInMap(Vector &list, const string &key, vector<idx_t> &offsets, bool is_key_null, idx_t offset,
                       idx_t length) {
	auto &list_vector = ListVector::GetEntry(list);
	VectorData vector_data;
	list_vector.Orrify(ListVector::GetListSize(list), vector_data);
	auto data = (string_t *)vector_data.data;
	auto validity_mask = vector_data.validity;
	if (is_key_null) {
		for (idx_t i = offset; i < offset + length; i++) {
			if (!validity_mask.RowIsValid(i)) {
				offsets.push_back(i);
			}
		}
	} else {
		string_t key_str_t(key);
		for (idx_t i = offset; i < offset + length; i++) {
			if (!validity_mask.RowIsValid(i)) {
				continue;
			}
			if (Equals::Operation<string_t>(data[i], key_str_t)) {
				offsets.push_back(i);
			}
		}
	}
}

vector<idx_t> ListVector::Search(Vector &list, const Value &key, idx_t row) {
	vector<idx_t> offsets;

	auto &list_vector = ListVector::GetEntry(list);
	auto &entry = ((list_entry_t *)list.GetData())[row];

	switch (list_vector.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedSearchInMap<int8_t>(list, key, offsets, key.IsNull(), entry.offset, entry.length);
		break;
	case PhysicalType::INT16:
		TemplatedSearchInMap<int16_t>(list, key, offsets, key.IsNull(), entry.offset, entry.length);
		break;
	case PhysicalType::INT32:
		TemplatedSearchInMap<int32_t>(list, key, offsets, key.IsNull(), entry.offset, entry.length);
		break;
	case PhysicalType::INT64:
		TemplatedSearchInMap<int64_t>(list, key, offsets, key.IsNull(), entry.offset, entry.length);
		break;
	case PhysicalType::INT128:
		TemplatedSearchInMap<hugeint_t>(list, key, offsets, key.IsNull(), entry.offset, entry.length);
		break;
	case PhysicalType::UINT8:
		TemplatedSearchInMap<uint8_t>(list, key, offsets, key.IsNull(), entry.offset, entry.length);
		break;
	case PhysicalType::UINT16:
		TemplatedSearchInMap<uint16_t>(list, key, offsets, key.IsNull(), entry.offset, entry.length);
		break;
	case PhysicalType::UINT32:
		TemplatedSearchInMap<uint32_t>(list, key, offsets, key.IsNull(), entry.offset, entry.length);
		break;
	case PhysicalType::UINT64:
		TemplatedSearchInMap<uint64_t>(list, key, offsets, key.IsNull(), entry.offset, entry.length);
		break;
	case PhysicalType::FLOAT:
		TemplatedSearchInMap<float>(list, key, offsets, key.IsNull(), entry.offset, entry.length);
		break;
	case PhysicalType::DOUBLE:
		TemplatedSearchInMap<double>(list, key, offsets, key.IsNull(), entry.offset, entry.length);
		break;
	case PhysicalType::VARCHAR:
		SearchStringInMap(list, StringValue::Get(key), offsets, key.IsNull(), entry.offset, entry.length);
		break;
	default:
		throw InvalidTypeException(list.GetType().id(), "Invalid type for List Vector Search");
	}
	return offsets;
}

Value ListVector::GetValuesFromOffsets(Vector &list, vector<idx_t> &offsets) {
	auto &child_vec = ListVector::GetEntry(list);
	vector<Value> list_values;
	list_values.reserve(offsets.size());
	for (auto &offset : offsets) {
		list_values.push_back(child_vec.GetValue(offset));
	}
	return Value::LIST(ListType::GetChildType(list.GetType()), move(list_values));
}

idx_t ListVector::GetListSize(const Vector &vec) {
	if (vec.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(vec);
		return ListVector::GetListSize(child);
	}
	D_ASSERT(vec.auxiliary);
	return ((VectorListBuffer &)*vec.auxiliary).size;
}

void ListVector::ReferenceEntry(Vector &vector, Vector &other) {
	D_ASSERT(vector.GetType().id() == LogicalTypeId::LIST);
	D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR ||
	         vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
	D_ASSERT(other.GetType().id() == LogicalTypeId::LIST);
	D_ASSERT(other.GetVectorType() == VectorType::FLAT_VECTOR || other.GetVectorType() == VectorType::CONSTANT_VECTOR);
	vector.auxiliary = other.auxiliary;
}

void ListVector::SetListSize(Vector &vec, idx_t size) {
	if (vec.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(vec);
		ListVector::SetListSize(child, size);
	}
	((VectorListBuffer &)*vec.auxiliary).size = size;
}

void ListVector::Append(Vector &target, const Vector &source, idx_t source_size, idx_t source_offset) {
	if (source_size - source_offset == 0) {
		//! Nothing to add
		return;
	}
	auto &target_buffer = (VectorListBuffer &)*target.auxiliary;
	target_buffer.Append(source, source_size, source_offset);
}

void ListVector::Append(Vector &target, const Vector &source, const SelectionVector &sel, idx_t source_size,
                        idx_t source_offset) {
	if (source_size - source_offset == 0) {
		//! Nothing to add
		return;
	}
	auto &target_buffer = (VectorListBuffer &)*target.auxiliary;
	target_buffer.Append(source, sel, source_size, source_offset);
}

void ListVector::PushBack(Vector &target, const Value &insert) {
	auto &target_buffer = (VectorListBuffer &)*target.auxiliary;
	target_buffer.PushBack(insert);
}

} // namespace duckdb








namespace duckdb {

buffer_ptr<VectorBuffer> VectorBuffer::CreateStandardVector(PhysicalType type, idx_t capacity) {
	return make_buffer<VectorBuffer>(capacity * GetTypeIdSize(type));
}

buffer_ptr<VectorBuffer> VectorBuffer::CreateConstantVector(PhysicalType type) {
	return make_buffer<VectorBuffer>(GetTypeIdSize(type));
}

buffer_ptr<VectorBuffer> VectorBuffer::CreateConstantVector(const LogicalType &type) {
	return VectorBuffer::CreateConstantVector(type.InternalType());
}

buffer_ptr<VectorBuffer> VectorBuffer::CreateStandardVector(const LogicalType &type, idx_t capacity) {
	return VectorBuffer::CreateStandardVector(type.InternalType(), capacity);
}

VectorStringBuffer::VectorStringBuffer() : VectorBuffer(VectorBufferType::STRING_BUFFER) {
}

VectorStructBuffer::VectorStructBuffer() : VectorBuffer(VectorBufferType::STRUCT_BUFFER) {
}

VectorStructBuffer::VectorStructBuffer(const LogicalType &type, idx_t capacity)
    : VectorBuffer(VectorBufferType::STRUCT_BUFFER) {
	auto &child_types = StructType::GetChildTypes(type);
	for (auto &child_type : child_types) {
		auto vector = make_unique<Vector>(child_type.second, capacity);
		children.push_back(move(vector));
	}
}

VectorStructBuffer::VectorStructBuffer(Vector &other, const SelectionVector &sel, idx_t count)
    : VectorBuffer(VectorBufferType::STRUCT_BUFFER) {
	auto &other_vector = StructVector::GetEntries(other);
	for (auto &child_vector : other_vector) {
		auto vector = make_unique<Vector>(*child_vector, sel, count);
		children.push_back(move(vector));
	}
}

VectorStructBuffer::~VectorStructBuffer() {
}

VectorListBuffer::VectorListBuffer(unique_ptr<Vector> vector, idx_t initial_capacity)
    : VectorBuffer(VectorBufferType::LIST_BUFFER), capacity(initial_capacity), child(move(vector)) {
}

VectorListBuffer::VectorListBuffer(const LogicalType &list_type, idx_t initial_capacity)
    : VectorBuffer(VectorBufferType::LIST_BUFFER), capacity(initial_capacity),
      child(make_unique<Vector>(ListType::GetChildType(list_type), initial_capacity)) {
}

void VectorListBuffer::Reserve(idx_t to_reserve) {
	if (to_reserve > capacity) {
		idx_t new_capacity = NextPowerOfTwo(to_reserve);
		D_ASSERT(new_capacity >= to_reserve);
		child->Resize(capacity, new_capacity);
		capacity = new_capacity;
	}
}

void VectorListBuffer::Append(const Vector &to_append, idx_t to_append_size, idx_t source_offset) {
	Reserve(size + to_append_size - source_offset);
	VectorOperations::Copy(to_append, *child, to_append_size, source_offset, size);
	size += to_append_size - source_offset;
}

void VectorListBuffer::Append(const Vector &to_append, const SelectionVector &sel, idx_t to_append_size,
                              idx_t source_offset) {
	Reserve(size + to_append_size - source_offset);
	VectorOperations::Copy(to_append, *child, sel, to_append_size, source_offset, size);
	size += to_append_size - source_offset;
}

void VectorListBuffer::PushBack(const Value &insert) {
	if (size + 1 > capacity) {
		child->Resize(capacity, capacity * 2);
		capacity *= 2;
	}
	child->SetValue(size++, insert);
}

VectorListBuffer::~VectorListBuffer() {
}

ManagedVectorBuffer::ManagedVectorBuffer(BufferHandle handle)
    : VectorBuffer(VectorBufferType::MANAGED_BUFFER), handle(move(handle)) {
}

ManagedVectorBuffer::~ManagedVectorBuffer() {
}

} // namespace duckdb




namespace duckdb {

class VectorCacheBuffer : public VectorBuffer {
public:
	explicit VectorCacheBuffer(Allocator &allocator, const LogicalType &type_p)
	    : VectorBuffer(VectorBufferType::OPAQUE_BUFFER), type(type_p) {
		auto internal_type = type.InternalType();
		switch (internal_type) {
		case PhysicalType::LIST: {
			// memory for the list offsets
			owned_data = allocator.Allocate(STANDARD_VECTOR_SIZE * GetTypeIdSize(internal_type));
			// child data of the list
			auto &child_type = ListType::GetChildType(type);
			child_caches.push_back(make_buffer<VectorCacheBuffer>(allocator, child_type));
			auto child_vector = make_unique<Vector>(child_type, false, false);
			auxiliary = make_unique<VectorListBuffer>(move(child_vector));
			break;
		}
		case PhysicalType::STRUCT: {
			auto &child_types = StructType::GetChildTypes(type);
			for (auto &child_type : child_types) {
				child_caches.push_back(make_buffer<VectorCacheBuffer>(allocator, child_type.second));
			}
			auto struct_buffer = make_unique<VectorStructBuffer>(type);
			auxiliary = move(struct_buffer);
			break;
		}
		default:
			owned_data = allocator.Allocate(STANDARD_VECTOR_SIZE * GetTypeIdSize(internal_type));
			break;
		}
	}

	void ResetFromCache(Vector &result, const buffer_ptr<VectorBuffer> &buffer) {
		D_ASSERT(type == result.GetType());
		auto internal_type = type.InternalType();
		result.vector_type = VectorType::FLAT_VECTOR;
		AssignSharedPointer(result.buffer, buffer);
		result.validity.Reset();
		switch (internal_type) {
		case PhysicalType::LIST: {
			result.data = owned_data->get();
			// reinitialize the VectorListBuffer
			AssignSharedPointer(result.auxiliary, auxiliary);
			// propagate through child
			auto &list_buffer = (VectorListBuffer &)*result.auxiliary;
			list_buffer.capacity = STANDARD_VECTOR_SIZE;
			list_buffer.size = 0;

			auto &list_child = list_buffer.GetChild();
			auto &child_cache = (VectorCacheBuffer &)*child_caches[0];
			child_cache.ResetFromCache(list_child, child_caches[0]);
			break;
		}
		case PhysicalType::STRUCT: {
			// struct does not have data
			result.data = nullptr;
			// reinitialize the VectorStructBuffer
			AssignSharedPointer(result.auxiliary, auxiliary);
			// propagate through children
			auto &children = ((VectorStructBuffer &)*result.auxiliary).GetChildren();
			for (idx_t i = 0; i < children.size(); i++) {
				auto &child_cache = (VectorCacheBuffer &)*child_caches[i];
				child_cache.ResetFromCache(*children[i], child_caches[i]);
			}
			break;
		}
		default:
			// regular type: no aux data and reset data to cached data
			result.data = owned_data->get();
			result.auxiliary.reset();
			break;
		}
	}

	const LogicalType &GetType() {
		return type;
	}

private:
	//! The type of the vector cache
	LogicalType type;
	//! Owned data
	unique_ptr<AllocatedData> owned_data;
	//! Child caches (if any). Used for nested types.
	vector<buffer_ptr<VectorBuffer>> child_caches;
	//! Aux data for the vector (if any)
	buffer_ptr<VectorBuffer> auxiliary;
};

VectorCache::VectorCache(Allocator &allocator, const LogicalType &type_p) {
	buffer = make_unique<VectorCacheBuffer>(allocator, type_p);
}

void VectorCache::ResetFromCache(Vector &result) const {
	D_ASSERT(buffer);
	auto &vcache = (VectorCacheBuffer &)*buffer;
	vcache.ResetFromCache(result, buffer);
}

const LogicalType &VectorCache::GetType() const {
	auto &vcache = (VectorCacheBuffer &)*buffer;
	return vcache.GetType();
}

} // namespace duckdb


namespace duckdb {

const SelectionVector *ConstantVector::ZeroSelectionVector() {
	static const SelectionVector ZERO_SELECTION_VECTOR = SelectionVector((sel_t *)ConstantVector::ZERO_VECTOR);
	return &ZERO_SELECTION_VECTOR;
}

const SelectionVector *FlatVector::IncrementalSelectionVector() {
	static const SelectionVector INCREMENTAL_SELECTION_VECTOR;
	return &INCREMENTAL_SELECTION_VECTOR;
}

const sel_t ConstantVector::ZERO_VECTOR[STANDARD_VECTOR_SIZE] = {0};

} // namespace duckdb


















#include <cmath>

namespace duckdb {

LogicalType::LogicalType() : LogicalType(LogicalTypeId::INVALID) {
}

LogicalType::LogicalType(LogicalTypeId id) : id_(id) {
	physical_type_ = GetInternalType();
}
LogicalType::LogicalType(LogicalTypeId id, shared_ptr<ExtraTypeInfo> type_info_p)
    : id_(id), type_info_(move(type_info_p)) {
	physical_type_ = GetInternalType();
}

LogicalType::LogicalType(const LogicalType &other)
    : id_(other.id_), physical_type_(other.physical_type_), type_info_(other.type_info_) {
}

LogicalType::LogicalType(LogicalType &&other) noexcept
    : id_(other.id_), physical_type_(other.physical_type_), type_info_(move(other.type_info_)) {
}

hash_t LogicalType::Hash() const {
	return duckdb::Hash<uint8_t>((uint8_t)id_);
}

PhysicalType LogicalType::GetInternalType() {
	switch (id_) {
	case LogicalTypeId::BOOLEAN:
		return PhysicalType::BOOL;
	case LogicalTypeId::TINYINT:
		return PhysicalType::INT8;
	case LogicalTypeId::UTINYINT:
		return PhysicalType::UINT8;
	case LogicalTypeId::SMALLINT:
		return PhysicalType::INT16;
	case LogicalTypeId::USMALLINT:
		return PhysicalType::UINT16;
	case LogicalTypeId::SQLNULL:
	case LogicalTypeId::DATE:
	case LogicalTypeId::INTEGER:
		return PhysicalType::INT32;
	case LogicalTypeId::UINTEGER:
		return PhysicalType::UINT32;
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::TIMESTAMP_TZ:
		return PhysicalType::INT64;
	case LogicalTypeId::UBIGINT:
		return PhysicalType::UINT64;
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UUID:
		return PhysicalType::INT128;
	case LogicalTypeId::FLOAT:
		return PhysicalType::FLOAT;
	case LogicalTypeId::DOUBLE:
		return PhysicalType::DOUBLE;
	case LogicalTypeId::DECIMAL: {
		if (!type_info_) {
			return PhysicalType::INVALID;
		}
		auto width = DecimalType::GetWidth(*this);
		if (width <= Decimal::MAX_WIDTH_INT16) {
			return PhysicalType::INT16;
		} else if (width <= Decimal::MAX_WIDTH_INT32) {
			return PhysicalType::INT32;
		} else if (width <= Decimal::MAX_WIDTH_INT64) {
			return PhysicalType::INT64;
		} else if (width <= Decimal::MAX_WIDTH_INT128) {
			return PhysicalType::INT128;
		} else {
			throw InternalException("Widths bigger than %d are not supported", DecimalType::MaxWidth());
		}
	}
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::CHAR:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::JSON:
		return PhysicalType::VARCHAR;
	case LogicalTypeId::INTERVAL:
		return PhysicalType::INTERVAL;
	case LogicalTypeId::MAP:
	case LogicalTypeId::STRUCT:
		return PhysicalType::STRUCT;
	case LogicalTypeId::LIST:
		return PhysicalType::LIST;
	case LogicalTypeId::POINTER:
		// LCOV_EXCL_START
		if (sizeof(uintptr_t) == sizeof(uint32_t)) {
			return PhysicalType::UINT32;
		} else if (sizeof(uintptr_t) == sizeof(uint64_t)) {
			return PhysicalType::UINT64;
		} else {
			throw InternalException("Unsupported pointer size");
		}
		// LCOV_EXCL_STOP
	case LogicalTypeId::VALIDITY:
		return PhysicalType::BIT;
	case LogicalTypeId::ENUM: {
		D_ASSERT(type_info_);
		return EnumType::GetPhysicalType(*this);
	}
	case LogicalTypeId::TABLE:
	case LogicalTypeId::ANY:
	case LogicalTypeId::INVALID:
	case LogicalTypeId::UNKNOWN:
		return PhysicalType::INVALID;
	case LogicalTypeId::USER:
		return PhysicalType::UNKNOWN;
	case LogicalTypeId::AGGREGATE_STATE:
		return PhysicalType::VARCHAR;
	default:
		throw InternalException("Invalid LogicalType %s", ToString());
	}
}

constexpr const LogicalTypeId LogicalType::INVALID;
constexpr const LogicalTypeId LogicalType::SQLNULL;
constexpr const LogicalTypeId LogicalType::BOOLEAN;
constexpr const LogicalTypeId LogicalType::TINYINT;
constexpr const LogicalTypeId LogicalType::UTINYINT;
constexpr const LogicalTypeId LogicalType::SMALLINT;
constexpr const LogicalTypeId LogicalType::USMALLINT;
constexpr const LogicalTypeId LogicalType::INTEGER;
constexpr const LogicalTypeId LogicalType::UINTEGER;
constexpr const LogicalTypeId LogicalType::BIGINT;
constexpr const LogicalTypeId LogicalType::UBIGINT;
constexpr const LogicalTypeId LogicalType::HUGEINT;
constexpr const LogicalTypeId LogicalType::UUID;
constexpr const LogicalTypeId LogicalType::FLOAT;
constexpr const LogicalTypeId LogicalType::DOUBLE;
constexpr const LogicalTypeId LogicalType::DATE;

constexpr const LogicalTypeId LogicalType::TIMESTAMP;
constexpr const LogicalTypeId LogicalType::TIMESTAMP_MS;
constexpr const LogicalTypeId LogicalType::TIMESTAMP_NS;
constexpr const LogicalTypeId LogicalType::TIMESTAMP_S;

constexpr const LogicalTypeId LogicalType::TIME;

constexpr const LogicalTypeId LogicalType::TIME_TZ;
constexpr const LogicalTypeId LogicalType::TIMESTAMP_TZ;

constexpr const LogicalTypeId LogicalType::HASH;
constexpr const LogicalTypeId LogicalType::POINTER;

constexpr const LogicalTypeId LogicalType::VARCHAR;
constexpr const LogicalTypeId LogicalType::JSON;

constexpr const LogicalTypeId LogicalType::BLOB;
constexpr const LogicalTypeId LogicalType::INTERVAL;
constexpr const LogicalTypeId LogicalType::ROW_TYPE;

// TODO these are incomplete and should maybe not exist as such
constexpr const LogicalTypeId LogicalType::TABLE;

constexpr const LogicalTypeId LogicalType::ANY;

const vector<LogicalType> LogicalType::Numeric() {
	vector<LogicalType> types = {LogicalType::TINYINT,   LogicalType::SMALLINT,  LogicalType::INTEGER,
	                             LogicalType::BIGINT,    LogicalType::HUGEINT,   LogicalType::FLOAT,
	                             LogicalType::DOUBLE,    LogicalTypeId::DECIMAL, LogicalType::UTINYINT,
	                             LogicalType::USMALLINT, LogicalType::UINTEGER,  LogicalType::UBIGINT};
	return types;
}

const vector<LogicalType> LogicalType::Integral() {
	vector<LogicalType> types = {LogicalType::TINYINT,   LogicalType::SMALLINT, LogicalType::INTEGER,
	                             LogicalType::BIGINT,    LogicalType::HUGEINT,  LogicalType::UTINYINT,
	                             LogicalType::USMALLINT, LogicalType::UINTEGER, LogicalType::UBIGINT};
	return types;
}

const vector<LogicalType> LogicalType::AllTypes() {
	vector<LogicalType> types = {
	    LogicalType::BOOLEAN,  LogicalType::TINYINT,   LogicalType::SMALLINT,     LogicalType::INTEGER,
	    LogicalType::BIGINT,   LogicalType::DATE,      LogicalType::TIMESTAMP,    LogicalType::DOUBLE,
	    LogicalType::FLOAT,    LogicalType::VARCHAR,   LogicalType::BLOB,         LogicalType::INTERVAL,
	    LogicalType::HUGEINT,  LogicalTypeId::DECIMAL, LogicalType::UTINYINT,     LogicalType::USMALLINT,
	    LogicalType::UINTEGER, LogicalType::UBIGINT,   LogicalType::TIME,         LogicalTypeId::LIST,
	    LogicalTypeId::STRUCT, LogicalType::TIME_TZ,   LogicalType::TIMESTAMP_TZ, LogicalTypeId::MAP,
	    LogicalType::UUID,     LogicalType::JSON};
	return types;
}

const PhysicalType ROW_TYPE = PhysicalType::INT64;

// LCOV_EXCL_START
string TypeIdToString(PhysicalType type) {
	switch (type) {
	case PhysicalType::BOOL:
		return "BOOL";
	case PhysicalType::INT8:
		return "INT8";
	case PhysicalType::INT16:
		return "INT16";
	case PhysicalType::INT32:
		return "INT32";
	case PhysicalType::INT64:
		return "INT64";
	case PhysicalType::UINT8:
		return "UINT8";
	case PhysicalType::UINT16:
		return "UINT16";
	case PhysicalType::UINT32:
		return "UINT32";
	case PhysicalType::UINT64:
		return "UINT64";
	case PhysicalType::INT128:
		return "INT128";
	case PhysicalType::FLOAT:
		return "FLOAT";
	case PhysicalType::DOUBLE:
		return "DOUBLE";
	case PhysicalType::VARCHAR:
		return "VARCHAR";
	case PhysicalType::INTERVAL:
		return "INTERVAL";
	case PhysicalType::STRUCT:
		return "STRUCT";
	case PhysicalType::LIST:
		return "LIST";
	case PhysicalType::INVALID:
		return "INVALID";
	case PhysicalType::BIT:
		return "BIT";
	case PhysicalType::NA:
		return "NA";
	case PhysicalType::HALF_FLOAT:
		return "HALF_FLOAT";
	case PhysicalType::STRING:
		return "ARROW_STRING";
	case PhysicalType::BINARY:
		return "BINARY";
	case PhysicalType::FIXED_SIZE_BINARY:
		return "FIXED_SIZE_BINARY";
	case PhysicalType::DATE32:
		return "DATE32";
	case PhysicalType::DATE64:
		return "DATE64";
	case PhysicalType::TIMESTAMP:
		return "TIMESTAMP";
	case PhysicalType::TIME32:
		return "TIME32";
	case PhysicalType::TIME64:
		return "TIME64";
	case PhysicalType::UNION:
		return "UNION";
	case PhysicalType::DICTIONARY:
		return "DICTIONARY";
	case PhysicalType::MAP:
		return "MAP";
	case PhysicalType::EXTENSION:
		return "EXTENSION";
	case PhysicalType::FIXED_SIZE_LIST:
		return "FIXED_SIZE_LIST";
	case PhysicalType::DURATION:
		return "DURATION";
	case PhysicalType::LARGE_STRING:
		return "LARGE_STRING";
	case PhysicalType::LARGE_BINARY:
		return "LARGE_BINARY";
	case PhysicalType::LARGE_LIST:
		return "LARGE_LIST";
	case PhysicalType::UNKNOWN:
		return "UNKNOWN";
	}
	return "INVALID";
}
// LCOV_EXCL_STOP

idx_t GetTypeIdSize(PhysicalType type) {
	switch (type) {
	case PhysicalType::BIT:
	case PhysicalType::BOOL:
		return sizeof(bool);
	case PhysicalType::INT8:
		return sizeof(int8_t);
	case PhysicalType::INT16:
		return sizeof(int16_t);
	case PhysicalType::INT32:
		return sizeof(int32_t);
	case PhysicalType::INT64:
		return sizeof(int64_t);
	case PhysicalType::UINT8:
		return sizeof(uint8_t);
	case PhysicalType::UINT16:
		return sizeof(uint16_t);
	case PhysicalType::UINT32:
		return sizeof(uint32_t);
	case PhysicalType::UINT64:
		return sizeof(uint64_t);
	case PhysicalType::INT128:
		return sizeof(hugeint_t);
	case PhysicalType::FLOAT:
		return sizeof(float);
	case PhysicalType::DOUBLE:
		return sizeof(double);
	case PhysicalType::VARCHAR:
		return sizeof(string_t);
	case PhysicalType::INTERVAL:
		return sizeof(interval_t);
	case PhysicalType::STRUCT:
	case PhysicalType::UNKNOWN:
		return 0; // no own payload
	case PhysicalType::LIST:
		return sizeof(list_entry_t); // offset + len
	default:
		throw InternalException("Invalid PhysicalType for GetTypeIdSize");
	}
}

bool TypeIsConstantSize(PhysicalType type) {
	return (type >= PhysicalType::BOOL && type <= PhysicalType::DOUBLE) ||
	       (type >= PhysicalType::FIXED_SIZE_BINARY && type <= PhysicalType::INTERVAL) ||
	       type == PhysicalType::INTERVAL || type == PhysicalType::INT128;
}
bool TypeIsIntegral(PhysicalType type) {
	return (type >= PhysicalType::UINT8 && type <= PhysicalType::INT64) || type == PhysicalType::INT128;
}
bool TypeIsNumeric(PhysicalType type) {
	return (type >= PhysicalType::UINT8 && type <= PhysicalType::DOUBLE) || type == PhysicalType::INT128;
}
bool TypeIsInteger(PhysicalType type) {
	return (type >= PhysicalType::UINT8 && type <= PhysicalType::INT64) || type == PhysicalType::INT128;
}

// LCOV_EXCL_START
string LogicalTypeIdToString(LogicalTypeId id) {
	switch (id) {
	case LogicalTypeId::BOOLEAN:
		return "BOOLEAN";
	case LogicalTypeId::TINYINT:
		return "TINYINT";
	case LogicalTypeId::SMALLINT:
		return "SMALLINT";
	case LogicalTypeId::INTEGER:
		return "INTEGER";
	case LogicalTypeId::BIGINT:
		return "BIGINT";
	case LogicalTypeId::HUGEINT:
		return "HUGEINT";
	case LogicalTypeId::UUID:
		return "UUID";
	case LogicalTypeId::UTINYINT:
		return "UTINYINT";
	case LogicalTypeId::USMALLINT:
		return "USMALLINT";
	case LogicalTypeId::UINTEGER:
		return "UINTEGER";
	case LogicalTypeId::UBIGINT:
		return "UBIGINT";
	case LogicalTypeId::DATE:
		return "DATE";
	case LogicalTypeId::TIME:
		return "TIME";
	case LogicalTypeId::TIMESTAMP:
		return "TIMESTAMP";
	case LogicalTypeId::TIMESTAMP_MS:
		return "TIMESTAMP_MS";
	case LogicalTypeId::TIMESTAMP_NS:
		return "TIMESTAMP_NS";
	case LogicalTypeId::TIMESTAMP_SEC:
		return "TIMESTAMP_S";
	case LogicalTypeId::TIMESTAMP_TZ:
		return "TIMESTAMP WITH TIME ZONE";
	case LogicalTypeId::TIME_TZ:
		return "TIME WITH TIME ZONE";
	case LogicalTypeId::FLOAT:
		return "FLOAT";
	case LogicalTypeId::DOUBLE:
		return "DOUBLE";
	case LogicalTypeId::DECIMAL:
		return "DECIMAL";
	case LogicalTypeId::VARCHAR:
		return "VARCHAR";
	case LogicalTypeId::BLOB:
		return "BLOB";
	case LogicalTypeId::CHAR:
		return "CHAR";
	case LogicalTypeId::INTERVAL:
		return "INTERVAL";
	case LogicalTypeId::SQLNULL:
		return "NULL";
	case LogicalTypeId::ANY:
		return "ANY";
	case LogicalTypeId::VALIDITY:
		return "VALIDITY";
	case LogicalTypeId::STRUCT:
		return "STRUCT";
	case LogicalTypeId::LIST:
		return "LIST";
	case LogicalTypeId::MAP:
		return "MAP";
	case LogicalTypeId::POINTER:
		return "POINTER";
	case LogicalTypeId::TABLE:
		return "TABLE";
	case LogicalTypeId::INVALID:
		return "INVALID";
	case LogicalTypeId::UNKNOWN:
		return "UNKNOWN";
	case LogicalTypeId::ENUM:
		return "ENUM";
	case LogicalTypeId::AGGREGATE_STATE:
		return "AGGREGATE_STATE";
	case LogicalTypeId::USER:
		return "USER";
	case LogicalTypeId::JSON:
		return "JSON";
	}
	return "UNDEFINED";
}

string LogicalType::ToString() const {
	auto alias = GetAlias();
	if (!alias.empty()) {
		return alias;
	}
	switch (id_) {
	case LogicalTypeId::STRUCT: {
		if (!type_info_) {
			return "STRUCT";
		}
		auto &child_types = StructType::GetChildTypes(*this);
		string ret = "STRUCT(";
		for (size_t i = 0; i < child_types.size(); i++) {
			ret += child_types[i].first + " " + child_types[i].second.ToString();
			if (i < child_types.size() - 1) {
				ret += ", ";
			}
		}
		ret += ")";
		return ret;
	}
	case LogicalTypeId::LIST: {
		if (!type_info_) {
			return "LIST";
		}
		return ListType::GetChildType(*this).ToString() + "[]";
	}
	case LogicalTypeId::MAP: {
		if (!type_info_) {
			return "MAP";
		}
		auto &child_types = StructType::GetChildTypes(*this);
		if (child_types.empty()) {
			return "MAP(?)";
		}
		if (child_types.size() != 2) {
			throw InternalException("Map needs exactly two child elements");
		}
		return "MAP(" + ListType::GetChildType(child_types[0].second).ToString() + ", " +
		       ListType::GetChildType(child_types[1].second).ToString() + ")";
	}
	case LogicalTypeId::DECIMAL: {
		if (!type_info_) {
			return "DECIMAL";
		}
		auto width = DecimalType::GetWidth(*this);
		auto scale = DecimalType::GetScale(*this);
		if (width == 0) {
			return "DECIMAL";
		}
		return StringUtil::Format("DECIMAL(%d,%d)", width, scale);
	}
	case LogicalTypeId::ENUM: {
		return KeywordHelper::WriteOptionallyQuoted(EnumType::GetTypeName(*this));
	}
	case LogicalTypeId::USER: {
		return KeywordHelper::WriteOptionallyQuoted(UserType::GetTypeName(*this));
	}
	case LogicalTypeId::AGGREGATE_STATE: {
		return AggregateStateType::GetTypeName(*this);
	}
	default:
		return LogicalTypeIdToString(id_);
	}
}
// LCOV_EXCL_STOP

LogicalTypeId TransformStringToLogicalTypeId(const string &str) {
	auto type = DefaultTypeGenerator::GetDefaultType(str);
	if (type == LogicalTypeId::INVALID) {
		// This is a User Type, at this point we don't know if its one of the User Defined Types or an error
		// It is checked in the binder
		type = LogicalTypeId::USER;
	}
	return type;
}

LogicalType TransformStringToLogicalType(const string &str) {
	if (StringUtil::Lower(str) == "null") {
		return LogicalType::SQLNULL;
	}
	return Parser::ParseColumnList("dummy " + str)[0].Type();
}

bool LogicalType::IsIntegral() const {
	switch (id_) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::HUGEINT:
		return true;
	default:
		return false;
	}
}

bool LogicalType::IsNumeric() const {
	switch (id_) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
		return true;
	default:
		return false;
	}
}

bool LogicalType::GetDecimalProperties(uint8_t &width, uint8_t &scale) const {
	switch (id_) {
	case LogicalTypeId::SQLNULL:
		width = 0;
		scale = 0;
		break;
	case LogicalTypeId::BOOLEAN:
		width = 1;
		scale = 0;
		break;
	case LogicalTypeId::TINYINT:
		// tinyint: [-127, 127] = DECIMAL(3,0)
		width = 3;
		scale = 0;
		break;
	case LogicalTypeId::SMALLINT:
		// smallint: [-32767, 32767] = DECIMAL(5,0)
		width = 5;
		scale = 0;
		break;
	case LogicalTypeId::INTEGER:
		// integer: [-2147483647, 2147483647] = DECIMAL(10,0)
		width = 10;
		scale = 0;
		break;
	case LogicalTypeId::BIGINT:
		// bigint: [-9223372036854775807, 9223372036854775807] = DECIMAL(19,0)
		width = 19;
		scale = 0;
		break;
	case LogicalTypeId::UTINYINT:
		// UInt8 — [0 : 255]
		width = 3;
		scale = 0;
		break;
	case LogicalTypeId::USMALLINT:
		// UInt16 — [0 : 65535]
		width = 5;
		scale = 0;
		break;
	case LogicalTypeId::UINTEGER:
		// UInt32 — [0 : 4294967295]
		width = 10;
		scale = 0;
		break;
	case LogicalTypeId::UBIGINT:
		// UInt64 — [0 : 18446744073709551615]
		width = 20;
		scale = 0;
		break;
	case LogicalTypeId::HUGEINT:
		// hugeint: max size decimal (38, 0)
		// note that a hugeint is not guaranteed to fit in this
		width = 38;
		scale = 0;
		break;
	case LogicalTypeId::DECIMAL:
		width = DecimalType::GetWidth(*this);
		scale = DecimalType::GetScale(*this);
		break;
	default:
		return false;
	}
	return true;
}

LogicalType LogicalType::MaxLogicalType(const LogicalType &left, const LogicalType &right) {
	if (left.id() < right.id()) {
		return right;
	} else if (right.id() < left.id()) {
		return left;
	} else {
		// Since both left and right are equal we get the left type as our type_id for checks
		auto type_id = left.id();
		if (type_id == LogicalTypeId::ENUM) {
			// If both types are different ENUMs we do a string comparison.
			return left == right ? left : LogicalType::VARCHAR;
		}
		if (type_id == LogicalTypeId::VARCHAR) {
			// varchar: use type that has collation (if any)
			if (StringType::GetCollation(right).empty()) {
				return left;
			} else {
				return right;
			}
		} else if (type_id == LogicalTypeId::DECIMAL) {
			// unify the width/scale so that the resulting decimal always fits
			// "width - scale" gives us the number of digits on the left side of the decimal point
			// "scale" gives us the number of digits allowed on the right of the deciaml point
			// using the max of these of the two types gives us the new decimal size
			auto extra_width_left = DecimalType::GetWidth(left) - DecimalType::GetScale(left);
			auto extra_width_right = DecimalType::GetWidth(right) - DecimalType::GetScale(right);
			auto extra_width = MaxValue<uint8_t>(extra_width_left, extra_width_right);
			auto scale = MaxValue<uint8_t>(DecimalType::GetScale(left), DecimalType::GetScale(right));
			auto width = extra_width + scale;
			if (width > DecimalType::MaxWidth()) {
				// if the resulting decimal does not fit, we truncate the scale
				width = DecimalType::MaxWidth();
				scale = width - extra_width;
			}
			return LogicalType::DECIMAL(width, scale);
		} else if (type_id == LogicalTypeId::LIST) {
			// list: perform max recursively on child type
			auto new_child = MaxLogicalType(ListType::GetChildType(left), ListType::GetChildType(right));
			return LogicalType::LIST(move(new_child));
		} else if (type_id == LogicalTypeId::STRUCT) {
			// struct: perform recursively
			auto &left_child_types = StructType::GetChildTypes(left);
			auto &right_child_types = StructType::GetChildTypes(right);
			if (left_child_types.size() != right_child_types.size()) {
				// child types are not of equal size, we can't cast anyway
				// just return the left child
				return left;
			}
			child_list_t<LogicalType> child_types;
			for (idx_t i = 0; i < left_child_types.size(); i++) {
				auto child_type = MaxLogicalType(left_child_types[i].second, right_child_types[i].second);
				child_types.push_back(make_pair(left_child_types[i].first, move(child_type)));
			}
			return LogicalType::STRUCT(move(child_types));
		} else {
			// types are equal but no extra specifier: just return the type
			return left;
		}
	}
}

void LogicalType::Verify() const {
#ifdef DEBUG
	if (id_ == LogicalTypeId::DECIMAL) {
		D_ASSERT(DecimalType::GetWidth(*this) >= 1 && DecimalType::GetWidth(*this) <= Decimal::MAX_WIDTH_DECIMAL);
		D_ASSERT(DecimalType::GetScale(*this) >= 0 && DecimalType::GetScale(*this) <= DecimalType::GetWidth(*this));
	}
#endif
}

bool ApproxEqual(float ldecimal, float rdecimal) {
	if (Value::IsNan(ldecimal) && Value::IsNan(rdecimal)) {
		return true;
	}
	if (!Value::FloatIsFinite(ldecimal) || !Value::FloatIsFinite(rdecimal)) {
		return ldecimal == rdecimal;
	}
	float epsilon = std::fabs(rdecimal) * 0.01 + 0.00000001;
	return std::fabs(ldecimal - rdecimal) <= epsilon;
}

bool ApproxEqual(double ldecimal, double rdecimal) {
	if (Value::IsNan(ldecimal) && Value::IsNan(rdecimal)) {
		return true;
	}
	if (!Value::DoubleIsFinite(ldecimal) || !Value::DoubleIsFinite(rdecimal)) {
		return ldecimal == rdecimal;
	}
	double epsilon = std::fabs(rdecimal) * 0.01 + 0.00000001;
	return std::fabs(ldecimal - rdecimal) <= epsilon;
}

//===--------------------------------------------------------------------===//
// Extra Type Info
//===--------------------------------------------------------------------===//
enum class ExtraTypeInfoType : uint8_t {
	INVALID_TYPE_INFO = 0,
	GENERIC_TYPE_INFO = 1,
	DECIMAL_TYPE_INFO = 2,
	STRING_TYPE_INFO = 3,
	LIST_TYPE_INFO = 4,
	STRUCT_TYPE_INFO = 5,
	ENUM_TYPE_INFO = 6,
	USER_TYPE_INFO = 7,
	AGGREGATE_STATE_TYPE_INFO = 8
};

struct ExtraTypeInfo {
	explicit ExtraTypeInfo(ExtraTypeInfoType type) : type(type) {
	}
	explicit ExtraTypeInfo(ExtraTypeInfoType type, string alias) : type(type), alias(move(alias)) {
	}
	virtual ~ExtraTypeInfo() {
	}

	ExtraTypeInfoType type;
	string alias;
	TypeCatalogEntry *catalog_entry = nullptr;

public:
	bool Equals(ExtraTypeInfo *other_p) const {
		if (type == ExtraTypeInfoType::INVALID_TYPE_INFO || type == ExtraTypeInfoType::STRING_TYPE_INFO ||
		    type == ExtraTypeInfoType::GENERIC_TYPE_INFO) {
			if (!other_p) {
				if (!alias.empty()) {
					return false;
				}
				return true;
			}
			if (alias != other_p->alias) {
				return false;
			}
			return true;
		}
		if (!other_p) {
			return false;
		}
		if (type != other_p->type) {
			return false;
		}
		auto &other = (ExtraTypeInfo &)*other_p;
		return alias == other.alias && EqualsInternal(other_p);
	}
	//! Serializes a ExtraTypeInfo to a stand-alone binary blob
	virtual void Serialize(FieldWriter &writer) const {};
	//! Serializes a ExtraTypeInfo to a stand-alone binary blob
	static void Serialize(ExtraTypeInfo *info, FieldWriter &writer);
	//! Deserializes a blob back into an ExtraTypeInfo
	static shared_ptr<ExtraTypeInfo> Deserialize(FieldReader &reader);

protected:
	virtual bool EqualsInternal(ExtraTypeInfo *other_p) const {
		// Do nothing
		return true;
	}
};

void LogicalType::SetAlias(string &alias) {
	if (!type_info_) {
		type_info_ = make_shared<ExtraTypeInfo>(ExtraTypeInfoType::GENERIC_TYPE_INFO, alias);
	} else {
		type_info_->alias = alias;
	}
}

string LogicalType::GetAlias() const {
	if (!type_info_) {
		return string();
	} else {
		return type_info_->alias;
	}
}

void LogicalType::SetCatalog(LogicalType &type, TypeCatalogEntry *catalog_entry) {
	auto info = type.AuxInfo();
	D_ASSERT(info);
	((ExtraTypeInfo &)*info).catalog_entry = catalog_entry;
}
TypeCatalogEntry *LogicalType::GetCatalog(const LogicalType &type) {
	auto info = type.AuxInfo();
	if (!info) {
		return nullptr;
	}
	return ((ExtraTypeInfo &)*info).catalog_entry;
}

//===--------------------------------------------------------------------===//
// Decimal Type
//===--------------------------------------------------------------------===//
struct DecimalTypeInfo : public ExtraTypeInfo {
	DecimalTypeInfo(uint8_t width_p, uint8_t scale_p)
	    : ExtraTypeInfo(ExtraTypeInfoType::DECIMAL_TYPE_INFO), width(width_p), scale(scale_p) {
	}

	uint8_t width;
	uint8_t scale;

public:
	void Serialize(FieldWriter &writer) const override {
		writer.WriteField<uint8_t>(width);
		writer.WriteField<uint8_t>(scale);
	}

	static shared_ptr<ExtraTypeInfo> Deserialize(FieldReader &reader) {
		auto width = reader.ReadRequired<uint8_t>();
		auto scale = reader.ReadRequired<uint8_t>();
		return make_shared<DecimalTypeInfo>(width, scale);
	}

protected:
	bool EqualsInternal(ExtraTypeInfo *other_p) const override {
		auto &other = (DecimalTypeInfo &)*other_p;
		return width == other.width && scale == other.scale;
	}
};

uint8_t DecimalType::GetWidth(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::DECIMAL);
	auto info = type.AuxInfo();
	D_ASSERT(info);
	return ((DecimalTypeInfo &)*info).width;
}

uint8_t DecimalType::GetScale(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::DECIMAL);
	auto info = type.AuxInfo();
	D_ASSERT(info);
	return ((DecimalTypeInfo &)*info).scale;
}

uint8_t DecimalType::MaxWidth() {
	return 38;
}

LogicalType LogicalType::DECIMAL(int width, int scale) {
	auto type_info = make_shared<DecimalTypeInfo>(width, scale);
	return LogicalType(LogicalTypeId::DECIMAL, move(type_info));
}

//===--------------------------------------------------------------------===//
// String Type
//===--------------------------------------------------------------------===//
struct StringTypeInfo : public ExtraTypeInfo {
	explicit StringTypeInfo(string collation_p)
	    : ExtraTypeInfo(ExtraTypeInfoType::STRING_TYPE_INFO), collation(move(collation_p)) {
	}

	string collation;

public:
	void Serialize(FieldWriter &writer) const override {
		writer.WriteString(collation);
	}

	static shared_ptr<ExtraTypeInfo> Deserialize(FieldReader &reader) {
		auto collation = reader.ReadRequired<string>();
		return make_shared<StringTypeInfo>(move(collation));
	}

protected:
	bool EqualsInternal(ExtraTypeInfo *other_p) const override {
		// collation info has no impact on equality
		return true;
	}
};

string StringType::GetCollation(const LogicalType &type) {
	if (type.id() != LogicalTypeId::VARCHAR) {
		return string();
	}
	auto info = type.AuxInfo();
	if (!info) {
		return string();
	}
	if (info->type == ExtraTypeInfoType::GENERIC_TYPE_INFO) {
		return string();
	}
	return ((StringTypeInfo &)*info).collation;
}

LogicalType LogicalType::VARCHAR_COLLATION(string collation) { // NOLINT
	auto string_info = make_shared<StringTypeInfo>(move(collation));
	return LogicalType(LogicalTypeId::VARCHAR, move(string_info));
}

//===--------------------------------------------------------------------===//
// List Type
//===--------------------------------------------------------------------===//
struct ListTypeInfo : public ExtraTypeInfo {
	explicit ListTypeInfo(LogicalType child_type_p)
	    : ExtraTypeInfo(ExtraTypeInfoType::LIST_TYPE_INFO), child_type(move(child_type_p)) {
	}

	LogicalType child_type;

public:
	void Serialize(FieldWriter &writer) const override {
		writer.WriteSerializable(child_type);
	}

	static shared_ptr<ExtraTypeInfo> Deserialize(FieldReader &reader) {
		auto child_type = reader.ReadRequiredSerializable<LogicalType, LogicalType>();
		return make_shared<ListTypeInfo>(move(child_type));
	}

protected:
	bool EqualsInternal(ExtraTypeInfo *other_p) const override {
		auto &other = (ListTypeInfo &)*other_p;
		return child_type == other.child_type;
	}
};

const LogicalType &ListType::GetChildType(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::LIST);
	auto info = type.AuxInfo();
	D_ASSERT(info);
	return ((ListTypeInfo &)*info).child_type;
}

LogicalType LogicalType::LIST(LogicalType child) {
	auto info = make_shared<ListTypeInfo>(move(child));
	return LogicalType(LogicalTypeId::LIST, move(info));
}

//===--------------------------------------------------------------------===//
// Struct Type
//===--------------------------------------------------------------------===//
struct StructTypeInfo : public ExtraTypeInfo {
	explicit StructTypeInfo(child_list_t<LogicalType> child_types_p)
	    : ExtraTypeInfo(ExtraTypeInfoType::STRUCT_TYPE_INFO), child_types(move(child_types_p)) {
	}

	child_list_t<LogicalType> child_types;

public:
	void Serialize(FieldWriter &writer) const override {
		writer.WriteField<uint32_t>(child_types.size());
		auto &serializer = writer.GetSerializer();
		for (idx_t i = 0; i < child_types.size(); i++) {
			serializer.WriteString(child_types[i].first);
			child_types[i].second.Serialize(serializer);
		}
	}

	static shared_ptr<ExtraTypeInfo> Deserialize(FieldReader &reader) {
		child_list_t<LogicalType> child_list;
		auto child_types_size = reader.ReadRequired<uint32_t>();
		auto &source = reader.GetSource();
		for (uint32_t i = 0; i < child_types_size; i++) {
			auto name = source.Read<string>();
			auto type = LogicalType::Deserialize(source);
			child_list.push_back(make_pair(move(name), move(type)));
		}
		return make_shared<StructTypeInfo>(move(child_list));
	}

protected:
	bool EqualsInternal(ExtraTypeInfo *other_p) const override {
		auto &other = (StructTypeInfo &)*other_p;
		return child_types == other.child_types;
	}
};

struct AggregateStateTypeInfo : public ExtraTypeInfo {
	explicit AggregateStateTypeInfo(aggregate_state_t state_type_p)
	    : ExtraTypeInfo(ExtraTypeInfoType::AGGREGATE_STATE_TYPE_INFO), state_type(move(state_type_p)) {
	}

	aggregate_state_t state_type;

public:
	void Serialize(FieldWriter &writer) const override {
		auto &serializer = writer.GetSerializer();
		writer.WriteString(state_type.function_name);
		state_type.return_type.Serialize(serializer);
		writer.WriteField<uint32_t>(state_type.bound_argument_types.size());
		for (idx_t i = 0; i < state_type.bound_argument_types.size(); i++) {
			state_type.bound_argument_types[i].Serialize(serializer);
		}
	}

	static shared_ptr<ExtraTypeInfo> Deserialize(FieldReader &reader) {
		auto &source = reader.GetSource();

		auto function_name = reader.ReadRequired<string>();
		auto return_type = LogicalType::Deserialize(source);
		auto bound_argument_types_size = reader.ReadRequired<uint32_t>();
		vector<LogicalType> bound_argument_types;

		for (uint32_t i = 0; i < bound_argument_types_size; i++) {
			auto type = LogicalType::Deserialize(source);
			bound_argument_types.push_back(move(type));
		}
		return make_shared<AggregateStateTypeInfo>(
		    aggregate_state_t(move(function_name), move(return_type), move(bound_argument_types)));
	}

protected:
	bool EqualsInternal(ExtraTypeInfo *other_p) const override {
		auto &other = (AggregateStateTypeInfo &)*other_p;
		return state_type.function_name == other.state_type.function_name &&
		       state_type.return_type == other.state_type.return_type &&
		       state_type.bound_argument_types == other.state_type.bound_argument_types;
	}
};

const aggregate_state_t &AggregateStateType::GetStateType(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::AGGREGATE_STATE);
	auto info = type.AuxInfo();
	D_ASSERT(info);
	return ((AggregateStateTypeInfo &)*info).state_type;
}

const string AggregateStateType::GetTypeName(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::AGGREGATE_STATE);
	auto info = type.AuxInfo();
	if (!info) {
		return "AGGREGATE_STATE<?>";
	}
	auto aggr_state = ((AggregateStateTypeInfo &)*info).state_type;
	return "AGGREGATE_STATE<" + aggr_state.function_name + "(" +
	       StringUtil::Join(aggr_state.bound_argument_types, aggr_state.bound_argument_types.size(), ", ",
	                        [](const LogicalType &arg_type) { return arg_type.ToString(); }) +
	       ")" + "::" + aggr_state.return_type.ToString() + ">";
}

const child_list_t<LogicalType> &StructType::GetChildTypes(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::STRUCT || type.id() == LogicalTypeId::MAP);
	auto info = type.AuxInfo();
	D_ASSERT(info);
	return ((StructTypeInfo &)*info).child_types;
}

const LogicalType &StructType::GetChildType(const LogicalType &type, idx_t index) {
	auto &child_types = StructType::GetChildTypes(type);
	D_ASSERT(index < child_types.size());
	return child_types[index].second;
}

const string &StructType::GetChildName(const LogicalType &type, idx_t index) {
	auto &child_types = StructType::GetChildTypes(type);
	D_ASSERT(index < child_types.size());
	return child_types[index].first;
}

idx_t StructType::GetChildCount(const LogicalType &type) {
	return StructType::GetChildTypes(type).size();
}

LogicalType LogicalType::STRUCT(child_list_t<LogicalType> children) {
	auto info = make_shared<StructTypeInfo>(move(children));
	return LogicalType(LogicalTypeId::STRUCT, move(info));
}

LogicalType LogicalType::AGGREGATE_STATE(aggregate_state_t state_type) { // NOLINT
	auto info = make_shared<AggregateStateTypeInfo>(move(state_type));
	return LogicalType(LogicalTypeId::AGGREGATE_STATE, move(info));
}

//===--------------------------------------------------------------------===//
// Map Type
//===--------------------------------------------------------------------===//
LogicalType LogicalType::MAP(child_list_t<LogicalType> children) {
	auto info = make_shared<StructTypeInfo>(move(children));
	return LogicalType(LogicalTypeId::MAP, move(info));
}

LogicalType LogicalType::MAP(LogicalType key, LogicalType value) {
	child_list_t<LogicalType> child_types;
	child_types.push_back({"key", LogicalType::LIST(move(key))});
	child_types.push_back({"value", LogicalType::LIST(move(value))});
	return LogicalType::MAP(move(child_types));
}

const LogicalType &MapType::KeyType(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::MAP);
	return ListType::GetChildType(StructType::GetChildTypes(type)[0].second);
}

const LogicalType &MapType::ValueType(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::MAP);
	return ListType::GetChildType(StructType::GetChildTypes(type)[1].second);
}

//===--------------------------------------------------------------------===//
// User Type
//===--------------------------------------------------------------------===//
struct UserTypeInfo : public ExtraTypeInfo {
	explicit UserTypeInfo(string name_p)
	    : ExtraTypeInfo(ExtraTypeInfoType::USER_TYPE_INFO), user_type_name(move(name_p)) {
	}

	string user_type_name;

public:
	void Serialize(FieldWriter &writer) const override {
		writer.WriteString(user_type_name);
	}

	static shared_ptr<ExtraTypeInfo> Deserialize(FieldReader &reader) {
		auto enum_name = reader.ReadRequired<string>();
		return make_shared<UserTypeInfo>(move(enum_name));
	}

protected:
	bool EqualsInternal(ExtraTypeInfo *other_p) const override {
		auto &other = (UserTypeInfo &)*other_p;
		return other.user_type_name == user_type_name;
	}
};

const string &UserType::GetTypeName(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::USER);
	auto info = type.AuxInfo();
	D_ASSERT(info);
	return ((UserTypeInfo &)*info).user_type_name;
}

LogicalType LogicalType::USER(const string &user_type_name) {
	auto info = make_shared<UserTypeInfo>(user_type_name);
	return LogicalType(LogicalTypeId::USER, move(info));
}

//===--------------------------------------------------------------------===//
// Enum Type
//===--------------------------------------------------------------------===//

enum EnumDictType : uint8_t { INVALID = 0, VECTOR_DICT = 1, DEDUP_POINTER = 2 };

struct EnumTypeInfo : public ExtraTypeInfo {
	explicit EnumTypeInfo(string enum_name_p, Vector &values_insert_order_p, idx_t dict_size_p)
	    : ExtraTypeInfo(ExtraTypeInfoType::ENUM_TYPE_INFO), dict_type(EnumDictType::VECTOR_DICT),
	      enum_name(move(enum_name_p)), values_insert_order(values_insert_order_p), dict_size(dict_size_p) {
	}
	explicit EnumTypeInfo()
	    : ExtraTypeInfo(ExtraTypeInfoType::ENUM_TYPE_INFO), dict_type(EnumDictType::DEDUP_POINTER),
	      enum_name("dedup_pointer"), values_insert_order(Vector(LogicalType::VARCHAR)), dict_size(0) {
	}
	EnumDictType dict_type;
	string enum_name;
	Vector values_insert_order;
	idx_t dict_size;

protected:
	// Equalities are only used in enums with different catalog entries
	bool EqualsInternal(ExtraTypeInfo *other_p) const override {
		auto &other = (EnumTypeInfo &)*other_p;
		if (dict_type != other.dict_type) {
			return false;
		}
		if (dict_type == EnumDictType::DEDUP_POINTER) {
			return true;
		}
		D_ASSERT(dict_type == EnumDictType::VECTOR_DICT);
		// We must check if both enums have the same size
		if (other.dict_size != dict_size) {
			return false;
		}
		auto other_vector_ptr = FlatVector::GetData<string_t>(other.values_insert_order);
		auto this_vector_ptr = FlatVector::GetData<string_t>(values_insert_order);

		// Now we must check if all strings are the same
		for (idx_t i = 0; i < dict_size; i++) {
			if (!Equals::Operation(other_vector_ptr[i], this_vector_ptr[i])) {
				return false;
			}
		}
		return true;
	}

	void Serialize(FieldWriter &writer) const override {
		if (dict_type != EnumDictType::VECTOR_DICT) {
			throw InternalException("Cannot serialize non-vector dictionary ENUM types");
		}
		writer.WriteField<uint32_t>(dict_size);
		writer.WriteString(enum_name);
		((Vector &)values_insert_order).Serialize(dict_size, writer.GetSerializer());
	}
};

template <class T>
struct EnumTypeInfoTemplated : public EnumTypeInfo {
	explicit EnumTypeInfoTemplated(const string &enum_name_p, Vector &values_insert_order_p, idx_t size_p)
	    : EnumTypeInfo(enum_name_p, values_insert_order_p, size_p) {
		for (idx_t count = 0; count < size_p; count++) {
			values[values_insert_order_p.GetValue(count).ToString()] = count;
		}
	}

	static shared_ptr<EnumTypeInfoTemplated> Deserialize(FieldReader &reader, uint32_t size) {
		auto enum_name = reader.ReadRequired<string>();
		Vector values_insert_order(LogicalType::VARCHAR, size);
		values_insert_order.Deserialize(size, reader.GetSource());
		return make_shared<EnumTypeInfoTemplated>(move(enum_name), values_insert_order, size);
	}
	unordered_map<string, T> values;
};

const string &EnumType::GetTypeName(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::ENUM);
	auto info = type.AuxInfo();
	D_ASSERT(info);
	return ((EnumTypeInfo &)*info).enum_name;
}

static PhysicalType EnumVectorDictType(idx_t size) {
	if (size <= NumericLimits<uint8_t>::Maximum()) {
		return PhysicalType::UINT8;
	} else if (size <= NumericLimits<uint16_t>::Maximum()) {
		return PhysicalType::UINT16;
	} else if (size <= NumericLimits<uint32_t>::Maximum()) {
		return PhysicalType::UINT32;
	} else {
		throw InternalException("Enum size must be lower than " + std::to_string(NumericLimits<uint32_t>::Maximum()));
	}
}

LogicalType LogicalType::ENUM(const string &enum_name, Vector &ordered_data, idx_t size) {
	// Generate EnumTypeInfo
	shared_ptr<ExtraTypeInfo> info;
	auto enum_internal_type = EnumVectorDictType(size);
	switch (enum_internal_type) {
	case PhysicalType::UINT8:
		info = make_shared<EnumTypeInfoTemplated<uint8_t>>(enum_name, ordered_data, size);
		break;
	case PhysicalType::UINT16:
		info = make_shared<EnumTypeInfoTemplated<uint16_t>>(enum_name, ordered_data, size);
		break;
	case PhysicalType::UINT32:
		info = make_shared<EnumTypeInfoTemplated<uint32_t>>(enum_name, ordered_data, size);
		break;
	default:
		throw InternalException("Invalid Physical Type for ENUMs");
	}
	// Generate Actual Enum Type
	return LogicalType(LogicalTypeId::ENUM, info);
}

LogicalType LogicalType::DEDUP_POINTER_ENUM() { // NOLINT
	auto info = make_shared<EnumTypeInfo>();
	D_ASSERT(info->dict_type == EnumDictType::DEDUP_POINTER);
	return LogicalType(LogicalTypeId::ENUM, info);
}

template <class T>
int64_t TemplatedGetPos(unordered_map<string, T> &map, const string &key) {
	auto it = map.find(key);
	if (it == map.end()) {
		return -1;
	}
	return it->second;
}
int64_t EnumType::GetPos(const LogicalType &type, const string &key) {
	auto info = type.AuxInfo();
	switch (type.InternalType()) {
	case PhysicalType::UINT8:
		return TemplatedGetPos(((EnumTypeInfoTemplated<uint8_t> &)*info).values, key);
	case PhysicalType::UINT16:
		return TemplatedGetPos(((EnumTypeInfoTemplated<uint16_t> &)*info).values, key);
	case PhysicalType::UINT32:
		return TemplatedGetPos(((EnumTypeInfoTemplated<uint32_t> &)*info).values, key);
	default:
		throw InternalException("ENUM can only have unsigned integers (except UINT64) as physical types");
	}
}

const string EnumType::GetValue(const Value &val) {
	auto info = val.type().AuxInfo();
	auto &enum_info = ((EnumTypeInfo &)*info);
	if (enum_info.dict_type == EnumDictType::DEDUP_POINTER) {
		return (const char *)val.GetValue<uint64_t>();
	}
	auto &values_insert_order = ((EnumTypeInfo &)*info).values_insert_order;
	return StringValue::Get(values_insert_order.GetValue(val.GetValue<uint32_t>()));
}

Vector &EnumType::GetValuesInsertOrder(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::ENUM);
	auto info = type.AuxInfo();
	D_ASSERT(info);
	return ((EnumTypeInfo &)*info).values_insert_order;
}

idx_t EnumType::GetSize(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::ENUM);
	auto info = type.AuxInfo();
	D_ASSERT(info);
	return ((EnumTypeInfo &)*info).dict_size;
}

void EnumType::SetCatalog(LogicalType &type, TypeCatalogEntry *catalog_entry) {
	D_ASSERT(type.id() == LogicalTypeId::ENUM);
	auto info = type.AuxInfo();
	D_ASSERT(info);
	((EnumTypeInfo &)*info).catalog_entry = catalog_entry;
}
TypeCatalogEntry *EnumType::GetCatalog(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::ENUM);
	auto info = type.AuxInfo();
	D_ASSERT(info);
	return ((EnumTypeInfo &)*info).catalog_entry;
}

PhysicalType EnumType::GetPhysicalType(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::ENUM);
	auto aux_info = type.AuxInfo();
	D_ASSERT(aux_info);
	auto &info = (EnumTypeInfo &)*aux_info;

	if (info.dict_type == EnumDictType::DEDUP_POINTER) {
		return PhysicalType::UINT64; // for pointer enum types
	}
	D_ASSERT(info.dict_type == EnumDictType::VECTOR_DICT);
	return EnumVectorDictType(info.dict_size);
}

//===--------------------------------------------------------------------===//
// Extra Type Info
//===--------------------------------------------------------------------===//
void ExtraTypeInfo::Serialize(ExtraTypeInfo *info, FieldWriter &writer) {
	if (!info) {
		writer.WriteField<ExtraTypeInfoType>(ExtraTypeInfoType::INVALID_TYPE_INFO);
		writer.WriteString(string());
	} else {
		writer.WriteField<ExtraTypeInfoType>(info->type);
		info->Serialize(writer);
		writer.WriteString(info->alias);
	}
}
shared_ptr<ExtraTypeInfo> ExtraTypeInfo::Deserialize(FieldReader &reader) {
	auto type = reader.ReadRequired<ExtraTypeInfoType>();
	shared_ptr<ExtraTypeInfo> extra_info;
	switch (type) {
	case ExtraTypeInfoType::INVALID_TYPE_INFO: {
		auto alias = reader.ReadField<string>(string());
		if (!alias.empty()) {
			return make_shared<ExtraTypeInfo>(type, alias);
		}
		return nullptr;
	} break;
	case ExtraTypeInfoType::GENERIC_TYPE_INFO: {
		extra_info = make_shared<ExtraTypeInfo>(type);
	} break;
	case ExtraTypeInfoType::DECIMAL_TYPE_INFO:
		extra_info = DecimalTypeInfo::Deserialize(reader);
		break;
	case ExtraTypeInfoType::STRING_TYPE_INFO:
		extra_info = StringTypeInfo::Deserialize(reader);
		break;
	case ExtraTypeInfoType::LIST_TYPE_INFO:
		extra_info = ListTypeInfo::Deserialize(reader);
		break;
	case ExtraTypeInfoType::STRUCT_TYPE_INFO:
		extra_info = StructTypeInfo::Deserialize(reader);
		break;
	case ExtraTypeInfoType::USER_TYPE_INFO:
		extra_info = UserTypeInfo::Deserialize(reader);
		break;
	case ExtraTypeInfoType::ENUM_TYPE_INFO: {
		auto enum_size = reader.ReadRequired<uint32_t>();
		auto enum_internal_type = EnumVectorDictType(enum_size);
		switch (enum_internal_type) {
		case PhysicalType::UINT8:
			extra_info = EnumTypeInfoTemplated<uint8_t>::Deserialize(reader, enum_size);
			break;
		case PhysicalType::UINT16:
			extra_info = EnumTypeInfoTemplated<uint16_t>::Deserialize(reader, enum_size);
			break;
		case PhysicalType::UINT32:
			extra_info = EnumTypeInfoTemplated<uint32_t>::Deserialize(reader, enum_size);
			break;
		default:
			throw InternalException("Invalid Physical Type for ENUMs");
		}
	} break;
	case ExtraTypeInfoType::AGGREGATE_STATE_TYPE_INFO:
		extra_info = AggregateStateTypeInfo::Deserialize(reader);
		break;

	default:
		throw InternalException("Unimplemented type info in ExtraTypeInfo::Deserialize");
	}
	auto alias = reader.ReadField<string>(string());
	extra_info->alias = alias;
	return extra_info;
}

//===--------------------------------------------------------------------===//
// Logical Type
//===--------------------------------------------------------------------===//

// the destructor needs to know about the extra type info
LogicalType::~LogicalType() {
}

void LogicalType::Serialize(Serializer &serializer) const {
	FieldWriter writer(serializer);
	writer.WriteField<LogicalTypeId>(id_);
	ExtraTypeInfo::Serialize(type_info_.get(), writer);
	writer.Finalize();
}

LogicalType LogicalType::Deserialize(Deserializer &source) {
	FieldReader reader(source);
	auto id = reader.ReadRequired<LogicalTypeId>();
	auto info = ExtraTypeInfo::Deserialize(reader);
	reader.Finalize();

	return LogicalType(id, move(info));
}

bool LogicalType::operator==(const LogicalType &rhs) const {
	if (id_ != rhs.id_) {
		return false;
	}
	if (type_info_.get() == rhs.type_info_.get()) {
		return true;
	}
	if (type_info_) {
		return type_info_->Equals(rhs.type_info_.get());
	} else {
		D_ASSERT(rhs.type_info_);
		return rhs.type_info_->Equals(type_info_.get());
	}
}

} // namespace duckdb





namespace duckdb {

//===--------------------------------------------------------------------===//
// Comparison Operations
//===--------------------------------------------------------------------===//

struct ValuePositionComparator {
	// Return true if the positional Values definitely match.
	// Default to the same as the final value
	template <typename OP>
	static inline bool Definite(const Value &lhs, const Value &rhs) {
		return Final<OP>(lhs, rhs);
	}

	// Select the positional Values that need further testing.
	// Usually this means Is Not Distinct, as those are the semantics used by Postges
	template <typename OP>
	static inline bool Possible(const Value &lhs, const Value &rhs) {
		return ValueOperations::NotDistinctFrom(lhs, rhs);
	}

	// Return true if the positional Values definitely match in the final position
	// This needs to be specialised.
	template <typename OP>
	static inline bool Final(const Value &lhs, const Value &rhs) {
		return false;
	}

	// Tie-break based on length when one of the sides has been exhausted, returning true if the LHS matches.
	// This essentially means that the existing positions compare equal.
	// Default to the same semantics as the OP for idx_t. This works in most cases.
	template <typename OP>
	static inline bool TieBreak(const idx_t lpos, const idx_t rpos) {
		return OP::Operation(lpos, rpos);
	}
};

// Equals must always check every column
template <>
inline bool ValuePositionComparator::Definite<duckdb::Equals>(const Value &lhs, const Value &rhs) {
	return false;
}

template <>
inline bool ValuePositionComparator::Final<duckdb::Equals>(const Value &lhs, const Value &rhs) {
	return ValueOperations::NotDistinctFrom(lhs, rhs);
}

// NotEquals must check everything that matched
template <>
inline bool ValuePositionComparator::Possible<duckdb::NotEquals>(const Value &lhs, const Value &rhs) {
	return true;
}

template <>
inline bool ValuePositionComparator::Final<duckdb::NotEquals>(const Value &lhs, const Value &rhs) {
	return ValueOperations::NotDistinctFrom(lhs, rhs);
}

// Non-strict inequalities must use strict comparisons for Definite
template <>
bool ValuePositionComparator::Definite<duckdb::LessThanEquals>(const Value &lhs, const Value &rhs) {
	return ValueOperations::DistinctLessThan(lhs, rhs);
}

template <>
bool ValuePositionComparator::Final<duckdb::LessThanEquals>(const Value &lhs, const Value &rhs) {
	return ValueOperations::DistinctLessThanEquals(lhs, rhs);
}

template <>
bool ValuePositionComparator::Definite<duckdb::GreaterThanEquals>(const Value &lhs, const Value &rhs) {
	return ValueOperations::DistinctGreaterThan(lhs, rhs);
}

template <>
bool ValuePositionComparator::Final<duckdb::GreaterThanEquals>(const Value &lhs, const Value &rhs) {
	return ValueOperations::DistinctGreaterThanEquals(lhs, rhs);
}

// Strict inequalities just use strict for both Definite and Final
template <>
bool ValuePositionComparator::Final<duckdb::LessThan>(const Value &lhs, const Value &rhs) {
	return ValueOperations::DistinctLessThan(lhs, rhs);
}

template <>
bool ValuePositionComparator::Final<duckdb::GreaterThan>(const Value &lhs, const Value &rhs) {
	return ValueOperations::DistinctGreaterThan(lhs, rhs);
}

template <class OP>
static bool TemplatedBooleanOperation(const Value &left, const Value &right) {
	const auto &left_type = left.type();
	const auto &right_type = right.type();
	if (left_type != right_type) {
		Value left_copy = left;
		Value right_copy = right;

		LogicalType comparison_type = BoundComparisonExpression::BindComparison(left_type, right_type);
		if (!left_copy.TryCastAs(comparison_type) || !right_copy.TryCastAs(comparison_type)) {
			return false;
		}
		D_ASSERT(left_copy.type() == right_copy.type());
		return TemplatedBooleanOperation<OP>(left_copy, right_copy);
	}
	switch (left_type.InternalType()) {
	case PhysicalType::BOOL:
		return OP::Operation(left.GetValueUnsafe<bool>(), right.GetValueUnsafe<bool>());
	case PhysicalType::INT8:
		return OP::Operation(left.GetValueUnsafe<int8_t>(), right.GetValueUnsafe<int8_t>());
	case PhysicalType::INT16:
		return OP::Operation(left.GetValueUnsafe<int16_t>(), right.GetValueUnsafe<int16_t>());
	case PhysicalType::INT32:
		return OP::Operation(left.GetValueUnsafe<int32_t>(), right.GetValueUnsafe<int32_t>());
	case PhysicalType::INT64:
		return OP::Operation(left.GetValueUnsafe<int64_t>(), right.GetValueUnsafe<int64_t>());
	case PhysicalType::UINT8:
		return OP::Operation(left.GetValueUnsafe<uint8_t>(), right.GetValueUnsafe<uint8_t>());
	case PhysicalType::UINT16:
		return OP::Operation(left.GetValueUnsafe<uint16_t>(), right.GetValueUnsafe<uint16_t>());
	case PhysicalType::UINT32:
		return OP::Operation(left.GetValueUnsafe<uint32_t>(), right.GetValueUnsafe<uint32_t>());
	case PhysicalType::UINT64:
		return OP::Operation(left.GetValueUnsafe<uint64_t>(), right.GetValueUnsafe<uint64_t>());
	case PhysicalType::INT128:
		return OP::Operation(left.GetValueUnsafe<hugeint_t>(), right.GetValueUnsafe<hugeint_t>());
	case PhysicalType::FLOAT:
		return OP::Operation(left.GetValueUnsafe<float>(), right.GetValueUnsafe<float>());
	case PhysicalType::DOUBLE:
		return OP::Operation(left.GetValueUnsafe<double>(), right.GetValueUnsafe<double>());
	case PhysicalType::INTERVAL:
		return OP::Operation(left.GetValueUnsafe<interval_t>(), right.GetValueUnsafe<interval_t>());
	case PhysicalType::VARCHAR:
		return OP::Operation(StringValue::Get(left), StringValue::Get(right));
	case PhysicalType::STRUCT: {
		auto &left_children = StructValue::GetChildren(left);
		auto &right_children = StructValue::GetChildren(right);
		// this should be enforced by the type
		D_ASSERT(left_children.size() == right_children.size());
		idx_t i = 0;
		for (; i < left_children.size() - 1; ++i) {
			if (ValuePositionComparator::Definite<OP>(left_children[i], right_children[i])) {
				return true;
			}
			if (!ValuePositionComparator::Possible<OP>(left_children[i], right_children[i])) {
				return false;
			}
		}
		return ValuePositionComparator::Final<OP>(left_children[i], right_children[i]);
	}
	case PhysicalType::LIST: {
		auto &left_children = ListValue::GetChildren(left);
		auto &right_children = ListValue::GetChildren(right);
		for (idx_t pos = 0;; ++pos) {
			if (pos == left_children.size() || pos == right_children.size()) {
				return ValuePositionComparator::TieBreak<OP>(left_children.size(), right_children.size());
			}
			if (ValuePositionComparator::Definite<OP>(left_children[pos], right_children[pos])) {
				return true;
			}
			if (!ValuePositionComparator::Possible<OP>(left_children[pos], right_children[pos])) {
				return false;
			}
		}
		return false;
	}
	default:
		throw InternalException("Unimplemented type for value comparison");
	}
}

bool ValueOperations::Equals(const Value &left, const Value &right) {
	if (left.IsNull() || right.IsNull()) {
		throw InternalException("Comparison on NULL values");
	}
	return TemplatedBooleanOperation<duckdb::Equals>(left, right);
}

bool ValueOperations::NotEquals(const Value &left, const Value &right) {
	return !ValueOperations::Equals(left, right);
}

bool ValueOperations::GreaterThan(const Value &left, const Value &right) {
	if (left.IsNull() || right.IsNull()) {
		throw InternalException("Comparison on NULL values");
	}
	return TemplatedBooleanOperation<duckdb::GreaterThan>(left, right);
}

bool ValueOperations::GreaterThanEquals(const Value &left, const Value &right) {
	if (left.IsNull() || right.IsNull()) {
		throw InternalException("Comparison on NULL values");
	}
	return TemplatedBooleanOperation<duckdb::GreaterThanEquals>(left, right);
}

bool ValueOperations::LessThan(const Value &left, const Value &right) {
	return ValueOperations::GreaterThan(right, left);
}

bool ValueOperations::LessThanEquals(const Value &left, const Value &right) {
	return ValueOperations::GreaterThanEquals(right, left);
}

bool ValueOperations::NotDistinctFrom(const Value &left, const Value &right) {
	if (left.IsNull() && right.IsNull()) {
		return true;
	}
	if (left.IsNull() != right.IsNull()) {
		return false;
	}
	return TemplatedBooleanOperation<duckdb::Equals>(left, right);
}

bool ValueOperations::DistinctFrom(const Value &left, const Value &right) {
	return !ValueOperations::NotDistinctFrom(left, right);
}

bool ValueOperations::DistinctGreaterThan(const Value &left, const Value &right) {
	if (left.IsNull() && right.IsNull()) {
		return false;
	} else if (right.IsNull()) {
		return false;
	} else if (left.IsNull()) {
		return true;
	}
	return TemplatedBooleanOperation<duckdb::GreaterThan>(left, right);
}

bool ValueOperations::DistinctGreaterThanEquals(const Value &left, const Value &right) {
	if (left.IsNull()) {
		return true;
	} else if (right.IsNull()) {
		return false;
	}
	return TemplatedBooleanOperation<duckdb::GreaterThanEquals>(left, right);
}

bool ValueOperations::DistinctLessThan(const Value &left, const Value &right) {
	return ValueOperations::DistinctGreaterThan(right, left);
}

bool ValueOperations::DistinctLessThanEquals(const Value &left, const Value &right) {
	return ValueOperations::DistinctGreaterThanEquals(right, left);
}

} // namespace duckdb
//===--------------------------------------------------------------------===//
// boolean_operators.cpp
// Description: This file contains the implementation of the boolean
// operations AND OR !
//===--------------------------------------------------------------------===//





namespace duckdb {

//===--------------------------------------------------------------------===//
// AND/OR
//===--------------------------------------------------------------------===//
template <class OP>
static void TemplatedBooleanNullmask(Vector &left, Vector &right, Vector &result, idx_t count) {
	D_ASSERT(left.GetType().id() == LogicalTypeId::BOOLEAN && right.GetType().id() == LogicalTypeId::BOOLEAN &&
	         result.GetType().id() == LogicalTypeId::BOOLEAN);

	if (left.GetVectorType() == VectorType::CONSTANT_VECTOR && right.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		// operation on two constants, result is constant vector
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		auto ldata = ConstantVector::GetData<uint8_t>(left);
		auto rdata = ConstantVector::GetData<uint8_t>(right);
		auto result_data = ConstantVector::GetData<bool>(result);

		bool is_null = OP::Operation(*ldata > 0, *rdata > 0, ConstantVector::IsNull(left),
		                             ConstantVector::IsNull(right), *result_data);
		ConstantVector::SetNull(result, is_null);
	} else {
		// perform generic loop
		VectorData ldata, rdata;
		left.Orrify(count, ldata);
		right.Orrify(count, rdata);

		result.SetVectorType(VectorType::FLAT_VECTOR);
		auto left_data = (uint8_t *)ldata.data; // we use uint8 to avoid load of gunk bools
		auto right_data = (uint8_t *)rdata.data;
		auto result_data = FlatVector::GetData<bool>(result);
		auto &result_mask = FlatVector::Validity(result);
		if (!ldata.validity.AllValid() || !rdata.validity.AllValid()) {
			for (idx_t i = 0; i < count; i++) {
				auto lidx = ldata.sel->get_index(i);
				auto ridx = rdata.sel->get_index(i);
				bool is_null =
				    OP::Operation(left_data[lidx] > 0, right_data[ridx] > 0, !ldata.validity.RowIsValid(lidx),
				                  !rdata.validity.RowIsValid(ridx), result_data[i]);
				result_mask.Set(i, !is_null);
			}
		} else {
			for (idx_t i = 0; i < count; i++) {
				auto lidx = ldata.sel->get_index(i);
				auto ridx = rdata.sel->get_index(i);
				result_data[i] = OP::SimpleOperation(left_data[lidx], right_data[ridx]);
			}
		}
	}
}

/*
SQL AND Rules:

TRUE  AND TRUE   = TRUE
TRUE  AND FALSE  = FALSE
TRUE  AND NULL   = NULL
FALSE AND TRUE   = FALSE
FALSE AND FALSE  = FALSE
FALSE AND NULL   = FALSE
NULL  AND TRUE   = NULL
NULL  AND FALSE  = FALSE
NULL  AND NULL   = NULL

Basically:
- Only true if both are true
- False if either is false (regardless of NULLs)
- NULL otherwise
*/
struct TernaryAnd {
	static bool SimpleOperation(bool left, bool right) {
		return left && right;
	}
	static bool Operation(bool left, bool right, bool left_null, bool right_null, bool &result) {
		if (left_null && right_null) {
			// both NULL:
			// result is NULL
			return true;
		} else if (left_null) {
			// left is NULL:
			// result is FALSE if right is false
			// result is NULL if right is true
			result = right;
			return right;
		} else if (right_null) {
			// right is NULL:
			// result is FALSE if left is false
			// result is NULL if left is true
			result = left;
			return left;
		} else {
			// no NULL: perform the AND
			result = left && right;
			return false;
		}
	}
};

void VectorOperations::And(Vector &left, Vector &right, Vector &result, idx_t count) {
	TemplatedBooleanNullmask<TernaryAnd>(left, right, result, count);
}

/*
SQL OR Rules:

OR
TRUE  OR TRUE  = TRUE
TRUE  OR FALSE = TRUE
TRUE  OR NULL  = TRUE
FALSE OR TRUE  = TRUE
FALSE OR FALSE = FALSE
FALSE OR NULL  = NULL
NULL  OR TRUE  = TRUE
NULL  OR FALSE = NULL
NULL  OR NULL  = NULL

Basically:
- Only false if both are false
- True if either is true (regardless of NULLs)
- NULL otherwise
*/

struct TernaryOr {
	static bool SimpleOperation(bool left, bool right) {
		return left || right;
	}
	static bool Operation(bool left, bool right, bool left_null, bool right_null, bool &result) {
		if (left_null && right_null) {
			// both NULL:
			// result is NULL
			return true;
		} else if (left_null) {
			// left is NULL:
			// result is TRUE if right is true
			// result is NULL if right is false
			result = right;
			return !right;
		} else if (right_null) {
			// right is NULL:
			// result is TRUE if left is true
			// result is NULL if left is false
			result = left;
			return !left;
		} else {
			// no NULL: perform the OR
			result = left || right;
			return false;
		}
	}
};

void VectorOperations::Or(Vector &left, Vector &right, Vector &result, idx_t count) {
	TemplatedBooleanNullmask<TernaryOr>(left, right, result, count);
}

struct NotOperator {
	template <class TA, class TR>
	static inline TR Operation(TA left) {
		return !left;
	}
};

void VectorOperations::Not(Vector &input, Vector &result, idx_t count) {
	D_ASSERT(input.GetType() == LogicalType::BOOLEAN && result.GetType() == LogicalType::BOOLEAN);
	UnaryExecutor::Execute<bool, bool, NotOperator>(input, result, count);
}

} // namespace duckdb
//===--------------------------------------------------------------------===//
// comparison_operators.cpp
// Description: This file contains the implementation of the comparison
// operations == != >= <= > <
//===--------------------------------------------------------------------===//








namespace duckdb {

template <class T>
bool EqualsFloat(T left, T right) {
	if (DUCKDB_UNLIKELY(Value::IsNan(left) && Value::IsNan(right))) {
		return true;
	}
	return left == right;
}

template <>
bool Equals::Operation(float left, float right) {
	return EqualsFloat<float>(left, right);
}

template <>
bool Equals::Operation(double left, double right) {
	return EqualsFloat<double>(left, right);
}

template <class T>
bool GreaterThanFloat(T left, T right) {
	// handle nans
	// nan is always bigger than everything else
	bool left_is_nan = Value::IsNan(left);
	bool right_is_nan = Value::IsNan(right);
	// if right is nan, there is no number that is bigger than right
	if (DUCKDB_UNLIKELY(right_is_nan)) {
		return false;
	}
	// if left is nan, but right is not, left is always bigger
	if (DUCKDB_UNLIKELY(left_is_nan)) {
		return true;
	}
	return left > right;
}

template <>
bool GreaterThan::Operation(float left, float right) {
	return GreaterThanFloat<float>(left, right);
}

template <>
bool GreaterThan::Operation(double left, double right) {
	return GreaterThanFloat<double>(left, right);
}

template <class T>
bool GreaterThanEqualsFloat(T left, T right) {
	// handle nans
	// nan is always bigger than everything else
	bool left_is_nan = Value::IsNan(left);
	bool right_is_nan = Value::IsNan(right);
	// if right is nan, there is no bigger number
	// we only return true if left is also nan (in which case the numbers are equal)
	if (DUCKDB_UNLIKELY(right_is_nan)) {
		return left_is_nan;
	}
	// if left is nan, but right is not, left is always bigger
	if (DUCKDB_UNLIKELY(left_is_nan)) {
		return true;
	}
	return left >= right;
}

template <>
bool GreaterThanEquals::Operation(float left, float right) {
	return GreaterThanEqualsFloat<float>(left, right);
}

template <>
bool GreaterThanEquals::Operation(double left, double right) {
	return GreaterThanEqualsFloat<double>(left, right);
}

struct ComparisonSelector {
	template <typename OP>
	static idx_t Select(Vector &left, Vector &right, const SelectionVector *sel, idx_t count, SelectionVector *true_sel,
	                    SelectionVector *false_sel) {
		throw NotImplementedException("Unknown comparison operation!");
	}
};

template <>
inline idx_t ComparisonSelector::Select<duckdb::Equals>(Vector &left, Vector &right, const SelectionVector *sel,
                                                        idx_t count, SelectionVector *true_sel,
                                                        SelectionVector *false_sel) {
	return VectorOperations::Equals(left, right, sel, count, true_sel, false_sel);
}

template <>
inline idx_t ComparisonSelector::Select<duckdb::NotEquals>(Vector &left, Vector &right, const SelectionVector *sel,
                                                           idx_t count, SelectionVector *true_sel,
                                                           SelectionVector *false_sel) {
	return VectorOperations::NotEquals(left, right, sel, count, true_sel, false_sel);
}

template <>
inline idx_t ComparisonSelector::Select<duckdb::GreaterThan>(Vector &left, Vector &right, const SelectionVector *sel,
                                                             idx_t count, SelectionVector *true_sel,
                                                             SelectionVector *false_sel) {
	return VectorOperations::GreaterThan(left, right, sel, count, true_sel, false_sel);
}

template <>
inline idx_t ComparisonSelector::Select<duckdb::GreaterThanEquals>(Vector &left, Vector &right,
                                                                   const SelectionVector *sel, idx_t count,
                                                                   SelectionVector *true_sel,
                                                                   SelectionVector *false_sel) {
	return VectorOperations::GreaterThanEquals(left, right, sel, count, true_sel, false_sel);
}

template <>
inline idx_t ComparisonSelector::Select<duckdb::LessThan>(Vector &left, Vector &right, const SelectionVector *sel,
                                                          idx_t count, SelectionVector *true_sel,
                                                          SelectionVector *false_sel) {
	return VectorOperations::LessThan(left, right, sel, count, true_sel, false_sel);
}

template <>
inline idx_t ComparisonSelector::Select<duckdb::LessThanEquals>(Vector &left, Vector &right, const SelectionVector *sel,
                                                                idx_t count, SelectionVector *true_sel,
                                                                SelectionVector *false_sel) {
	return VectorOperations::LessThanEquals(left, right, sel, count, true_sel, false_sel);
}

static void ComparesNotNull(VectorData &ldata, VectorData &rdata, ValidityMask &vresult, idx_t count) {
	for (idx_t i = 0; i < count; ++i) {
		auto lidx = ldata.sel->get_index(i);
		auto ridx = rdata.sel->get_index(i);
		if (!ldata.validity.RowIsValid(lidx) || !rdata.validity.RowIsValid(ridx)) {
			vresult.SetInvalid(i);
		}
	}
}

template <typename OP>
static void NestedComparisonExecutor(Vector &left, Vector &right, Vector &result, idx_t count) {
	const auto left_constant = left.GetVectorType() == VectorType::CONSTANT_VECTOR;
	const auto right_constant = right.GetVectorType() == VectorType::CONSTANT_VECTOR;

	if ((left_constant && ConstantVector::IsNull(left)) || (right_constant && ConstantVector::IsNull(right))) {
		// either left or right is constant NULL: result is constant NULL
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(result, true);
		return;
	}

	if (left_constant && right_constant) {
		// both sides are constant, and neither is NULL so just compare one element.
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		SelectionVector true_sel(1);
		auto match_count = ComparisonSelector::Select<OP>(left, right, nullptr, 1, &true_sel, nullptr);
		auto result_data = ConstantVector::GetData<bool>(result);
		result_data[0] = match_count > 0;
		return;
	}

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<bool>(result);
	auto &result_validity = FlatVector::Validity(result);

	VectorData leftv, rightv;
	left.Orrify(count, leftv);
	right.Orrify(count, rightv);
	if (!leftv.validity.AllValid() || !rightv.validity.AllValid()) {
		ComparesNotNull(leftv, rightv, result_validity, count);
	}
	SelectionVector true_sel(count);
	SelectionVector false_sel(count);
	idx_t match_count = ComparisonSelector::Select<OP>(left, right, nullptr, count, &true_sel, &false_sel);

	for (idx_t i = 0; i < match_count; ++i) {
		const auto idx = true_sel.get_index(i);
		result_data[idx] = true;
	}

	const idx_t no_match_count = count - match_count;
	for (idx_t i = 0; i < no_match_count; ++i) {
		const auto idx = false_sel.get_index(i);
		result_data[idx] = false;
	}
}

struct ComparisonExecutor {
private:
	template <class T, class OP>
	static inline void TemplatedExecute(Vector &left, Vector &right, Vector &result, idx_t count) {
		BinaryExecutor::Execute<T, T, bool, OP>(left, right, result, count);
	}

public:
	template <class OP>
	static inline void Execute(Vector &left, Vector &right, Vector &result, idx_t count) {
		D_ASSERT(left.GetType() == right.GetType() && result.GetType() == LogicalType::BOOLEAN);
		// the inplace loops take the result as the last parameter
		switch (left.GetType().InternalType()) {
		case PhysicalType::BOOL:
		case PhysicalType::INT8:
			TemplatedExecute<int8_t, OP>(left, right, result, count);
			break;
		case PhysicalType::INT16:
			TemplatedExecute<int16_t, OP>(left, right, result, count);
			break;
		case PhysicalType::INT32:
			TemplatedExecute<int32_t, OP>(left, right, result, count);
			break;
		case PhysicalType::INT64:
			TemplatedExecute<int64_t, OP>(left, right, result, count);
			break;
		case PhysicalType::UINT8:
			TemplatedExecute<uint8_t, OP>(left, right, result, count);
			break;
		case PhysicalType::UINT16:
			TemplatedExecute<uint16_t, OP>(left, right, result, count);
			break;
		case PhysicalType::UINT32:
			TemplatedExecute<uint32_t, OP>(left, right, result, count);
			break;
		case PhysicalType::UINT64:
			TemplatedExecute<uint64_t, OP>(left, right, result, count);
			break;
		case PhysicalType::INT128:
			TemplatedExecute<hugeint_t, OP>(left, right, result, count);
			break;
		case PhysicalType::FLOAT:
			TemplatedExecute<float, OP>(left, right, result, count);
			break;
		case PhysicalType::DOUBLE:
			TemplatedExecute<double, OP>(left, right, result, count);
			break;
		case PhysicalType::INTERVAL:
			TemplatedExecute<interval_t, OP>(left, right, result, count);
			break;
		case PhysicalType::VARCHAR:
			TemplatedExecute<string_t, OP>(left, right, result, count);
			break;
		case PhysicalType::LIST:
		case PhysicalType::MAP:
		case PhysicalType::STRUCT:
			NestedComparisonExecutor<OP>(left, right, result, count);
			break;
		default:
			throw InternalException("Invalid type for comparison");
		}
	}
};

void VectorOperations::Equals(Vector &left, Vector &right, Vector &result, idx_t count) {
	ComparisonExecutor::Execute<duckdb::Equals>(left, right, result, count);
}

void VectorOperations::NotEquals(Vector &left, Vector &right, Vector &result, idx_t count) {
	ComparisonExecutor::Execute<duckdb::NotEquals>(left, right, result, count);
}

void VectorOperations::GreaterThanEquals(Vector &left, Vector &right, Vector &result, idx_t count) {
	ComparisonExecutor::Execute<duckdb::GreaterThanEquals>(left, right, result, count);
}

void VectorOperations::LessThanEquals(Vector &left, Vector &right, Vector &result, idx_t count) {
	ComparisonExecutor::Execute<duckdb::LessThanEquals>(left, right, result, count);
}

void VectorOperations::GreaterThan(Vector &left, Vector &right, Vector &result, idx_t count) {
	ComparisonExecutor::Execute<duckdb::GreaterThan>(left, right, result, count);
}

void VectorOperations::LessThan(Vector &left, Vector &right, Vector &result, idx_t count) {
	ComparisonExecutor::Execute<duckdb::LessThan>(left, right, result, count);
}

} // namespace duckdb
//===--------------------------------------------------------------------===//
// generators.cpp
// Description: This file contains the implementation of different generators
//===--------------------------------------------------------------------===//





namespace duckdb {

template <class T>
void TemplatedGenerateSequence(Vector &result, idx_t count, int64_t start, int64_t increment) {
	D_ASSERT(result.GetType().IsNumeric());
	if (start > NumericLimits<T>::Maximum() || increment > NumericLimits<T>::Maximum()) {
		throw Exception("Sequence start or increment out of type range");
	}
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<T>(result);
	auto value = (T)start;
	for (idx_t i = 0; i < count; i++) {
		if (i > 0) {
			value += increment;
		}
		result_data[i] = value;
	}
}

void VectorOperations::GenerateSequence(Vector &result, idx_t count, int64_t start, int64_t increment) {
	if (!result.GetType().IsNumeric()) {
		throw InvalidTypeException(result.GetType(), "Can only generate sequences for numeric values!");
	}
	switch (result.GetType().InternalType()) {
	case PhysicalType::INT8:
		TemplatedGenerateSequence<int8_t>(result, count, start, increment);
		break;
	case PhysicalType::INT16:
		TemplatedGenerateSequence<int16_t>(result, count, start, increment);
		break;
	case PhysicalType::INT32:
		TemplatedGenerateSequence<int32_t>(result, count, start, increment);
		break;
	case PhysicalType::INT64:
		TemplatedGenerateSequence<int64_t>(result, count, start, increment);
		break;
	case PhysicalType::FLOAT:
		TemplatedGenerateSequence<float>(result, count, start, increment);
		break;
	case PhysicalType::DOUBLE:
		TemplatedGenerateSequence<double>(result, count, start, increment);
		break;
	default:
		throw NotImplementedException("Unimplemented type for generate sequence");
	}
}

template <class T>
void TemplatedGenerateSequence(Vector &result, idx_t count, const SelectionVector &sel, int64_t start,
                               int64_t increment) {
	D_ASSERT(result.GetType().IsNumeric());
	if (start > NumericLimits<T>::Maximum() || increment > NumericLimits<T>::Maximum()) {
		throw Exception("Sequence start or increment out of type range");
	}
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<T>(result);
	auto value = (T)start;
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		result_data[idx] = value + increment * idx;
	}
}

void VectorOperations::GenerateSequence(Vector &result, idx_t count, const SelectionVector &sel, int64_t start,
                                        int64_t increment) {
	if (!result.GetType().IsNumeric()) {
		throw InvalidTypeException(result.GetType(), "Can only generate sequences for numeric values!");
	}
	switch (result.GetType().InternalType()) {
	case PhysicalType::INT8:
		TemplatedGenerateSequence<int8_t>(result, count, sel, start, increment);
		break;
	case PhysicalType::INT16:
		TemplatedGenerateSequence<int16_t>(result, count, sel, start, increment);
		break;
	case PhysicalType::INT32:
		TemplatedGenerateSequence<int32_t>(result, count, sel, start, increment);
		break;
	case PhysicalType::INT64:
		TemplatedGenerateSequence<int64_t>(result, count, sel, start, increment);
		break;
	case PhysicalType::FLOAT:
		TemplatedGenerateSequence<float>(result, count, sel, start, increment);
		break;
	case PhysicalType::DOUBLE:
		TemplatedGenerateSequence<double>(result, count, sel, start, increment);
		break;
	default:
		throw NotImplementedException("Unimplemented type for generate sequence");
	}
}

} // namespace duckdb



namespace duckdb {

struct DistinctBinaryLambdaWrapper {
	template <class OP, class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE>
	static inline RESULT_TYPE Operation(LEFT_TYPE left, RIGHT_TYPE right, bool is_left_null, bool is_right_null) {
		return OP::template Operation<LEFT_TYPE>(left, right, is_left_null, is_right_null);
	}
};

template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OP>
static void DistinctExecuteGenericLoop(LEFT_TYPE *__restrict ldata, RIGHT_TYPE *__restrict rdata,
                                       RESULT_TYPE *__restrict result_data, const SelectionVector *__restrict lsel,
                                       const SelectionVector *__restrict rsel, idx_t count, ValidityMask &lmask,
                                       ValidityMask &rmask, ValidityMask &result_mask) {
	for (idx_t i = 0; i < count; i++) {
		auto lindex = lsel->get_index(i);
		auto rindex = rsel->get_index(i);
		auto lentry = ldata[lindex];
		auto rentry = rdata[rindex];
		result_data[i] =
		    OP::template Operation<LEFT_TYPE>(lentry, rentry, !lmask.RowIsValid(lindex), !rmask.RowIsValid(rindex));
	}
}

template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OP>
static void DistinctExecuteConstant(Vector &left, Vector &right, Vector &result) {
	result.SetVectorType(VectorType::CONSTANT_VECTOR);

	auto ldata = ConstantVector::GetData<LEFT_TYPE>(left);
	auto rdata = ConstantVector::GetData<RIGHT_TYPE>(right);
	auto result_data = ConstantVector::GetData<RESULT_TYPE>(result);
	*result_data =
	    OP::template Operation<LEFT_TYPE>(*ldata, *rdata, ConstantVector::IsNull(left), ConstantVector::IsNull(right));
}

template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OP>
static void DistinctExecuteGeneric(Vector &left, Vector &right, Vector &result, idx_t count) {
	if (left.GetVectorType() == VectorType::CONSTANT_VECTOR && right.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		DistinctExecuteConstant<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OP>(left, right, result);
	} else {
		VectorData ldata, rdata;

		left.Orrify(count, ldata);
		right.Orrify(count, rdata);

		result.SetVectorType(VectorType::FLAT_VECTOR);
		auto result_data = FlatVector::GetData<RESULT_TYPE>(result);
		DistinctExecuteGenericLoop<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OP>(
		    (LEFT_TYPE *)ldata.data, (RIGHT_TYPE *)rdata.data, result_data, ldata.sel, rdata.sel, count, ldata.validity,
		    rdata.validity, FlatVector::Validity(result));
	}
}

template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OP>
static void DistinctExecuteSwitch(Vector &left, Vector &right, Vector &result, idx_t count) {
	DistinctExecuteGeneric<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OP>(left, right, result, count);
}

template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OP>
static void DistinctExecute(Vector &left, Vector &right, Vector &result, idx_t count) {
	DistinctExecuteSwitch<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OP>(left, right, result, count);
}

template <class LEFT_TYPE, class RIGHT_TYPE, class OP, bool NO_NULL, bool HAS_TRUE_SEL, bool HAS_FALSE_SEL>
static inline idx_t
DistinctSelectGenericLoop(LEFT_TYPE *__restrict ldata, RIGHT_TYPE *__restrict rdata,
                          const SelectionVector *__restrict lsel, const SelectionVector *__restrict rsel,
                          const SelectionVector *__restrict result_sel, idx_t count, ValidityMask &lmask,
                          ValidityMask &rmask, SelectionVector *true_sel, SelectionVector *false_sel) {
	idx_t true_count = 0, false_count = 0;
	for (idx_t i = 0; i < count; i++) {
		auto result_idx = result_sel->get_index(i);
		auto lindex = lsel->get_index(i);
		auto rindex = rsel->get_index(i);
		if (NO_NULL) {
			if (OP::Operation(ldata[lindex], rdata[rindex], false, false)) {
				if (HAS_TRUE_SEL) {
					true_sel->set_index(true_count++, result_idx);
				}
			} else {
				if (HAS_FALSE_SEL) {
					false_sel->set_index(false_count++, result_idx);
				}
			}
		} else {
			if (OP::Operation(ldata[lindex], rdata[rindex], !lmask.RowIsValid(lindex), !rmask.RowIsValid(rindex))) {
				if (HAS_TRUE_SEL) {
					true_sel->set_index(true_count++, result_idx);
				}
			} else {
				if (HAS_FALSE_SEL) {
					false_sel->set_index(false_count++, result_idx);
				}
			}
		}
	}
	if (HAS_TRUE_SEL) {
		return true_count;
	} else {
		return count - false_count;
	}
}
template <class LEFT_TYPE, class RIGHT_TYPE, class OP, bool NO_NULL>
static inline idx_t
DistinctSelectGenericLoopSelSwitch(LEFT_TYPE *__restrict ldata, RIGHT_TYPE *__restrict rdata,
                                   const SelectionVector *__restrict lsel, const SelectionVector *__restrict rsel,
                                   const SelectionVector *__restrict result_sel, idx_t count, ValidityMask &lmask,
                                   ValidityMask &rmask, SelectionVector *true_sel, SelectionVector *false_sel) {
	if (true_sel && false_sel) {
		return DistinctSelectGenericLoop<LEFT_TYPE, RIGHT_TYPE, OP, NO_NULL, true, true>(
		    ldata, rdata, lsel, rsel, result_sel, count, lmask, rmask, true_sel, false_sel);
	} else if (true_sel) {
		return DistinctSelectGenericLoop<LEFT_TYPE, RIGHT_TYPE, OP, NO_NULL, true, false>(
		    ldata, rdata, lsel, rsel, result_sel, count, lmask, rmask, true_sel, false_sel);
	} else {
		D_ASSERT(false_sel);
		return DistinctSelectGenericLoop<LEFT_TYPE, RIGHT_TYPE, OP, NO_NULL, false, true>(
		    ldata, rdata, lsel, rsel, result_sel, count, lmask, rmask, true_sel, false_sel);
	}
}

template <class LEFT_TYPE, class RIGHT_TYPE, class OP>
static inline idx_t
DistinctSelectGenericLoopSwitch(LEFT_TYPE *__restrict ldata, RIGHT_TYPE *__restrict rdata,
                                const SelectionVector *__restrict lsel, const SelectionVector *__restrict rsel,
                                const SelectionVector *__restrict result_sel, idx_t count, ValidityMask &lmask,
                                ValidityMask &rmask, SelectionVector *true_sel, SelectionVector *false_sel) {
	if (!lmask.AllValid() || !rmask.AllValid()) {
		return DistinctSelectGenericLoopSelSwitch<LEFT_TYPE, RIGHT_TYPE, OP, false>(
		    ldata, rdata, lsel, rsel, result_sel, count, lmask, rmask, true_sel, false_sel);
	} else {
		return DistinctSelectGenericLoopSelSwitch<LEFT_TYPE, RIGHT_TYPE, OP, true>(
		    ldata, rdata, lsel, rsel, result_sel, count, lmask, rmask, true_sel, false_sel);
	}
}

template <class LEFT_TYPE, class RIGHT_TYPE, class OP>
static idx_t DistinctSelectGeneric(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                   SelectionVector *true_sel, SelectionVector *false_sel) {
	VectorData ldata, rdata;

	left.Orrify(count, ldata);
	right.Orrify(count, rdata);

	return DistinctSelectGenericLoopSwitch<LEFT_TYPE, RIGHT_TYPE, OP>((LEFT_TYPE *)ldata.data, (RIGHT_TYPE *)rdata.data,
	                                                                  ldata.sel, rdata.sel, sel, count, ldata.validity,
	                                                                  rdata.validity, true_sel, false_sel);
}
template <class LEFT_TYPE, class RIGHT_TYPE, class OP, bool LEFT_CONSTANT, bool RIGHT_CONSTANT, bool NO_NULL,
          bool HAS_TRUE_SEL, bool HAS_FALSE_SEL>
static inline idx_t DistinctSelectFlatLoop(LEFT_TYPE *__restrict ldata, RIGHT_TYPE *__restrict rdata,
                                           const SelectionVector *sel, idx_t count, ValidityMask &lmask,
                                           ValidityMask &rmask, SelectionVector *true_sel, SelectionVector *false_sel) {
	idx_t true_count = 0, false_count = 0;
	for (idx_t i = 0; i < count; i++) {
		idx_t result_idx = sel->get_index(i);
		idx_t lidx = LEFT_CONSTANT ? 0 : i;
		idx_t ridx = RIGHT_CONSTANT ? 0 : i;
		const bool lnull = !lmask.RowIsValid(lidx);
		const bool rnull = !rmask.RowIsValid(ridx);
		bool comparison_result = OP::Operation(ldata[lidx], rdata[ridx], lnull, rnull);
		if (HAS_TRUE_SEL) {
			true_sel->set_index(true_count, result_idx);
			true_count += comparison_result;
		}
		if (HAS_FALSE_SEL) {
			false_sel->set_index(false_count, result_idx);
			false_count += !comparison_result;
		}
	}
	if (HAS_TRUE_SEL) {
		return true_count;
	} else {
		return count - false_count;
	}
}

template <class LEFT_TYPE, class RIGHT_TYPE, class OP, bool LEFT_CONSTANT, bool RIGHT_CONSTANT, bool NO_NULL>
static inline idx_t DistinctSelectFlatLoopSelSwitch(LEFT_TYPE *__restrict ldata, RIGHT_TYPE *__restrict rdata,
                                                    const SelectionVector *sel, idx_t count, ValidityMask &lmask,
                                                    ValidityMask &rmask, SelectionVector *true_sel,
                                                    SelectionVector *false_sel) {
	if (true_sel && false_sel) {
		return DistinctSelectFlatLoop<LEFT_TYPE, RIGHT_TYPE, OP, LEFT_CONSTANT, RIGHT_CONSTANT, NO_NULL, true, true>(
		    ldata, rdata, sel, count, lmask, rmask, true_sel, false_sel);
	} else if (true_sel) {
		return DistinctSelectFlatLoop<LEFT_TYPE, RIGHT_TYPE, OP, LEFT_CONSTANT, RIGHT_CONSTANT, NO_NULL, true, false>(
		    ldata, rdata, sel, count, lmask, rmask, true_sel, false_sel);
	} else {
		D_ASSERT(false_sel);
		return DistinctSelectFlatLoop<LEFT_TYPE, RIGHT_TYPE, OP, LEFT_CONSTANT, RIGHT_CONSTANT, NO_NULL, false, true>(
		    ldata, rdata, sel, count, lmask, rmask, true_sel, false_sel);
	}
}

template <class LEFT_TYPE, class RIGHT_TYPE, class OP, bool LEFT_CONSTANT, bool RIGHT_CONSTANT>
static inline idx_t DistinctSelectFlatLoopSwitch(LEFT_TYPE *__restrict ldata, RIGHT_TYPE *__restrict rdata,
                                                 const SelectionVector *sel, idx_t count, ValidityMask &lmask,
                                                 ValidityMask &rmask, SelectionVector *true_sel,
                                                 SelectionVector *false_sel) {
	return DistinctSelectFlatLoopSelSwitch<LEFT_TYPE, RIGHT_TYPE, OP, LEFT_CONSTANT, RIGHT_CONSTANT, true>(
	    ldata, rdata, sel, count, lmask, rmask, true_sel, false_sel);
}
template <class LEFT_TYPE, class RIGHT_TYPE, class OP, bool LEFT_CONSTANT, bool RIGHT_CONSTANT>
static idx_t DistinctSelectFlat(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                SelectionVector *true_sel, SelectionVector *false_sel) {
	auto ldata = FlatVector::GetData<LEFT_TYPE>(left);
	auto rdata = FlatVector::GetData<RIGHT_TYPE>(right);
	if (LEFT_CONSTANT) {
		ValidityMask validity;
		if (ConstantVector::IsNull(left)) {
			validity.SetAllInvalid(1);
		}
		return DistinctSelectFlatLoopSwitch<LEFT_TYPE, RIGHT_TYPE, OP, LEFT_CONSTANT, RIGHT_CONSTANT>(
		    ldata, rdata, sel, count, validity, FlatVector::Validity(right), true_sel, false_sel);
	} else if (RIGHT_CONSTANT) {
		ValidityMask validity;
		if (ConstantVector::IsNull(right)) {
			validity.SetAllInvalid(1);
		}
		return DistinctSelectFlatLoopSwitch<LEFT_TYPE, RIGHT_TYPE, OP, LEFT_CONSTANT, RIGHT_CONSTANT>(
		    ldata, rdata, sel, count, FlatVector::Validity(left), validity, true_sel, false_sel);
	} else {
		return DistinctSelectFlatLoopSwitch<LEFT_TYPE, RIGHT_TYPE, OP, LEFT_CONSTANT, RIGHT_CONSTANT>(
		    ldata, rdata, sel, count, FlatVector::Validity(left), FlatVector::Validity(right), true_sel, false_sel);
	}
}
template <class LEFT_TYPE, class RIGHT_TYPE, class OP>
static idx_t DistinctSelectConstant(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                    SelectionVector *true_sel, SelectionVector *false_sel) {
	auto ldata = ConstantVector::GetData<LEFT_TYPE>(left);
	auto rdata = ConstantVector::GetData<RIGHT_TYPE>(right);

	// both sides are constant, return either 0 or the count
	// in this case we do not fill in the result selection vector at all
	if (!OP::Operation(*ldata, *rdata, ConstantVector::IsNull(left), ConstantVector::IsNull(right))) {
		if (false_sel) {
			for (idx_t i = 0; i < count; i++) {
				false_sel->set_index(i, sel->get_index(i));
			}
		}
		return 0;
	} else {
		if (true_sel) {
			for (idx_t i = 0; i < count; i++) {
				true_sel->set_index(i, sel->get_index(i));
			}
		}
		return count;
	}
}

template <class LEFT_TYPE, class RIGHT_TYPE, class OP>
static idx_t DistinctSelect(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                            SelectionVector *true_sel, SelectionVector *false_sel) {
	if (!sel) {
		sel = FlatVector::IncrementalSelectionVector();
	}
	if (left.GetVectorType() == VectorType::CONSTANT_VECTOR && right.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		return DistinctSelectConstant<LEFT_TYPE, RIGHT_TYPE, OP>(left, right, sel, count, true_sel, false_sel);
	} else if (left.GetVectorType() == VectorType::CONSTANT_VECTOR &&
	           right.GetVectorType() == VectorType::FLAT_VECTOR) {
		return DistinctSelectFlat<LEFT_TYPE, RIGHT_TYPE, OP, true, false>(left, right, sel, count, true_sel, false_sel);
	} else if (left.GetVectorType() == VectorType::FLAT_VECTOR &&
	           right.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		return DistinctSelectFlat<LEFT_TYPE, RIGHT_TYPE, OP, false, true>(left, right, sel, count, true_sel, false_sel);
	} else if (left.GetVectorType() == VectorType::FLAT_VECTOR && right.GetVectorType() == VectorType::FLAT_VECTOR) {
		return DistinctSelectFlat<LEFT_TYPE, RIGHT_TYPE, OP, false, false>(left, right, sel, count, true_sel,
		                                                                   false_sel);
	} else {
		return DistinctSelectGeneric<LEFT_TYPE, RIGHT_TYPE, OP>(left, right, sel, count, true_sel, false_sel);
	}
}

template <class OP>
static idx_t DistinctSelectNotNull(Vector &left, Vector &right, const idx_t count, idx_t &true_count,
                                   const SelectionVector &sel, SelectionVector &maybe_vec, OptionalSelection &true_opt,
                                   OptionalSelection &false_opt) {
	VectorData lvdata, rvdata;
	left.Orrify(count, lvdata);
	right.Orrify(count, rvdata);

	auto &lmask = lvdata.validity;
	auto &rmask = rvdata.validity;

	idx_t remaining = 0;
	if (lmask.AllValid() && rmask.AllValid()) {
		//	None are NULL, distinguish values.
		for (idx_t i = 0; i < count; ++i) {
			const auto idx = sel.get_index(i);
			maybe_vec.set_index(remaining++, idx);
		}
		return remaining;
	}

	// Slice the Vectors down to the rows that are not determined (i.e., neither is NULL)
	SelectionVector slicer(count);
	true_count = 0;
	idx_t false_count = 0;
	for (idx_t i = 0; i < count; ++i) {
		const auto result_idx = sel.get_index(i);
		const auto lidx = lvdata.sel->get_index(i);
		const auto ridx = rvdata.sel->get_index(i);
		const auto lnull = !lmask.RowIsValid(lidx);
		const auto rnull = !rmask.RowIsValid(ridx);
		if (lnull || rnull) {
			// If either is NULL then we can major distinguish them
			if (!OP::Operation(false, false, lnull, rnull)) {
				false_opt.Append(false_count, result_idx);
			} else {
				true_opt.Append(true_count, result_idx);
			}
		} else {
			//	Neither is NULL, distinguish values.
			slicer.set_index(remaining, i);
			maybe_vec.set_index(remaining++, result_idx);
		}
	}

	true_opt.Advance(true_count);
	false_opt.Advance(false_count);

	if (remaining && remaining < count) {
		left.Slice(slicer, remaining);
		right.Slice(slicer, remaining);
	}

	return remaining;
}

struct PositionComparator {
	// Select the rows that definitely match.
	// Default to the same as the final row
	template <typename OP>
	static idx_t Definite(Vector &left, Vector &right, const SelectionVector &sel, idx_t count,
	                      SelectionVector *true_sel, SelectionVector &false_sel) {
		return Final<OP>(left, right, sel, count, true_sel, &false_sel);
	}

	// Select the possible rows that need further testing.
	// Usually this means Is Not Distinct, as those are the semantics used by Postges
	template <typename OP>
	static idx_t Possible(Vector &left, Vector &right, const SelectionVector &sel, idx_t count,
	                      SelectionVector &true_sel, SelectionVector *false_sel) {
		return VectorOperations::NestedEquals(left, right, sel, count, &true_sel, false_sel);
	}

	// Select the matching rows for the final position.
	// This needs to be specialised.
	template <typename OP>
	static idx_t Final(Vector &left, Vector &right, const SelectionVector &sel, idx_t count, SelectionVector *true_sel,
	                   SelectionVector *false_sel) {
		return 0;
	}

	// Tie-break based on length when one of the sides has been exhausted, returning true if the LHS matches.
	// This essentially means that the existing positions compare equal.
	// Default to the same semantics as the OP for idx_t. This works in most cases.
	template <typename OP>
	static bool TieBreak(const idx_t lpos, const idx_t rpos) {
		return OP::Operation(lpos, rpos, false, false);
	}
};

// NotDistinctFrom must always check every column
template <>
idx_t PositionComparator::Definite<duckdb::NotDistinctFrom>(Vector &left, Vector &right, const SelectionVector &sel,
                                                            idx_t count, SelectionVector *true_sel,
                                                            SelectionVector &false_sel) {
	return 0;
}

template <>
idx_t PositionComparator::Final<duckdb::NotDistinctFrom>(Vector &left, Vector &right, const SelectionVector &sel,
                                                         idx_t count, SelectionVector *true_sel,
                                                         SelectionVector *false_sel) {
	return VectorOperations::NestedEquals(left, right, sel, count, true_sel, false_sel);
}

// DistinctFrom must check everything that matched
template <>
idx_t PositionComparator::Possible<duckdb::DistinctFrom>(Vector &left, Vector &right, const SelectionVector &sel,
                                                         idx_t count, SelectionVector &true_sel,
                                                         SelectionVector *false_sel) {
	return count;
}

template <>
idx_t PositionComparator::Final<duckdb::DistinctFrom>(Vector &left, Vector &right, const SelectionVector &sel,
                                                      idx_t count, SelectionVector *true_sel,
                                                      SelectionVector *false_sel) {
	return VectorOperations::NestedNotEquals(left, right, sel, count, true_sel, false_sel);
}

// Non-strict inequalities must use strict comparisons for Definite
template <>
idx_t PositionComparator::Definite<duckdb::DistinctLessThanEquals>(Vector &left, Vector &right,
                                                                   const SelectionVector &sel, idx_t count,
                                                                   SelectionVector *true_sel,
                                                                   SelectionVector &false_sel) {
	return VectorOperations::DistinctLessThan(left, right, &sel, count, true_sel, &false_sel);
}

template <>
idx_t PositionComparator::Final<duckdb::DistinctLessThanEquals>(Vector &left, Vector &right, const SelectionVector &sel,
                                                                idx_t count, SelectionVector *true_sel,
                                                                SelectionVector *false_sel) {
	return VectorOperations::DistinctLessThanEquals(left, right, &sel, count, true_sel, false_sel);
}

template <>
idx_t PositionComparator::Definite<duckdb::DistinctGreaterThanEquals>(Vector &left, Vector &right,
                                                                      const SelectionVector &sel, idx_t count,
                                                                      SelectionVector *true_sel,
                                                                      SelectionVector &false_sel) {
	return VectorOperations::DistinctGreaterThan(left, right, &sel, count, true_sel, &false_sel);
}

template <>
idx_t PositionComparator::Final<duckdb::DistinctGreaterThanEquals>(Vector &left, Vector &right,
                                                                   const SelectionVector &sel, idx_t count,
                                                                   SelectionVector *true_sel,
                                                                   SelectionVector *false_sel) {
	return VectorOperations::DistinctGreaterThanEquals(left, right, &sel, count, true_sel, false_sel);
}

// Strict inequalities just use strict for both Definite and Final
template <>
idx_t PositionComparator::Final<duckdb::DistinctLessThan>(Vector &left, Vector &right, const SelectionVector &sel,
                                                          idx_t count, SelectionVector *true_sel,
                                                          SelectionVector *false_sel) {
	return VectorOperations::DistinctLessThan(left, right, &sel, count, true_sel, false_sel);
}

template <>
idx_t PositionComparator::Final<duckdb::DistinctGreaterThan>(Vector &left, Vector &right, const SelectionVector &sel,
                                                             idx_t count, SelectionVector *true_sel,
                                                             SelectionVector *false_sel) {
	return VectorOperations::DistinctGreaterThan(left, right, &sel, count, true_sel, false_sel);
}

using StructEntries = vector<unique_ptr<Vector>>;

static void ExtractNestedSelection(const SelectionVector &slice_sel, const idx_t count, const SelectionVector &sel,
                                   OptionalSelection &opt) {

	for (idx_t i = 0; i < count;) {
		const auto slice_idx = slice_sel.get_index(i);
		const auto result_idx = sel.get_index(slice_idx);
		opt.Append(i, result_idx);
	}
	opt.Advance(count);
}

static void DensifyNestedSelection(const SelectionVector &dense_sel, const idx_t count, SelectionVector &slice_sel) {
	for (idx_t i = 0; i < count; ++i) {
		slice_sel.set_index(i, dense_sel.get_index(i));
	}
}

template <class OP>
static idx_t DistinctSelectStruct(Vector &left, Vector &right, idx_t count, const SelectionVector &sel,
                                  OptionalSelection &true_opt, OptionalSelection &false_opt) {
	if (count == 0) {
		return 0;
	}

	// Avoid allocating in the 99% of the cases where we don't need to.
	StructEntries lsliced, rsliced;
	auto &lchildren = StructVector::GetEntries(left);
	auto &rchildren = StructVector::GetEntries(right);
	D_ASSERT(lchildren.size() == rchildren.size());

	// In order to reuse the comparators, we have to track what passed and failed internally.
	// To do that, we need local SVs that we then merge back into the real ones after every pass.
	const auto vcount = count;
	SelectionVector slice_sel(count);
	for (idx_t i = 0; i < count; ++i) {
		slice_sel.set_index(i, i);
	}

	SelectionVector true_sel(count);
	SelectionVector false_sel(count);

	idx_t match_count = 0;
	for (idx_t col_no = 0; col_no < lchildren.size(); ++col_no) {
		// Slice the children to maintain density
		Vector lchild(*lchildren[col_no]);
		lchild.Normalify(vcount);
		lchild.Slice(slice_sel, count);

		Vector rchild(*rchildren[col_no]);
		rchild.Normalify(vcount);
		rchild.Slice(slice_sel, count);

		// Find everything that definitely matches
		auto true_count = PositionComparator::Definite<OP>(lchild, rchild, slice_sel, count, &true_sel, false_sel);
		if (true_count > 0) {
			auto false_count = count - true_count;

			// Extract the definite matches into the true result
			ExtractNestedSelection(false_count ? true_sel : slice_sel, true_count, sel, true_opt);

			// Remove the definite matches from the slicing vector
			DensifyNestedSelection(false_sel, false_count, slice_sel);

			match_count += true_count;
			count -= true_count;
		}

		if (col_no != lchildren.size() - 1) {
			// Find what might match on the next position
			true_count = PositionComparator::Possible<OP>(lchild, rchild, slice_sel, count, true_sel, &false_sel);
			auto false_count = count - true_count;

			// Extract the definite failures into the false result
			ExtractNestedSelection(true_count ? false_sel : slice_sel, false_count, sel, false_opt);

			// Remove any definite failures from the slicing vector
			if (false_count) {
				DensifyNestedSelection(true_sel, true_count, slice_sel);
			}

			count = true_count;
		} else {
			true_count = PositionComparator::Final<OP>(lchild, rchild, slice_sel, count, &true_sel, &false_sel);
			auto false_count = count - true_count;

			// Extract the definite matches into the true result
			ExtractNestedSelection(false_count ? true_sel : slice_sel, true_count, sel, true_opt);

			// Extract the definite failures into the false result
			ExtractNestedSelection(true_count ? false_sel : slice_sel, false_count, sel, false_opt);

			match_count += true_count;
		}
	}
	return match_count;
}

static void PositionListCursor(SelectionVector &cursor, VectorData &vdata, const idx_t pos,
                               const SelectionVector &slice_sel, const idx_t count) {
	const auto data = (const list_entry_t *)vdata.data;
	for (idx_t i = 0; i < count; ++i) {
		const auto slice_idx = slice_sel.get_index(i);

		const auto lidx = vdata.sel->get_index(slice_idx);
		const auto &entry = data[lidx];
		cursor.set_index(i, entry.offset + pos);
	}
}

template <class OP>
static idx_t DistinctSelectList(Vector &left, Vector &right, idx_t count, const SelectionVector &sel,
                                OptionalSelection &true_opt, OptionalSelection &false_opt) {
	if (count == 0) {
		return count;
	}

	// Create dictionary views of the children so we can vectorise the positional comparisons.
	SelectionVector lcursor(count);
	SelectionVector rcursor(count);

	ListVector::GetEntry(left).Normalify(count);
	ListVector::GetEntry(right).Normalify(count);
	Vector lchild(ListVector::GetEntry(left), lcursor, count);
	Vector rchild(ListVector::GetEntry(right), rcursor, count);

	// To perform the positional comparison, we use a vectorisation of the following algorithm:
	// bool CompareLists(T *left, idx_t nleft, T *right, nright) {
	// 	for (idx_t pos = 0; ; ++pos) {
	// 		if (nleft == pos || nright == pos)
	// 			return OP::TieBreak(nleft, nright);
	// 		if (OP::Definite(*left, *right))
	// 			return true;
	// 		if (!OP::Maybe(*left, *right))
	// 			return false;
	// 		}
	//	 	++left, ++right;
	// 	}
	// }

	// Get pointers to the list entries
	VectorData lvdata;
	left.Orrify(count, lvdata);
	const auto ldata = (const list_entry_t *)lvdata.data;

	VectorData rvdata;
	right.Orrify(count, rvdata);
	const auto rdata = (const list_entry_t *)rvdata.data;

	// In order to reuse the comparators, we have to track what passed and failed internally.
	// To do that, we need local SVs that we then merge back into the real ones after every pass.
	SelectionVector slice_sel(count);
	for (idx_t i = 0; i < count; ++i) {
		slice_sel.set_index(i, i);
	}

	SelectionVector true_sel(count);
	SelectionVector false_sel(count);

	idx_t match_count = 0;
	for (idx_t pos = 0; count > 0; ++pos) {
		// Set up the cursors for the current position
		PositionListCursor(lcursor, lvdata, pos, slice_sel, count);
		PositionListCursor(rcursor, rvdata, pos, slice_sel, count);

		// Tie-break the pairs where one of the LISTs is exhausted.
		idx_t true_count = 0;
		idx_t false_count = 0;
		idx_t maybe_count = 0;
		for (idx_t i = 0; i < count; ++i) {
			const auto slice_idx = slice_sel.get_index(i);
			const auto lidx = lvdata.sel->get_index(slice_idx);
			const auto &lentry = ldata[lidx];
			const auto ridx = rvdata.sel->get_index(slice_idx);
			const auto &rentry = rdata[ridx];
			if (lentry.length == pos || rentry.length == pos) {
				const auto idx = sel.get_index(slice_idx);
				if (PositionComparator::TieBreak<OP>(lentry.length, rentry.length)) {
					true_opt.Append(true_count, idx);
				} else {
					false_opt.Append(false_count, idx);
				}
			} else {
				true_sel.set_index(maybe_count++, slice_idx);
			}
		}
		true_opt.Advance(true_count);
		false_opt.Advance(false_count);
		match_count += true_count;

		// Redensify the list cursors
		if (maybe_count < count) {
			count = maybe_count;
			DensifyNestedSelection(true_sel, count, slice_sel);
			PositionListCursor(lcursor, lvdata, pos, slice_sel, count);
			PositionListCursor(rcursor, rvdata, pos, slice_sel, count);
		}

		// Find everything that definitely matches
		true_count = PositionComparator::Definite<OP>(lchild, rchild, slice_sel, count, &true_sel, false_sel);
		if (true_count) {
			false_count = count - true_count;
			ExtractNestedSelection(false_count ? true_sel : slice_sel, true_count, sel, true_opt);
			match_count += true_count;

			// Redensify the list cursors
			count -= true_count;
			DensifyNestedSelection(false_sel, count, slice_sel);
			PositionListCursor(lcursor, lvdata, pos, slice_sel, count);
			PositionListCursor(rcursor, rvdata, pos, slice_sel, count);
		}

		// Find what might match on the next position
		true_count = PositionComparator::Possible<OP>(lchild, rchild, slice_sel, count, true_sel, &false_sel);
		false_count = count - true_count;
		ExtractNestedSelection(true_count ? false_sel : slice_sel, false_count, sel, false_opt);

		if (false_count) {
			DensifyNestedSelection(true_sel, true_count, slice_sel);
		}
		count = true_count;
	}

	return match_count;
}

template <class OP, class OPNESTED>
static idx_t DistinctSelectNested(Vector &left, Vector &right, const SelectionVector *sel, const idx_t count,
                                  SelectionVector *true_sel, SelectionVector *false_sel) {
	// The Select operations all use a dense pair of input vectors to partition
	// a selection vector in a single pass. But to implement progressive comparisons,
	// we have to make multiple passes, so we need to keep track of the original input positions
	// and then scatter the output selections when we are done.
	if (!sel) {
		sel = FlatVector::IncrementalSelectionVector();
	}

	// Make buffered selections for progressive comparisons
	// TODO: Remove unnecessary allocations
	SelectionVector true_vec(count);
	OptionalSelection true_opt(&true_vec);

	SelectionVector false_vec(count);
	OptionalSelection false_opt(&false_vec);

	SelectionVector maybe_vec(count);

	// Handle NULL nested values
	Vector l_not_null(left);
	Vector r_not_null(right);

	idx_t match_count = 0;
	auto unknown =
	    DistinctSelectNotNull<OP>(l_not_null, r_not_null, count, match_count, *sel, maybe_vec, true_opt, false_opt);

	if (PhysicalType::LIST == left.GetType().InternalType()) {
		match_count += DistinctSelectList<OPNESTED>(l_not_null, r_not_null, unknown, maybe_vec, true_opt, false_opt);
	} else {
		match_count += DistinctSelectStruct<OPNESTED>(l_not_null, r_not_null, unknown, maybe_vec, true_opt, false_opt);
	}

	// Copy the buffered selections to the output selections
	if (true_sel) {
		DensifyNestedSelection(true_vec, match_count, *true_sel);
	}

	if (false_sel) {
		DensifyNestedSelection(false_vec, count - match_count, *false_sel);
	}

	return match_count;
}

template <typename OP>
static void NestedDistinctExecute(Vector &left, Vector &right, Vector &result, idx_t count);

template <class T, class OP>
static inline void TemplatedDistinctExecute(Vector &left, Vector &right, Vector &result, idx_t count) {
	DistinctExecute<T, T, bool, OP>(left, right, result, count);
}
template <class OP>
static void ExecuteDistinct(Vector &left, Vector &right, Vector &result, idx_t count) {
	D_ASSERT(left.GetType() == right.GetType() && result.GetType() == LogicalType::BOOLEAN);
	// the inplace loops take the result as the last parameter
	switch (left.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedDistinctExecute<int8_t, OP>(left, right, result, count);
		break;
	case PhysicalType::INT16:
		TemplatedDistinctExecute<int16_t, OP>(left, right, result, count);
		break;
	case PhysicalType::INT32:
		TemplatedDistinctExecute<int32_t, OP>(left, right, result, count);
		break;
	case PhysicalType::INT64:
		TemplatedDistinctExecute<int64_t, OP>(left, right, result, count);
		break;
	case PhysicalType::UINT8:
		TemplatedDistinctExecute<uint8_t, OP>(left, right, result, count);
		break;
	case PhysicalType::UINT16:
		TemplatedDistinctExecute<uint16_t, OP>(left, right, result, count);
		break;
	case PhysicalType::UINT32:
		TemplatedDistinctExecute<uint32_t, OP>(left, right, result, count);
		break;
	case PhysicalType::UINT64:
		TemplatedDistinctExecute<uint64_t, OP>(left, right, result, count);
		break;
	case PhysicalType::INT128:
		TemplatedDistinctExecute<hugeint_t, OP>(left, right, result, count);
		break;
	case PhysicalType::FLOAT:
		TemplatedDistinctExecute<float, OP>(left, right, result, count);
		break;
	case PhysicalType::DOUBLE:
		TemplatedDistinctExecute<double, OP>(left, right, result, count);
		break;
	case PhysicalType::INTERVAL:
		TemplatedDistinctExecute<interval_t, OP>(left, right, result, count);
		break;
	case PhysicalType::VARCHAR:
		TemplatedDistinctExecute<string_t, OP>(left, right, result, count);
		break;
	case PhysicalType::LIST:
	case PhysicalType::MAP:
	case PhysicalType::STRUCT:
		NestedDistinctExecute<OP>(left, right, result, count);
		break;
	default:
		throw InternalException("Invalid type for distinct comparison");
	}
}

template <class OP, class OPNESTED = OP>
static idx_t TemplatedDistinctSelectOperation(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                              SelectionVector *true_sel, SelectionVector *false_sel) {
	// the inplace loops take the result as the last parameter
	switch (left.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return DistinctSelect<int8_t, int8_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::INT16:
		return DistinctSelect<int16_t, int16_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::INT32:
		return DistinctSelect<int32_t, int32_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::INT64:
		return DistinctSelect<int64_t, int64_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::UINT8:
		return DistinctSelect<uint8_t, uint8_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::UINT16:
		return DistinctSelect<uint16_t, uint16_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::UINT32:
		return DistinctSelect<uint32_t, uint32_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::UINT64:
		return DistinctSelect<uint64_t, uint64_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::INT128:
		return DistinctSelect<hugeint_t, hugeint_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::FLOAT:
		return DistinctSelect<float, float, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::DOUBLE:
		return DistinctSelect<double, double, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::INTERVAL:
		return DistinctSelect<interval_t, interval_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::VARCHAR:
		return DistinctSelect<string_t, string_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::MAP:
	case PhysicalType::STRUCT:
	case PhysicalType::LIST:
		return DistinctSelectNested<OP, OPNESTED>(left, right, sel, count, true_sel, false_sel);
	default:
		throw InternalException("Invalid type for distinct selection");
	}
}

template <typename OP>
static void NestedDistinctExecute(Vector &left, Vector &right, Vector &result, idx_t count) {
	const auto left_constant = left.GetVectorType() == VectorType::CONSTANT_VECTOR;
	const auto right_constant = right.GetVectorType() == VectorType::CONSTANT_VECTOR;

	if (left_constant && right_constant) {
		// both sides are constant, so just compare one element.
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		auto result_data = ConstantVector::GetData<bool>(result);
		SelectionVector true_sel(1);
		auto match_count = TemplatedDistinctSelectOperation<OP>(left, right, nullptr, 1, &true_sel, nullptr);
		result_data[0] = match_count > 0;
		return;
	}

	SelectionVector true_sel(count);
	SelectionVector false_sel(count);

	// DISTINCT is either true or false
	idx_t match_count = TemplatedDistinctSelectOperation<OP>(left, right, nullptr, count, &true_sel, &false_sel);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<bool>(result);

	for (idx_t i = 0; i < match_count; ++i) {
		const auto idx = true_sel.get_index(i);
		result_data[idx] = true;
	}

	const idx_t no_match_count = count - match_count;
	for (idx_t i = 0; i < no_match_count; ++i) {
		const auto idx = false_sel.get_index(i);
		result_data[idx] = false;
	}
}

void VectorOperations::DistinctFrom(Vector &left, Vector &right, Vector &result, idx_t count) {
	ExecuteDistinct<duckdb::DistinctFrom>(left, right, result, count);
}

void VectorOperations::NotDistinctFrom(Vector &left, Vector &right, Vector &result, idx_t count) {
	ExecuteDistinct<duckdb::NotDistinctFrom>(left, right, result, count);
}

// true := A != B with nulls being equal
idx_t VectorOperations::DistinctFrom(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                     SelectionVector *true_sel, SelectionVector *false_sel) {
	return TemplatedDistinctSelectOperation<duckdb::DistinctFrom>(left, right, sel, count, true_sel, false_sel);
}
// true := A == B with nulls being equal
idx_t VectorOperations::NotDistinctFrom(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                        SelectionVector *true_sel, SelectionVector *false_sel) {
	return TemplatedDistinctSelectOperation<duckdb::NotDistinctFrom>(left, right, sel, count, true_sel, false_sel);
}

// true := A > B with nulls being maximal
idx_t VectorOperations::DistinctGreaterThan(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                            SelectionVector *true_sel, SelectionVector *false_sel) {
	return TemplatedDistinctSelectOperation<duckdb::DistinctGreaterThan>(left, right, sel, count, true_sel, false_sel);
}

// true := A > B with nulls being minimal
idx_t VectorOperations::DistinctGreaterThanNullsFirst(Vector &left, Vector &right, const SelectionVector *sel,
                                                      idx_t count, SelectionVector *true_sel,
                                                      SelectionVector *false_sel) {
	return TemplatedDistinctSelectOperation<duckdb::DistinctGreaterThanNullsFirst, duckdb::DistinctGreaterThan>(
	    left, right, sel, count, true_sel, false_sel);
}
// true := A >= B with nulls being maximal
idx_t VectorOperations::DistinctGreaterThanEquals(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                                  SelectionVector *true_sel, SelectionVector *false_sel) {
	return TemplatedDistinctSelectOperation<duckdb::DistinctGreaterThanEquals>(left, right, sel, count, true_sel,
	                                                                           false_sel);
}
// true := A < B with nulls being maximal
idx_t VectorOperations::DistinctLessThan(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                         SelectionVector *true_sel, SelectionVector *false_sel) {
	return TemplatedDistinctSelectOperation<duckdb::DistinctLessThan>(left, right, sel, count, true_sel, false_sel);
}

// true := A < B with nulls being minimal
idx_t VectorOperations::DistinctLessThanNullsFirst(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                                   SelectionVector *true_sel, SelectionVector *false_sel) {
	return TemplatedDistinctSelectOperation<duckdb::DistinctLessThanNullsFirst, duckdb::DistinctLessThan>(
	    left, right, sel, count, true_sel, false_sel);
}

// true := A <= B with nulls being maximal
idx_t VectorOperations::DistinctLessThanEquals(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                               SelectionVector *true_sel, SelectionVector *false_sel) {
	return TemplatedDistinctSelectOperation<duckdb::DistinctLessThanEquals>(left, right, sel, count, true_sel,
	                                                                        false_sel);
}

// true := A != B with nulls being equal, inputs selected
idx_t VectorOperations::NestedNotEquals(Vector &left, Vector &right, const SelectionVector &sel, idx_t count,
                                        SelectionVector *true_sel, SelectionVector *false_sel) {
	return TemplatedDistinctSelectOperation<duckdb::DistinctFrom>(left, right, &sel, count, true_sel, false_sel);
}
// true := A == B with nulls being equal, inputs selected
idx_t VectorOperations::NestedEquals(Vector &left, Vector &right, const SelectionVector &sel, idx_t count,
                                     SelectionVector *true_sel, SelectionVector *false_sel) {
	return TemplatedDistinctSelectOperation<duckdb::NotDistinctFrom>(left, right, &sel, count, true_sel, false_sel);
}

} // namespace duckdb
//===--------------------------------------------------------------------===//
// null_operators.cpp
// Description: This file contains the implementation of the
// IS NULL/NOT IS NULL operators
//===--------------------------------------------------------------------===//




namespace duckdb {

template <bool INVERSE>
void IsNullLoop(Vector &input, Vector &result, idx_t count) {
	D_ASSERT(result.GetType() == LogicalType::BOOLEAN);

	if (input.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		auto result_data = ConstantVector::GetData<bool>(result);
		*result_data = INVERSE ? !ConstantVector::IsNull(input) : ConstantVector::IsNull(input);
	} else {
		VectorData data;
		input.Orrify(count, data);

		result.SetVectorType(VectorType::FLAT_VECTOR);
		auto result_data = FlatVector::GetData<bool>(result);
		for (idx_t i = 0; i < count; i++) {
			auto idx = data.sel->get_index(i);
			result_data[i] = INVERSE ? data.validity.RowIsValid(idx) : !data.validity.RowIsValid(idx);
		}
	}
}

void VectorOperations::IsNotNull(Vector &input, Vector &result, idx_t count) {
	IsNullLoop<true>(input, result, count);
}

void VectorOperations::IsNull(Vector &input, Vector &result, idx_t count) {
	IsNullLoop<false>(input, result, count);
}

bool VectorOperations::HasNotNull(Vector &input, idx_t count) {
	if (count == 0) {
		return false;
	}
	if (input.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		return !ConstantVector::IsNull(input);
	} else {
		VectorData data;
		input.Orrify(count, data);

		if (data.validity.AllValid()) {
			return true;
		}
		for (idx_t i = 0; i < count; i++) {
			auto idx = data.sel->get_index(i);
			if (data.validity.RowIsValid(idx)) {
				return true;
			}
		}
		return false;
	}
}

bool VectorOperations::HasNull(Vector &input, idx_t count) {
	if (count == 0) {
		return false;
	}
	if (input.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		return ConstantVector::IsNull(input);
	} else {
		VectorData data;
		input.Orrify(count, data);

		if (data.validity.AllValid()) {
			return false;
		}
		for (idx_t i = 0; i < count; i++) {
			auto idx = data.sel->get_index(i);
			if (!data.validity.RowIsValid(idx)) {
				return true;
			}
		}
		return false;
	}
}

idx_t VectorOperations::CountNotNull(Vector &input, const idx_t count) {
	idx_t valid = 0;

	VectorData vdata;
	input.Orrify(count, vdata);
	if (vdata.validity.AllValid()) {
		return count;
	}
	switch (input.GetVectorType()) {
	case VectorType::FLAT_VECTOR:
		valid += vdata.validity.CountValid(count);
		break;
	case VectorType::CONSTANT_VECTOR:
		valid += vdata.validity.CountValid(1) * count;
		break;
	default:
		for (idx_t i = 0; i < count; ++i) {
			const auto row_idx = vdata.sel->get_index(i);
			valid += int(vdata.validity.RowIsValid(row_idx));
		}
		break;
	}

	return valid;
}

} // namespace duckdb
//===--------------------------------------------------------------------===//
// numeric_inplace_operators.cpp
// Description: This file contains the implementation of numeric inplace ops
// += *= /= -= %=
//===--------------------------------------------------------------------===//



#include <algorithm>

namespace duckdb {

//===--------------------------------------------------------------------===//
// In-Place Addition
//===--------------------------------------------------------------------===//

void VectorOperations::AddInPlace(Vector &input, int64_t right, idx_t count) {
	D_ASSERT(input.GetType().id() == LogicalTypeId::POINTER);
	if (right == 0) {
		return;
	}
	switch (input.GetVectorType()) {
	case VectorType::CONSTANT_VECTOR: {
		D_ASSERT(!ConstantVector::IsNull(input));
		auto data = ConstantVector::GetData<uintptr_t>(input);
		*data += right;
		break;
	}
	default: {
		D_ASSERT(input.GetVectorType() == VectorType::FLAT_VECTOR);
		auto data = FlatVector::GetData<uintptr_t>(input);
		for (idx_t i = 0; i < count; i++) {
			data[i] += right;
		}
		break;
	}
	}
}

} // namespace duckdb












namespace duckdb {

template <class OP>
struct VectorStringCastOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &mask, idx_t idx, void *dataptr) {
		auto result = (Vector *)dataptr;
		return OP::template Operation<INPUT_TYPE>(input, *result);
	}
};

struct VectorTryCastData {
	VectorTryCastData(Vector &result_p, string *error_message_p, bool strict_p)
	    : result(result_p), error_message(error_message_p), strict(strict_p) {
	}

	Vector &result;
	string *error_message;
	bool strict;
	bool all_converted = true;
};

template <class OP>
struct VectorTryCastOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &mask, idx_t idx, void *dataptr) {
		RESULT_TYPE output;
		if (DUCKDB_LIKELY(OP::template Operation<INPUT_TYPE, RESULT_TYPE>(input, output))) {
			return output;
		}
		auto data = (VectorTryCastData *)dataptr;
		return HandleVectorCastError::Operation<RESULT_TYPE>(CastExceptionText<INPUT_TYPE, RESULT_TYPE>(input), mask,
		                                                     idx, data->error_message, data->all_converted);
	}
};

template <class OP>
struct VectorTryCastStrictOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &mask, idx_t idx, void *dataptr) {
		auto data = (VectorTryCastData *)dataptr;
		RESULT_TYPE output;
		if (DUCKDB_LIKELY(OP::template Operation<INPUT_TYPE, RESULT_TYPE>(input, output, data->strict))) {
			return output;
		}
		return HandleVectorCastError::Operation<RESULT_TYPE>(CastExceptionText<INPUT_TYPE, RESULT_TYPE>(input), mask,
		                                                     idx, data->error_message, data->all_converted);
	}
};

template <class OP>
struct VectorTryCastErrorOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &mask, idx_t idx, void *dataptr) {
		auto data = (VectorTryCastData *)dataptr;
		RESULT_TYPE output;
		if (DUCKDB_LIKELY(
		        OP::template Operation<INPUT_TYPE, RESULT_TYPE>(input, output, data->error_message, data->strict))) {
			return output;
		}
		bool has_error = data->error_message && !data->error_message->empty();
		return HandleVectorCastError::Operation<RESULT_TYPE>(
		    has_error ? *data->error_message : CastExceptionText<INPUT_TYPE, RESULT_TYPE>(input), mask, idx,
		    data->error_message, data->all_converted);
	}
};

template <class OP>
struct VectorTryCastStringOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &mask, idx_t idx, void *dataptr) {
		auto data = (VectorTryCastData *)dataptr;
		RESULT_TYPE output;
		if (DUCKDB_LIKELY(OP::template Operation<INPUT_TYPE, RESULT_TYPE>(input, output, data->result,
		                                                                  data->error_message, data->strict))) {
			return output;
		}
		return HandleVectorCastError::Operation<RESULT_TYPE>(CastExceptionText<INPUT_TYPE, RESULT_TYPE>(input), mask,
		                                                     idx, data->error_message, data->all_converted);
	}
};

template <class SRC, class DST, class OP>
static bool TemplatedVectorTryCastLoop(Vector &source, Vector &result, idx_t count, bool strict,
                                       string *error_message) {
	VectorTryCastData input(result, error_message, strict);
	UnaryExecutor::GenericExecute<SRC, DST, OP>(source, result, count, &input, error_message);
	return input.all_converted;
}

template <class SRC, class DST, class OP>
static bool VectorTryCastLoop(Vector &source, Vector &result, idx_t count, string *error_message) {
	return TemplatedVectorTryCastLoop<SRC, DST, VectorTryCastOperator<OP>>(source, result, count, false, error_message);
}

template <class SRC, class DST, class OP>
static bool VectorTryCastStrictLoop(Vector &source, Vector &result, idx_t count, bool strict, string *error_message) {
	return TemplatedVectorTryCastLoop<SRC, DST, VectorTryCastStrictOperator<OP>>(source, result, count, strict,
	                                                                             error_message);
}

template <class SRC, class DST, class OP>
static bool VectorTryCastErrorLoop(Vector &source, Vector &result, idx_t count, bool strict, string *error_message) {
	return TemplatedVectorTryCastLoop<SRC, DST, VectorTryCastErrorOperator<OP>>(source, result, count, strict,
	                                                                            error_message);
}

template <class SRC, class DST, class OP>
static bool VectorTryCastStringLoop(Vector &source, Vector &result, idx_t count, bool strict, string *error_message) {
	return TemplatedVectorTryCastLoop<SRC, DST, VectorTryCastStringOperator<OP>>(source, result, count, strict,
	                                                                             error_message);
}

template <class SRC, class OP>
static void VectorStringCast(Vector &source, Vector &result, idx_t count) {
	D_ASSERT(result.GetType().InternalType() == PhysicalType::VARCHAR);
	UnaryExecutor::GenericExecute<SRC, string_t, VectorStringCastOperator<OP>>(source, result, count, (void *)&result);
}

template <class SRC>
static bool NumericCastSwitch(Vector &source, Vector &result, idx_t count, string *error_message) {
	// now switch on the result type
	switch (result.GetType().id()) {
	case LogicalTypeId::BOOLEAN:
		return VectorTryCastLoop<SRC, bool, duckdb::NumericTryCast>(source, result, count, error_message);
	case LogicalTypeId::TINYINT:
		return VectorTryCastLoop<SRC, int8_t, duckdb::NumericTryCast>(source, result, count, error_message);
	case LogicalTypeId::SMALLINT:
		return VectorTryCastLoop<SRC, int16_t, duckdb::NumericTryCast>(source, result, count, error_message);
	case LogicalTypeId::INTEGER:
		return VectorTryCastLoop<SRC, int32_t, duckdb::NumericTryCast>(source, result, count, error_message);
	case LogicalTypeId::BIGINT:
		return VectorTryCastLoop<SRC, int64_t, duckdb::NumericTryCast>(source, result, count, error_message);
	case LogicalTypeId::UTINYINT:
		return VectorTryCastLoop<SRC, uint8_t, duckdb::NumericTryCast>(source, result, count, error_message);
	case LogicalTypeId::USMALLINT:
		return VectorTryCastLoop<SRC, uint16_t, duckdb::NumericTryCast>(source, result, count, error_message);
	case LogicalTypeId::UINTEGER:
		return VectorTryCastLoop<SRC, uint32_t, duckdb::NumericTryCast>(source, result, count, error_message);
	case LogicalTypeId::UBIGINT:
		return VectorTryCastLoop<SRC, uint64_t, duckdb::NumericTryCast>(source, result, count, error_message);
	case LogicalTypeId::HUGEINT:
		return VectorTryCastLoop<SRC, hugeint_t, duckdb::NumericTryCast>(source, result, count, error_message);
	case LogicalTypeId::FLOAT:
		return VectorTryCastLoop<SRC, float, duckdb::NumericTryCast>(source, result, count, error_message);
	case LogicalTypeId::DOUBLE:
		return VectorTryCastLoop<SRC, double, duckdb::NumericTryCast>(source, result, count, error_message);
	case LogicalTypeId::DECIMAL:
		return ToDecimalCast<SRC>(source, result, count, error_message);
	case LogicalTypeId::JSON:
	case LogicalTypeId::VARCHAR: {
		VectorStringCast<SRC, duckdb::StringCast>(source, result, count);
		return true;
	}
	default:
		return TryVectorNullCast(source, result, count, error_message);
	}
}

template <class T>
bool FillEnum(string_t *source_data, ValidityMask &source_mask, const LogicalType &source_type, T *result_data,
              ValidityMask &result_mask, const LogicalType &result_type, idx_t count, string *error_message,
              const SelectionVector *sel) {
	bool all_converted = true;
	for (idx_t i = 0; i < count; i++) {
		idx_t source_idx = i;
		if (sel) {
			source_idx = sel->get_index(i);
		}
		if (source_mask.RowIsValid(source_idx)) {
			auto string_value = source_data[source_idx].GetString();
			auto pos = EnumType::GetPos(result_type, string_value);
			if (pos == -1) {
				result_data[i] =
				    HandleVectorCastError::Operation<T>(CastExceptionText<string_t, T>(source_data[source_idx]),
				                                        result_mask, i, error_message, all_converted);
			} else {
				result_data[i] = pos;
			}
		} else {
			result_mask.SetInvalid(i);
		}
	}
	return all_converted;
}

template <class T>
bool TransformEnum(Vector &source, Vector &result, idx_t count, string *error_message) {
	D_ASSERT(source.GetType().id() == LogicalTypeId::VARCHAR);
	auto enum_name = EnumType::GetTypeName(result.GetType());
	switch (source.GetVectorType()) {
	case VectorType::CONSTANT_VECTOR: {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);

		auto source_data = ConstantVector::GetData<string_t>(source);
		auto source_mask = ConstantVector::Validity(source);
		auto result_data = ConstantVector::GetData<T>(result);
		auto &result_mask = ConstantVector::Validity(result);

		return FillEnum(source_data, source_mask, source.GetType(), result_data, result_mask, result.GetType(), 1,
		                error_message, nullptr);
	}
	default: {
		VectorData vdata;
		source.Orrify(count, vdata);

		result.SetVectorType(VectorType::FLAT_VECTOR);

		auto source_data = (string_t *)vdata.data;
		auto source_sel = vdata.sel;
		auto source_mask = vdata.validity;
		auto result_data = FlatVector::GetData<T>(result);
		auto &result_mask = FlatVector::Validity(result);

		return FillEnum(source_data, source_mask, source.GetType(), result_data, result_mask, result.GetType(), count,
		                error_message, source_sel);
	}
	}
}
static bool VectorStringCastNumericSwitch(Vector &source, Vector &result, idx_t count, bool strict,
                                          string *error_message) {
	// now switch on the result type
	switch (result.GetType().id()) {
	case LogicalTypeId::ENUM: {
		switch (result.GetType().InternalType()) {
		case PhysicalType::UINT8:
			return TransformEnum<uint8_t>(source, result, count, error_message);
		case PhysicalType::UINT16:
			return TransformEnum<uint16_t>(source, result, count, error_message);
		case PhysicalType::UINT32:
			return TransformEnum<uint32_t>(source, result, count, error_message);
		default:
			throw InternalException("ENUM can only have unsigned integers (except UINT64) as physical types");
		}
	}
	case LogicalTypeId::BOOLEAN:
		return VectorTryCastStrictLoop<string_t, bool, duckdb::TryCast>(source, result, count, strict, error_message);
	case LogicalTypeId::TINYINT:
		return VectorTryCastStrictLoop<string_t, int8_t, duckdb::TryCast>(source, result, count, strict, error_message);
	case LogicalTypeId::SMALLINT:
		return VectorTryCastStrictLoop<string_t, int16_t, duckdb::TryCast>(source, result, count, strict,
		                                                                   error_message);
	case LogicalTypeId::INTEGER:
		return VectorTryCastStrictLoop<string_t, int32_t, duckdb::TryCast>(source, result, count, strict,
		                                                                   error_message);
	case LogicalTypeId::BIGINT:
		return VectorTryCastStrictLoop<string_t, int64_t, duckdb::TryCast>(source, result, count, strict,
		                                                                   error_message);
	case LogicalTypeId::UTINYINT:
		return VectorTryCastStrictLoop<string_t, uint8_t, duckdb::TryCast>(source, result, count, strict,
		                                                                   error_message);
	case LogicalTypeId::USMALLINT:
		return VectorTryCastStrictLoop<string_t, uint16_t, duckdb::TryCast>(source, result, count, strict,
		                                                                    error_message);
	case LogicalTypeId::UINTEGER:
		return VectorTryCastStrictLoop<string_t, uint32_t, duckdb::TryCast>(source, result, count, strict,
		                                                                    error_message);
	case LogicalTypeId::UBIGINT:
		return VectorTryCastStrictLoop<string_t, uint64_t, duckdb::TryCast>(source, result, count, strict,
		                                                                    error_message);
	case LogicalTypeId::HUGEINT:
		return VectorTryCastStrictLoop<string_t, hugeint_t, duckdb::TryCast>(source, result, count, strict,
		                                                                     error_message);
	case LogicalTypeId::FLOAT:
		return VectorTryCastStrictLoop<string_t, float, duckdb::TryCast>(source, result, count, strict, error_message);
	case LogicalTypeId::DOUBLE:
		return VectorTryCastStrictLoop<string_t, double, duckdb::TryCast>(source, result, count, strict, error_message);
	case LogicalTypeId::INTERVAL:
		return VectorTryCastErrorLoop<string_t, interval_t, duckdb::TryCastErrorMessage>(source, result, count, strict,
		                                                                                 error_message);
	case LogicalTypeId::DECIMAL:
		return ToDecimalCast<string_t>(source, result, count, error_message);
	default:
		return TryVectorNullCast(source, result, count, error_message);
	}
}

static bool StringCastSwitch(Vector &source, Vector &result, idx_t count, bool strict, string *error_message) {
	// now switch on the result type
	switch (result.GetType().id()) {
	case LogicalTypeId::DATE:
		return VectorTryCastErrorLoop<string_t, date_t, duckdb::TryCastErrorMessage>(source, result, count, strict,
		                                                                             error_message);
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
		return VectorTryCastErrorLoop<string_t, dtime_t, duckdb::TryCastErrorMessage>(source, result, count, strict,
		                                                                              error_message);
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		return VectorTryCastErrorLoop<string_t, timestamp_t, duckdb::TryCastErrorMessage>(source, result, count, strict,
		                                                                                  error_message);
	case LogicalTypeId::TIMESTAMP_NS:
		return VectorTryCastStrictLoop<string_t, timestamp_t, duckdb::TryCastToTimestampNS>(source, result, count,
		                                                                                    strict, error_message);
	case LogicalTypeId::TIMESTAMP_SEC:
		return VectorTryCastStrictLoop<string_t, timestamp_t, duckdb::TryCastToTimestampSec>(source, result, count,
		                                                                                     strict, error_message);
	case LogicalTypeId::TIMESTAMP_MS:
		return VectorTryCastStrictLoop<string_t, timestamp_t, duckdb::TryCastToTimestampMS>(source, result, count,
		                                                                                    strict, error_message);
	case LogicalTypeId::BLOB:
		return VectorTryCastStringLoop<string_t, string_t, duckdb::TryCastToBlob>(source, result, count, strict,
		                                                                          error_message);
	case LogicalTypeId::UUID:
		return VectorTryCastStringLoop<string_t, hugeint_t, duckdb::TryCastToUUID>(source, result, count, strict,
		                                                                           error_message);
	case LogicalTypeId::SQLNULL:
		return TryVectorNullCast(source, result, count, error_message);
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::JSON:
		result.Reinterpret(source);
		return true;
	default:
		return VectorStringCastNumericSwitch(source, result, count, strict, error_message);
	}
}

static bool DateCastSwitch(Vector &source, Vector &result, idx_t count, string *error_message) {
	// now switch on the result type
	switch (result.GetType().id()) {
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::JSON:
		// date to varchar
		VectorStringCast<date_t, duckdb::StringCast>(source, result, count);
		return true;
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		// date to timestamp
		return VectorTryCastLoop<date_t, timestamp_t, duckdb::TryCast>(source, result, count, error_message);
	case LogicalTypeId::TIMESTAMP_NS:
		return VectorTryCastLoop<date_t, timestamp_t, duckdb::TryCastToTimestampNS>(source, result, count,
		                                                                            error_message);
	case LogicalTypeId::TIMESTAMP_SEC:
		return VectorTryCastLoop<date_t, timestamp_t, duckdb::TryCastToTimestampSec>(source, result, count,
		                                                                             error_message);
	case LogicalTypeId::TIMESTAMP_MS:
		return VectorTryCastLoop<date_t, timestamp_t, duckdb::TryCastToTimestampMS>(source, result, count,
		                                                                            error_message);
	default:
		return TryVectorNullCast(source, result, count, error_message);
	}
}

static bool TimeCastSwitch(Vector &source, Vector &result, idx_t count, string *error_message) {
	// now switch on the result type
	switch (result.GetType().id()) {
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::JSON:
		// time to varchar
		VectorStringCast<dtime_t, duckdb::StringCast>(source, result, count);
		return true;
	case LogicalTypeId::TIME_TZ:
		// time to time with time zone
		UnaryExecutor::Execute<dtime_t, dtime_t, duckdb::Cast>(source, result, count);
		return true;
	default:
		return TryVectorNullCast(source, result, count, error_message);
	}
}

static bool TimeTzCastSwitch(Vector &source, Vector &result, idx_t count, string *error_message) {
	// now switch on the result type
	switch (result.GetType().id()) {
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::JSON:
		// time with time zone to varchar
		VectorStringCast<dtime_t, duckdb::StringCastTZ>(source, result, count);
		return true;
	case LogicalTypeId::TIME:
		// time with time zone to time
		UnaryExecutor::Execute<dtime_t, dtime_t, duckdb::Cast>(source, result, count);
		return true;
	default:
		return TryVectorNullCast(source, result, count, error_message);
	}
}

static bool TimestampCastSwitch(Vector &source, Vector &result, idx_t count, string *error_message) {
	// now switch on the result type
	switch (result.GetType().id()) {
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::JSON:
		// timestamp to varchar
		VectorStringCast<timestamp_t, duckdb::StringCast>(source, result, count);
		break;
	case LogicalTypeId::DATE:
		// timestamp to date
		UnaryExecutor::Execute<timestamp_t, date_t, duckdb::Cast>(source, result, count);
		break;
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
		// timestamp to time
		UnaryExecutor::Execute<timestamp_t, dtime_t, duckdb::Cast>(source, result, count);
		break;
	case LogicalTypeId::TIMESTAMP_TZ:
		// timestamp (us) to timestamp with time zone
		UnaryExecutor::Execute<timestamp_t, timestamp_t, duckdb::Cast>(source, result, count);
		break;
	case LogicalTypeId::TIMESTAMP_NS:
		// timestamp (us) to timestamp (ns)
		UnaryExecutor::Execute<timestamp_t, timestamp_t, duckdb::CastTimestampUsToNs>(source, result, count);
		break;
	case LogicalTypeId::TIMESTAMP_MS:
		// timestamp (us) to timestamp (ms)
		UnaryExecutor::Execute<timestamp_t, timestamp_t, duckdb::CastTimestampUsToMs>(source, result, count);
		break;
	case LogicalTypeId::TIMESTAMP_SEC:
		// timestamp (us) to timestamp (s)
		UnaryExecutor::Execute<timestamp_t, timestamp_t, duckdb::CastTimestampUsToSec>(source, result, count);
		break;
	default:
		return TryVectorNullCast(source, result, count, error_message);
	}
	return true;
}

static bool TimestampTzCastSwitch(Vector &source, Vector &result, idx_t count, string *error_message) {
	// now switch on the result type
	switch (result.GetType().id()) {
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::JSON:
		// timestamp with time zone to varchar
		VectorStringCast<timestamp_t, duckdb::StringCastTZ>(source, result, count);
		break;
	case LogicalTypeId::TIME_TZ:
		// timestamp with time zone to time with time zone.
		// TODO: set the offset to +00
		UnaryExecutor::Execute<timestamp_t, dtime_t, duckdb::Cast>(source, result, count);
		break;
	case LogicalTypeId::TIMESTAMP:
		// timestamp with time zone to timestamp (us)
		UnaryExecutor::Execute<timestamp_t, timestamp_t, duckdb::Cast>(source, result, count);
		break;
	default:
		return TryVectorNullCast(source, result, count, error_message);
	}
	return true;
}

static bool TimestampNsCastSwitch(Vector &source, Vector &result, idx_t count, string *error_message) {
	// now switch on the result type
	switch (result.GetType().id()) {
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::JSON:
		// timestamp (ns) to varchar
		VectorStringCast<timestamp_t, duckdb::CastFromTimestampNS>(source, result, count);
		break;
	case LogicalTypeId::TIMESTAMP:
		// timestamp (ns) to timestamp (us)
		UnaryExecutor::Execute<timestamp_t, timestamp_t, duckdb::CastTimestampNsToUs>(source, result, count);
		break;
	default:
		return TryVectorNullCast(source, result, count, error_message);
	}
	return true;
}

static bool TimestampMsCastSwitch(Vector &source, Vector &result, idx_t count, string *error_message) {
	// now switch on the result type
	switch (result.GetType().id()) {
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::JSON:
		// timestamp (ms) to varchar
		VectorStringCast<timestamp_t, duckdb::CastFromTimestampMS>(source, result, count);
		break;
	case LogicalTypeId::TIMESTAMP:
		// timestamp (ms) to timestamp (us)
		UnaryExecutor::Execute<timestamp_t, timestamp_t, duckdb::CastTimestampMsToUs>(source, result, count);
		break;
	default:
		return TryVectorNullCast(source, result, count, error_message);
	}
	return true;
}

static bool TimestampSecCastSwitch(Vector &source, Vector &result, idx_t count, string *error_message) {
	// now switch on the result type
	switch (result.GetType().id()) {
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::JSON:
		// timestamp (sec) to varchar
		VectorStringCast<timestamp_t, duckdb::CastFromTimestampSec>(source, result, count);
		break;
	case LogicalTypeId::TIMESTAMP:
		// timestamp (s) to timestamp (us)
		UnaryExecutor::Execute<timestamp_t, timestamp_t, duckdb::CastTimestampSecToUs>(source, result, count);
		break;
	default:
		return TryVectorNullCast(source, result, count, error_message);
	}
	return true;
}

static bool IntervalCastSwitch(Vector &source, Vector &result, idx_t count, string *error_message) {
	// now switch on the result type
	switch (result.GetType().id()) {
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::JSON:
		// time to varchar
		VectorStringCast<interval_t, duckdb::StringCast>(source, result, count);
		break;
	default:
		return TryVectorNullCast(source, result, count, error_message);
	}
	return true;
}

static bool UUIDCastSwitch(Vector &source, Vector &result, idx_t count, string *error_message) {
	// now switch on the result type
	switch (result.GetType().id()) {
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::JSON:
		// uuid to varchar
		VectorStringCast<hugeint_t, duckdb::CastFromUUID>(source, result, count);
		break;
	default:
		return TryVectorNullCast(source, result, count, error_message);
	}
	return true;
}

static bool BlobCastSwitch(Vector &source, Vector &result, idx_t count, string *error_message) {
	// now switch on the result type
	switch (result.GetType().id()) {
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::JSON:
		// blob to varchar
		VectorStringCast<string_t, duckdb::CastFromBlob>(source, result, count);
		break;
	case LogicalTypeId::AGGREGATE_STATE:
		result.Reinterpret(source);
		break;
	default:
		return TryVectorNullCast(source, result, count, error_message);
	}
	return true;
}

static bool ValueStringCastSwitch(Vector &source, Vector &result, idx_t count, string *error_message) {
	switch (result.GetType().id()) {
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::JSON:
		if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(source.GetVectorType());
		} else {
			result.SetVectorType(VectorType::FLAT_VECTOR);
		}
		for (idx_t i = 0; i < count; i++) {
			auto src_val = source.GetValue(i);
			if (src_val.IsNull()) {
				result.SetValue(i, Value(result.GetType()));
			} else {
				auto str_val = src_val.ToString();
				result.SetValue(i, Value(str_val));
			}
		}
		return true;
	default:
		return TryVectorNullCast(source, result, count, error_message);
	}
}

static bool ListCastSwitch(Vector &source, Vector &result, idx_t count, string *error_message) {
	switch (result.GetType().id()) {
	case LogicalTypeId::LIST: {
		// only handle constant and flat vectors here for now
		if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(source.GetVectorType());
			ConstantVector::SetNull(result, ConstantVector::IsNull(source));

			auto ldata = ConstantVector::GetData<list_entry_t>(source);
			auto tdata = ConstantVector::GetData<list_entry_t>(result);
			*tdata = *ldata;
		} else {
			source.Normalify(count);
			result.SetVectorType(VectorType::FLAT_VECTOR);
			FlatVector::SetValidity(result, FlatVector::Validity(source));

			auto ldata = FlatVector::GetData<list_entry_t>(source);
			auto tdata = FlatVector::GetData<list_entry_t>(result);
			for (idx_t i = 0; i < count; i++) {
				tdata[i] = ldata[i];
			}
		}
		auto &source_cc = ListVector::GetEntry(source);
		auto source_size = ListVector::GetListSize(source);

		ListVector::Reserve(result, source_size);
		auto &append_vector = ListVector::GetEntry(result);

		VectorOperations::Cast(source_cc, append_vector, source_size);
		ListVector::SetListSize(result, source_size);
		D_ASSERT(ListVector::GetListSize(result) == source_size);
		return true;
	}
	default:
		return ValueStringCastSwitch(source, result, count, error_message);
	}
}

template <class SRC_TYPE, class RES_TYPE>
bool FillEnum(Vector &source, Vector &result, idx_t count, string *error_message) {
	bool all_converted = true;
	result.SetVectorType(VectorType::FLAT_VECTOR);

	auto &str_vec = EnumType::GetValuesInsertOrder(source.GetType());
	auto str_vec_ptr = FlatVector::GetData<string_t>(str_vec);

	auto res_enum_type = result.GetType();

	VectorData vdata;
	source.Orrify(count, vdata);

	auto source_data = (SRC_TYPE *)vdata.data;
	auto source_sel = vdata.sel;
	auto source_mask = vdata.validity;

	auto result_data = FlatVector::GetData<RES_TYPE>(result);
	auto &result_mask = FlatVector::Validity(result);

	for (idx_t i = 0; i < count; i++) {
		auto src_idx = source_sel->get_index(i);
		if (!source_mask.RowIsValid(src_idx)) {
			result_mask.SetInvalid(i);
			continue;
		}
		auto str = str_vec_ptr[source_data[src_idx]].GetString();
		auto key = EnumType::GetPos(res_enum_type, str);
		if (key == -1) {
			// key doesn't exist on result enum
			if (!error_message) {
				result_data[i] = HandleVectorCastError::Operation<RES_TYPE>(
				    CastExceptionText<SRC_TYPE, RES_TYPE>(source_data[src_idx]), result_mask, i, error_message,
				    all_converted);
			} else {
				result_mask.SetInvalid(i);
			}
			continue;
		}
		result_data[i] = key;
	}
	return all_converted;
}

template <class SRC_TYPE>
bool FillEnumResultTemplate(Vector &source, Vector &result, idx_t count, string *error_message) {
	switch (source.GetType().InternalType()) {
	case PhysicalType::UINT8:
		return FillEnum<SRC_TYPE, uint8_t>(source, result, count, error_message);
	case PhysicalType::UINT16:
		return FillEnum<SRC_TYPE, uint16_t>(source, result, count, error_message);
	case PhysicalType::UINT32:
		return FillEnum<SRC_TYPE, uint32_t>(source, result, count, error_message);
	default:
		throw InternalException("ENUM can only have unsigned integers (except UINT64) as physical types");
	}
}

void EnumToVarchar(Vector &source, Vector &result, idx_t count, PhysicalType enum_physical_type) {
	if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(source.GetVectorType());
	} else {
		result.SetVectorType(VectorType::FLAT_VECTOR);
	}
	auto &str_vec = EnumType::GetValuesInsertOrder(source.GetType());
	auto str_vec_ptr = FlatVector::GetData<string_t>(str_vec);
	auto res_vec_ptr = FlatVector::GetData<string_t>(result);

	// TODO remove value api from this loop
	for (idx_t i = 0; i < count; i++) {
		auto src_val = source.GetValue(i);
		if (src_val.IsNull()) {
			result.SetValue(i, Value());
			continue;
		}

		uint64_t enum_idx;
		switch (enum_physical_type) {
		case PhysicalType::UINT8:
			enum_idx = UTinyIntValue::Get(src_val);
			break;
		case PhysicalType::UINT16:
			enum_idx = USmallIntValue::Get(src_val);
			break;
		case PhysicalType::UINT32:
			enum_idx = UIntegerValue::Get(src_val);
			break;
		case PhysicalType::UINT64: //  DEDUP_POINTER_ENUM
		{
			res_vec_ptr[i] = (const char *)UBigIntValue::Get(src_val);
			continue;
		}

		default:
			throw InternalException("ENUM can only have unsigned integers as physical types");
		}
		res_vec_ptr[i] = str_vec_ptr[enum_idx];
	}
}

static bool EnumCastSwitch(Vector &source, Vector &result, idx_t count, string *error_message, bool strict) {
	auto enum_physical_type = source.GetType().InternalType();
	switch (result.GetType().id()) {
	case LogicalTypeId::ENUM: {
		// This means they are both ENUMs, but of different types.
		switch (enum_physical_type) {
		case PhysicalType::UINT8:
			return FillEnumResultTemplate<uint8_t>(source, result, count, error_message);
		case PhysicalType::UINT16:
			return FillEnumResultTemplate<uint16_t>(source, result, count, error_message);
		case PhysicalType::UINT32:
			return FillEnumResultTemplate<uint32_t>(source, result, count, error_message);
		default:
			throw InternalException("ENUM can only have unsigned integers (except UINT64) as physical types");
		}
	}
	case LogicalTypeId::JSON:
	case LogicalTypeId::VARCHAR: {
		EnumToVarchar(source, result, count, enum_physical_type);
		break;
	}
	default: {
		// Cast to varchar
		Vector varchar_cast(LogicalType::VARCHAR, count);
		EnumToVarchar(source, varchar_cast, count, enum_physical_type);
		// Try to cast from varchar to whatever we wanted before
		VectorOperations::TryCast(varchar_cast, result, count, error_message, strict);
		break;
	}
	}
	return true;
}

static bool AggregateStateToBlobCast(Vector &source, Vector &result, idx_t count, string *error_message, bool strict) {
	if (result.GetType().id() != LogicalTypeId::BLOB) {
		throw TypeMismatchException(source.GetType(), result.GetType(),
		                            "Cannot cast AGGREGATE_STATE to anything but BLOB");
	}
	result.Reinterpret(source);
	return true;
}

static bool StructCastSwitch(Vector &source, Vector &result, idx_t count, string *error_message) {
	switch (result.GetType().id()) {
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::MAP: {
		auto &source_child_types = StructType::GetChildTypes(source.GetType());
		auto &result_child_types = StructType::GetChildTypes(result.GetType());
		if (source_child_types.size() != result_child_types.size()) {
			throw TypeMismatchException(source.GetType(), result.GetType(), "Cannot cast STRUCTs of different size");
		}
		auto &source_children = StructVector::GetEntries(source);
		D_ASSERT(source_children.size() == source_child_types.size());

		auto &result_children = StructVector::GetEntries(result);
		for (idx_t c_idx = 0; c_idx < result_child_types.size(); c_idx++) {
			auto &result_child_vector = result_children[c_idx];
			auto &source_child_vector = *source_children[c_idx];
			if (result_child_vector->GetType() != source_child_vector.GetType()) {
				VectorOperations::Cast(source_child_vector, *result_child_vector, count, false);
			} else {
				result_child_vector->Reference(source_child_vector);
			}
		}
		if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result, ConstantVector::IsNull(source));
		} else {
			source.Normalify(count);
			FlatVector::Validity(result) = FlatVector::Validity(source);
		}
		return true;
	}
	case LogicalTypeId::JSON:
	case LogicalTypeId::VARCHAR:
		if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(source.GetVectorType());
		} else {
			result.SetVectorType(VectorType::FLAT_VECTOR);
		}
		for (idx_t i = 0; i < count; i++) {
			auto src_val = source.GetValue(i);
			auto str_val = src_val.ToString();
			result.SetValue(i, Value(str_val));
		}
		return true;
	default:
		return TryVectorNullCast(source, result, count, error_message);
	}
}

bool VectorOperations::TryCast(Vector &source, Vector &result, idx_t count, string *error_message, bool strict) {
	D_ASSERT(source.GetType() != result.GetType());
	// first switch on source type
	switch (source.GetType().id()) {
	case LogicalTypeId::BOOLEAN:
		return NumericCastSwitch<bool>(source, result, count, error_message);
	case LogicalTypeId::TINYINT:
		return NumericCastSwitch<int8_t>(source, result, count, error_message);
	case LogicalTypeId::SMALLINT:
		return NumericCastSwitch<int16_t>(source, result, count, error_message);
	case LogicalTypeId::INTEGER:
		return NumericCastSwitch<int32_t>(source, result, count, error_message);
	case LogicalTypeId::BIGINT:
		return NumericCastSwitch<int64_t>(source, result, count, error_message);
	case LogicalTypeId::UTINYINT:
		return NumericCastSwitch<uint8_t>(source, result, count, error_message);
	case LogicalTypeId::USMALLINT:
		return NumericCastSwitch<uint16_t>(source, result, count, error_message);
	case LogicalTypeId::UINTEGER:
		return NumericCastSwitch<uint32_t>(source, result, count, error_message);
	case LogicalTypeId::UBIGINT:
		return NumericCastSwitch<uint64_t>(source, result, count, error_message);
	case LogicalTypeId::HUGEINT:
		return NumericCastSwitch<hugeint_t>(source, result, count, error_message);
	case LogicalTypeId::UUID:
		return UUIDCastSwitch(source, result, count, error_message);
	case LogicalTypeId::DECIMAL:
		return DecimalCastSwitch(source, result, count, error_message);
	case LogicalTypeId::FLOAT:
		return NumericCastSwitch<float>(source, result, count, error_message);
	case LogicalTypeId::DOUBLE:
		return NumericCastSwitch<double>(source, result, count, error_message);
	case LogicalTypeId::DATE:
		return DateCastSwitch(source, result, count, error_message);
	case LogicalTypeId::TIME:
		return TimeCastSwitch(source, result, count, error_message);
	case LogicalTypeId::TIME_TZ:
		return TimeTzCastSwitch(source, result, count, error_message);
	case LogicalTypeId::TIMESTAMP:
		return TimestampCastSwitch(source, result, count, error_message);
	case LogicalTypeId::TIMESTAMP_TZ:
		return TimestampTzCastSwitch(source, result, count, error_message);
	case LogicalTypeId::TIMESTAMP_NS:
		return TimestampNsCastSwitch(source, result, count, error_message);
	case LogicalTypeId::TIMESTAMP_MS:
		return TimestampMsCastSwitch(source, result, count, error_message);
	case LogicalTypeId::TIMESTAMP_SEC:
		return TimestampSecCastSwitch(source, result, count, error_message);
	case LogicalTypeId::INTERVAL:
		return IntervalCastSwitch(source, result, count, error_message);
	case LogicalTypeId::JSON:
	case LogicalTypeId::VARCHAR:
		return StringCastSwitch(source, result, count, strict, error_message);
	case LogicalTypeId::BLOB:
		return BlobCastSwitch(source, result, count, error_message);
	case LogicalTypeId::SQLNULL: {
		// cast a NULL to another type, just copy the properties and change the type
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(result, true);
		return true;
	}
	case LogicalTypeId::MAP:
	case LogicalTypeId::STRUCT:
		return StructCastSwitch(source, result, count, error_message);
	case LogicalTypeId::LIST:
		return ListCastSwitch(source, result, count, error_message);
	case LogicalTypeId::ENUM:
		return EnumCastSwitch(source, result, count, error_message, strict);
	case LogicalTypeId::AGGREGATE_STATE:
		return AggregateStateToBlobCast(source, result, count, error_message, strict);
	default:
		return TryVectorNullCast(source, result, count, error_message);
	}
}

void VectorOperations::Cast(Vector &source, Vector &result, idx_t count, bool strict) {
	VectorOperations::TryCast(source, result, count, nullptr, strict);
}

} // namespace duckdb
//===--------------------------------------------------------------------===//
// copy.cpp
// Description: This file contains the implementation of the different copy
// functions
//===--------------------------------------------------------------------===//







namespace duckdb {

template <class T>
static void TemplatedCopy(const Vector &source, const SelectionVector &sel, Vector &target, idx_t source_offset,
                          idx_t target_offset, idx_t copy_count) {
	auto ldata = FlatVector::GetData<T>(source);
	auto tdata = FlatVector::GetData<T>(target);
	for (idx_t i = 0; i < copy_count; i++) {
		auto source_idx = sel.get_index(source_offset + i);
		tdata[target_offset + i] = ldata[source_idx];
	}
}

void VectorOperations::Copy(const Vector &source_p, Vector &target, const SelectionVector &sel_p, idx_t source_count,
                            idx_t source_offset, idx_t target_offset) {
	D_ASSERT(source_offset <= source_count);
	D_ASSERT(source_p.GetType() == target.GetType());
	idx_t copy_count = source_count - source_offset;

	SelectionVector owned_sel;
	const SelectionVector *sel = &sel_p;

	const Vector *source = &source_p;
	bool finished = false;
	while (!finished) {
		switch (source->GetVectorType()) {
		case VectorType::DICTIONARY_VECTOR: {
			// dictionary vector: merge selection vectors
			auto &child = DictionaryVector::Child(*source);
			auto &dict_sel = DictionaryVector::SelVector(*source);
			// merge the selection vectors and verify the child
			auto new_buffer = dict_sel.Slice(*sel, source_count);
			owned_sel.Initialize(new_buffer);
			sel = &owned_sel;
			source = &child;
			break;
		}
		case VectorType::SEQUENCE_VECTOR: {
			int64_t start, increment;
			Vector seq(source->GetType());
			SequenceVector::GetSequence(*source, start, increment);
			VectorOperations::GenerateSequence(seq, source_count, *sel, start, increment);
			VectorOperations::Copy(seq, target, *sel, source_count, source_offset, target_offset);
			return;
		}
		case VectorType::CONSTANT_VECTOR:
			sel = ConstantVector::ZeroSelectionVector(copy_count, owned_sel);
			finished = true;
			break;
		case VectorType::FLAT_VECTOR:
			finished = true;
			break;
		default:
			throw NotImplementedException("FIXME unimplemented vector type for VectorOperations::Copy");
		}
	}

	if (copy_count == 0) {
		return;
	}

	// Allow copying of a single value to constant vectors
	const auto target_vector_type = target.GetVectorType();
	if (copy_count == 1 && target_vector_type == VectorType::CONSTANT_VECTOR) {
		target_offset = 0;
		target.SetVectorType(VectorType::FLAT_VECTOR);
	}
	D_ASSERT(target.GetVectorType() == VectorType::FLAT_VECTOR);

	// first copy the nullmask
	auto &tmask = FlatVector::Validity(target);
	if (source->GetVectorType() == VectorType::CONSTANT_VECTOR) {
		const bool valid = !ConstantVector::IsNull(*source);
		for (idx_t i = 0; i < copy_count; i++) {
			tmask.Set(target_offset + i, valid);
		}
	} else {
		auto &smask = FlatVector::Validity(*source);
		if (smask.IsMaskSet()) {
			for (idx_t i = 0; i < copy_count; i++) {
				auto idx = sel->get_index(source_offset + i);

				if (smask.RowIsValid(idx)) {
					// set valid
					if (!tmask.AllValid()) {
						tmask.SetValidUnsafe(target_offset + i);
					}
				} else {
					// set invalid
					if (tmask.AllValid()) {
						auto init_size = MaxValue<idx_t>(STANDARD_VECTOR_SIZE, target_offset + copy_count);
						tmask.Initialize(init_size);
					}
					tmask.SetInvalidUnsafe(target_offset + i);
				}
			}
		}
	}

	D_ASSERT(sel);

	// now copy over the data
	switch (source->GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedCopy<int8_t>(*source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::INT16:
		TemplatedCopy<int16_t>(*source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::INT32:
		TemplatedCopy<int32_t>(*source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::INT64:
		TemplatedCopy<int64_t>(*source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::UINT8:
		TemplatedCopy<uint8_t>(*source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::UINT16:
		TemplatedCopy<uint16_t>(*source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::UINT32:
		TemplatedCopy<uint32_t>(*source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::UINT64:
		TemplatedCopy<uint64_t>(*source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::INT128:
		TemplatedCopy<hugeint_t>(*source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::FLOAT:
		TemplatedCopy<float>(*source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::DOUBLE:
		TemplatedCopy<double>(*source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::INTERVAL:
		TemplatedCopy<interval_t>(*source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::VARCHAR: {
		auto ldata = FlatVector::GetData<string_t>(*source);
		auto tdata = FlatVector::GetData<string_t>(target);
		for (idx_t i = 0; i < copy_count; i++) {
			auto source_idx = sel->get_index(source_offset + i);
			auto target_idx = target_offset + i;
			if (tmask.RowIsValid(target_idx)) {
				tdata[target_idx] = StringVector::AddStringOrBlob(target, ldata[source_idx]);
			}
		}
		break;
	}
	case PhysicalType::STRUCT: {
		auto &source_children = StructVector::GetEntries(*source);
		auto &target_children = StructVector::GetEntries(target);
		D_ASSERT(source_children.size() == target_children.size());
		for (idx_t i = 0; i < source_children.size(); i++) {
			VectorOperations::Copy(*source_children[i], *target_children[i], sel_p, source_count, source_offset,
			                       target_offset);
		}
		break;
	}
	case PhysicalType::LIST: {
		D_ASSERT(target.GetType().InternalType() == PhysicalType::LIST);

		auto &source_child = ListVector::GetEntry(*source);
		auto sdata = FlatVector::GetData<list_entry_t>(*source);
		auto tdata = FlatVector::GetData<list_entry_t>(target);

		if (target_vector_type == VectorType::CONSTANT_VECTOR) {
			// If we are only writing one value, then the copied values (if any) are contiguous
			// and we can just Append from the offset position
			if (!tmask.RowIsValid(target_offset)) {
				break;
			}
			auto source_idx = sel->get_index(source_offset);
			auto &source_entry = sdata[source_idx];
			const idx_t source_child_size = source_entry.length + source_entry.offset;

			//! overwrite constant target vectors.
			ListVector::SetListSize(target, 0);
			ListVector::Append(target, source_child, source_child_size, source_entry.offset);

			auto &target_entry = tdata[target_offset];
			target_entry.length = source_entry.length;
			target_entry.offset = 0;
		} else {
			//! if the source has list offsets, we need to append them to the target
			//! build a selection vector for the copied child elements
			vector<sel_t> child_rows;
			for (idx_t i = 0; i < copy_count; ++i) {
				if (tmask.RowIsValid(target_offset + i)) {
					auto source_idx = sel->get_index(source_offset + i);
					auto &source_entry = sdata[source_idx];
					for (idx_t j = 0; j < source_entry.length; ++j) {
						child_rows.emplace_back(source_entry.offset + j);
					}
				}
			}
			idx_t source_child_size = child_rows.size();
			SelectionVector child_sel(child_rows.data());

			idx_t old_target_child_len = ListVector::GetListSize(target);

			//! append to list itself
			ListVector::Append(target, source_child, child_sel, source_child_size);

			//! now write the list offsets
			for (idx_t i = 0; i < copy_count; i++) {
				auto source_idx = sel->get_index(source_offset + i);
				auto &source_entry = sdata[source_idx];
				auto &target_entry = tdata[target_offset + i];

				target_entry.length = source_entry.length;
				target_entry.offset = old_target_child_len;
				if (tmask.RowIsValid(target_offset + i)) {
					old_target_child_len += target_entry.length;
				}
			}
		}
		break;
	}
	default:
		throw NotImplementedException("Unimplemented type '%s' for copy!",
		                              TypeIdToString(source->GetType().InternalType()));
	}

	if (target_vector_type != VectorType::FLAT_VECTOR) {
		target.SetVectorType(target_vector_type);
	}
}

void VectorOperations::Copy(const Vector &source, Vector &target, idx_t source_count, idx_t source_offset,
                            idx_t target_offset) {
	VectorOperations::Copy(source, target, *FlatVector::IncrementalSelectionVector(), source_count, source_offset,
	                       target_offset);
}

} // namespace duckdb
//===--------------------------------------------------------------------===//
// hash.cpp
// Description: This file contains the vectorized hash implementations
//===--------------------------------------------------------------------===//







namespace duckdb {

struct HashOp {
	static const hash_t NULL_HASH = 0xbf58476d1ce4e5b9;

	template <class T>
	static inline hash_t Operation(T input, bool is_null) {
		return is_null ? NULL_HASH : duckdb::Hash<T>(input);
	}
};

static inline hash_t CombineHashScalar(hash_t a, hash_t b) {
	return (a * UINT64_C(0xbf58476d1ce4e5b9)) ^ b;
}

template <bool HAS_RSEL, class T>
static inline void TightLoopHash(T *__restrict ldata, hash_t *__restrict result_data, const SelectionVector *rsel,
                                 idx_t count, const SelectionVector *__restrict sel_vector, ValidityMask &mask) {
	if (!mask.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			auto ridx = HAS_RSEL ? rsel->get_index(i) : i;
			auto idx = sel_vector->get_index(ridx);
			result_data[ridx] = HashOp::Operation(ldata[idx], !mask.RowIsValid(idx));
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto ridx = HAS_RSEL ? rsel->get_index(i) : i;
			auto idx = sel_vector->get_index(ridx);
			result_data[ridx] = duckdb::Hash<T>(ldata[idx]);
		}
	}
}

template <bool HAS_RSEL, class T>
static inline void TemplatedLoopHash(Vector &input, Vector &result, const SelectionVector *rsel, idx_t count) {
	if (input.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);

		auto ldata = ConstantVector::GetData<T>(input);
		auto result_data = ConstantVector::GetData<hash_t>(result);
		*result_data = HashOp::Operation(*ldata, ConstantVector::IsNull(input));
	} else {
		result.SetVectorType(VectorType::FLAT_VECTOR);

		VectorData idata;
		input.Orrify(count, idata);

		TightLoopHash<HAS_RSEL, T>((T *)idata.data, FlatVector::GetData<hash_t>(result), rsel, count, idata.sel,
		                           idata.validity);
	}
}

template <bool HAS_RSEL, bool FIRST_HASH>
static inline void StructLoopHash(Vector &input, Vector &hashes, const SelectionVector *rsel, idx_t count) {
	auto &children = StructVector::GetEntries(input);

	D_ASSERT(!children.empty());
	idx_t col_no = 0;
	if (HAS_RSEL) {
		if (FIRST_HASH) {
			VectorOperations::Hash(*children[col_no++], hashes, *rsel, count);
		} else {
			VectorOperations::CombineHash(hashes, *children[col_no++], *rsel, count);
		}
		while (col_no < children.size()) {
			VectorOperations::CombineHash(hashes, *children[col_no++], *rsel, count);
		}
	} else {
		if (FIRST_HASH) {
			VectorOperations::Hash(*children[col_no++], hashes, count);
		} else {
			VectorOperations::CombineHash(hashes, *children[col_no++], count);
		}
		while (col_no < children.size()) {
			VectorOperations::CombineHash(hashes, *children[col_no++], count);
		}
	}
}

template <bool HAS_RSEL, bool FIRST_HASH>
static inline void ListLoopHash(Vector &input, Vector &hashes, const SelectionVector *rsel, idx_t count) {
	auto hdata = FlatVector::GetData<hash_t>(hashes);

	VectorData idata;
	input.Orrify(count, idata);
	const auto ldata = (const list_entry_t *)idata.data;

	// Hash the children into a temporary
	auto &child = ListVector::GetEntry(input);
	const auto child_count = ListVector::GetListSize(input);

	Vector child_hashes(LogicalType::HASH, child_count);
	VectorOperations::Hash(child, child_hashes, child_count);
	auto chdata = FlatVector::GetData<hash_t>(child_hashes);

	// Reduce the number of entries to check to the non-empty ones
	SelectionVector unprocessed(count);
	SelectionVector cursor(HAS_RSEL ? STANDARD_VECTOR_SIZE : count);
	idx_t remaining = 0;
	for (idx_t i = 0; i < count; ++i) {
		const idx_t ridx = HAS_RSEL ? rsel->get_index(i) : i;
		const auto lidx = idata.sel->get_index(ridx);
		const auto &entry = ldata[lidx];
		if (idata.validity.RowIsValid(lidx) && entry.length > 0) {
			unprocessed.set_index(remaining++, ridx);
			cursor.set_index(ridx, entry.offset);
		} else if (FIRST_HASH) {
			hdata[ridx] = HashOp::NULL_HASH;
		}
		// Empty or NULL non-first elements have no effect.
	}

	count = remaining;
	if (count == 0) {
		return;
	}

	// Merge the first position hash into the main hash
	idx_t position = 1;
	if (FIRST_HASH) {
		remaining = 0;
		for (idx_t i = 0; i < count; ++i) {
			const auto ridx = unprocessed.get_index(i);
			const auto cidx = cursor.get_index(ridx);
			hdata[ridx] = chdata[cidx];

			const auto lidx = idata.sel->get_index(ridx);
			const auto &entry = ldata[lidx];
			if (entry.length > position) {
				// Entry still has values to hash
				unprocessed.set_index(remaining++, ridx);
				cursor.set_index(ridx, cidx + 1);
			}
		}
		count = remaining;
		if (count == 0) {
			return;
		}
		++position;
	}

	// Combine the hashes for the remaining positions until there are none left
	for (;; ++position) {
		remaining = 0;
		for (idx_t i = 0; i < count; ++i) {
			const auto ridx = unprocessed.get_index(i);
			const auto cidx = cursor.get_index(ridx);
			hdata[ridx] = CombineHashScalar(hdata[ridx], chdata[cidx]);

			const auto lidx = idata.sel->get_index(ridx);
			const auto &entry = ldata[lidx];
			if (entry.length > position) {
				// Entry still has values to hash
				unprocessed.set_index(remaining++, ridx);
				cursor.set_index(ridx, cidx + 1);
			}
		}

		count = remaining;
		if (count == 0) {
			break;
		}
	}
}

template <bool HAS_RSEL>
static inline void HashTypeSwitch(Vector &input, Vector &result, const SelectionVector *rsel, idx_t count) {
	D_ASSERT(result.GetType().id() == LogicalType::HASH);
	switch (input.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedLoopHash<HAS_RSEL, int8_t>(input, result, rsel, count);
		break;
	case PhysicalType::INT16:
		TemplatedLoopHash<HAS_RSEL, int16_t>(input, result, rsel, count);
		break;
	case PhysicalType::INT32:
		TemplatedLoopHash<HAS_RSEL, int32_t>(input, result, rsel, count);
		break;
	case PhysicalType::INT64:
		TemplatedLoopHash<HAS_RSEL, int64_t>(input, result, rsel, count);
		break;
	case PhysicalType::UINT8:
		TemplatedLoopHash<HAS_RSEL, uint8_t>(input, result, rsel, count);
		break;
	case PhysicalType::UINT16:
		TemplatedLoopHash<HAS_RSEL, uint16_t>(input, result, rsel, count);
		break;
	case PhysicalType::UINT32:
		TemplatedLoopHash<HAS_RSEL, uint32_t>(input, result, rsel, count);
		break;
	case PhysicalType::UINT64:
		TemplatedLoopHash<HAS_RSEL, uint64_t>(input, result, rsel, count);
		break;
	case PhysicalType::INT128:
		TemplatedLoopHash<HAS_RSEL, hugeint_t>(input, result, rsel, count);
		break;
	case PhysicalType::FLOAT:
		TemplatedLoopHash<HAS_RSEL, float>(input, result, rsel, count);
		break;
	case PhysicalType::DOUBLE:
		TemplatedLoopHash<HAS_RSEL, double>(input, result, rsel, count);
		break;
	case PhysicalType::INTERVAL:
		TemplatedLoopHash<HAS_RSEL, interval_t>(input, result, rsel, count);
		break;
	case PhysicalType::VARCHAR:
		TemplatedLoopHash<HAS_RSEL, string_t>(input, result, rsel, count);
		break;
	case PhysicalType::MAP:
	case PhysicalType::STRUCT:
		StructLoopHash<HAS_RSEL, true>(input, result, rsel, count);
		break;
	case PhysicalType::LIST:
		ListLoopHash<HAS_RSEL, true>(input, result, rsel, count);
		break;
	default:
		throw InvalidTypeException(input.GetType(), "Invalid type for hash");
	}
}

void VectorOperations::Hash(Vector &input, Vector &result, idx_t count) {
	HashTypeSwitch<false>(input, result, nullptr, count);
}

void VectorOperations::Hash(Vector &input, Vector &result, const SelectionVector &sel, idx_t count) {
	HashTypeSwitch<true>(input, result, &sel, count);
}

template <bool HAS_RSEL, class T>
static inline void TightLoopCombineHashConstant(T *__restrict ldata, hash_t constant_hash, hash_t *__restrict hash_data,
                                                const SelectionVector *rsel, idx_t count,
                                                const SelectionVector *__restrict sel_vector, ValidityMask &mask) {
	if (!mask.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			auto ridx = HAS_RSEL ? rsel->get_index(i) : i;
			auto idx = sel_vector->get_index(ridx);
			auto other_hash = HashOp::Operation(ldata[idx], !mask.RowIsValid(idx));
			hash_data[ridx] = CombineHashScalar(constant_hash, other_hash);
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto ridx = HAS_RSEL ? rsel->get_index(i) : i;
			auto idx = sel_vector->get_index(ridx);
			auto other_hash = duckdb::Hash<T>(ldata[idx]);
			hash_data[ridx] = CombineHashScalar(constant_hash, other_hash);
		}
	}
}

template <bool HAS_RSEL, class T>
static inline void TightLoopCombineHash(T *__restrict ldata, hash_t *__restrict hash_data, const SelectionVector *rsel,
                                        idx_t count, const SelectionVector *__restrict sel_vector, ValidityMask &mask) {
	if (!mask.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			auto ridx = HAS_RSEL ? rsel->get_index(i) : i;
			auto idx = sel_vector->get_index(ridx);
			auto other_hash = HashOp::Operation(ldata[idx], !mask.RowIsValid(idx));
			hash_data[ridx] = CombineHashScalar(hash_data[ridx], other_hash);
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto ridx = HAS_RSEL ? rsel->get_index(i) : i;
			auto idx = sel_vector->get_index(ridx);
			auto other_hash = duckdb::Hash<T>(ldata[idx]);
			hash_data[ridx] = CombineHashScalar(hash_data[ridx], other_hash);
		}
	}
}

template <bool HAS_RSEL, class T>
void TemplatedLoopCombineHash(Vector &input, Vector &hashes, const SelectionVector *rsel, idx_t count) {
	if (input.GetVectorType() == VectorType::CONSTANT_VECTOR && hashes.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		auto ldata = ConstantVector::GetData<T>(input);
		auto hash_data = ConstantVector::GetData<hash_t>(hashes);

		auto other_hash = HashOp::Operation(*ldata, ConstantVector::IsNull(input));
		*hash_data = CombineHashScalar(*hash_data, other_hash);
	} else {
		VectorData idata;
		input.Orrify(count, idata);
		if (hashes.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			// mix constant with non-constant, first get the constant value
			auto constant_hash = *ConstantVector::GetData<hash_t>(hashes);
			// now re-initialize the hashes vector to an empty flat vector
			hashes.SetVectorType(VectorType::FLAT_VECTOR);
			TightLoopCombineHashConstant<HAS_RSEL, T>((T *)idata.data, constant_hash,
			                                          FlatVector::GetData<hash_t>(hashes), rsel, count, idata.sel,
			                                          idata.validity);
		} else {
			D_ASSERT(hashes.GetVectorType() == VectorType::FLAT_VECTOR);
			TightLoopCombineHash<HAS_RSEL, T>((T *)idata.data, FlatVector::GetData<hash_t>(hashes), rsel, count,
			                                  idata.sel, idata.validity);
		}
	}
}

template <bool HAS_RSEL>
static inline void CombineHashTypeSwitch(Vector &hashes, Vector &input, const SelectionVector *rsel, idx_t count) {
	D_ASSERT(hashes.GetType().id() == LogicalType::HASH);
	switch (input.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedLoopCombineHash<HAS_RSEL, int8_t>(input, hashes, rsel, count);
		break;
	case PhysicalType::INT16:
		TemplatedLoopCombineHash<HAS_RSEL, int16_t>(input, hashes, rsel, count);
		break;
	case PhysicalType::INT32:
		TemplatedLoopCombineHash<HAS_RSEL, int32_t>(input, hashes, rsel, count);
		break;
	case PhysicalType::INT64:
		TemplatedLoopCombineHash<HAS_RSEL, int64_t>(input, hashes, rsel, count);
		break;
	case PhysicalType::UINT8:
		TemplatedLoopCombineHash<HAS_RSEL, uint8_t>(input, hashes, rsel, count);
		break;
	case PhysicalType::UINT16:
		TemplatedLoopCombineHash<HAS_RSEL, uint16_t>(input, hashes, rsel, count);
		break;
	case PhysicalType::UINT32:
		TemplatedLoopCombineHash<HAS_RSEL, uint32_t>(input, hashes, rsel, count);
		break;
	case PhysicalType::UINT64:
		TemplatedLoopCombineHash<HAS_RSEL, uint64_t>(input, hashes, rsel, count);
		break;
	case PhysicalType::INT128:
		TemplatedLoopCombineHash<HAS_RSEL, hugeint_t>(input, hashes, rsel, count);
		break;
	case PhysicalType::FLOAT:
		TemplatedLoopCombineHash<HAS_RSEL, float>(input, hashes, rsel, count);
		break;
	case PhysicalType::DOUBLE:
		TemplatedLoopCombineHash<HAS_RSEL, double>(input, hashes, rsel, count);
		break;
	case PhysicalType::INTERVAL:
		TemplatedLoopCombineHash<HAS_RSEL, interval_t>(input, hashes, rsel, count);
		break;
	case PhysicalType::VARCHAR:
		TemplatedLoopCombineHash<HAS_RSEL, string_t>(input, hashes, rsel, count);
		break;
	case PhysicalType::MAP:
	case PhysicalType::STRUCT:
		StructLoopHash<HAS_RSEL, false>(input, hashes, rsel, count);
		break;
	case PhysicalType::LIST:
		ListLoopHash<HAS_RSEL, false>(input, hashes, rsel, count);
		break;
	default:
		throw InvalidTypeException(input.GetType(), "Invalid type for hash");
	}
}

void VectorOperations::CombineHash(Vector &hashes, Vector &input, idx_t count) {
	CombineHashTypeSwitch<false>(hashes, input, nullptr, count);
}

void VectorOperations::CombineHash(Vector &hashes, Vector &input, const SelectionVector &rsel, idx_t count) {
	CombineHashTypeSwitch<true>(hashes, input, &rsel, count);
}

} // namespace duckdb




namespace duckdb {

template <class T>
static void CopyToStorageLoop(VectorData &vdata, idx_t count, data_ptr_t target) {
	auto ldata = (T *)vdata.data;
	auto result_data = (T *)target;
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		if (!vdata.validity.RowIsValid(idx)) {
			result_data[i] = NullValue<T>();
		} else {
			result_data[i] = ldata[idx];
		}
	}
}

void VectorOperations::WriteToStorage(Vector &source, idx_t count, data_ptr_t target) {
	if (count == 0) {
		return;
	}
	VectorData vdata;
	source.Orrify(count, vdata);

	switch (source.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		CopyToStorageLoop<int8_t>(vdata, count, target);
		break;
	case PhysicalType::INT16:
		CopyToStorageLoop<int16_t>(vdata, count, target);
		break;
	case PhysicalType::INT32:
		CopyToStorageLoop<int32_t>(vdata, count, target);
		break;
	case PhysicalType::INT64:
		CopyToStorageLoop<int64_t>(vdata, count, target);
		break;
	case PhysicalType::UINT8:
		CopyToStorageLoop<uint8_t>(vdata, count, target);
		break;
	case PhysicalType::UINT16:
		CopyToStorageLoop<uint16_t>(vdata, count, target);
		break;
	case PhysicalType::UINT32:
		CopyToStorageLoop<uint32_t>(vdata, count, target);
		break;
	case PhysicalType::UINT64:
		CopyToStorageLoop<uint64_t>(vdata, count, target);
		break;
	case PhysicalType::INT128:
		CopyToStorageLoop<hugeint_t>(vdata, count, target);
		break;
	case PhysicalType::FLOAT:
		CopyToStorageLoop<float>(vdata, count, target);
		break;
	case PhysicalType::DOUBLE:
		CopyToStorageLoop<double>(vdata, count, target);
		break;
	case PhysicalType::INTERVAL:
		CopyToStorageLoop<interval_t>(vdata, count, target);
		break;
	default:
		throw NotImplementedException("Unimplemented type for WriteToStorage");
	}
}

template <class T>
static void ReadFromStorageLoop(data_ptr_t source, idx_t count, Vector &result) {
	auto ldata = (T *)source;
	auto result_data = FlatVector::GetData<T>(result);
	for (idx_t i = 0; i < count; i++) {
		result_data[i] = ldata[i];
	}
}

void VectorOperations::ReadFromStorage(data_ptr_t source, idx_t count, Vector &result) {
	result.SetVectorType(VectorType::FLAT_VECTOR);
	switch (result.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		ReadFromStorageLoop<int8_t>(source, count, result);
		break;
	case PhysicalType::INT16:
		ReadFromStorageLoop<int16_t>(source, count, result);
		break;
	case PhysicalType::INT32:
		ReadFromStorageLoop<int32_t>(source, count, result);
		break;
	case PhysicalType::INT64:
		ReadFromStorageLoop<int64_t>(source, count, result);
		break;
	case PhysicalType::UINT8:
		ReadFromStorageLoop<uint8_t>(source, count, result);
		break;
	case PhysicalType::UINT16:
		ReadFromStorageLoop<uint16_t>(source, count, result);
		break;
	case PhysicalType::UINT32:
		ReadFromStorageLoop<uint32_t>(source, count, result);
		break;
	case PhysicalType::UINT64:
		ReadFromStorageLoop<uint64_t>(source, count, result);
		break;
	case PhysicalType::INT128:
		ReadFromStorageLoop<hugeint_t>(source, count, result);
		break;
	case PhysicalType::FLOAT:
		ReadFromStorageLoop<float>(source, count, result);
		break;
	case PhysicalType::DOUBLE:
		ReadFromStorageLoop<double>(source, count, result);
		break;
	case PhysicalType::INTERVAL:
		ReadFromStorageLoop<interval_t>(source, count, result);
		break;
	default:
		throw NotImplementedException("Unimplemented type for ReadFromStorage");
	}
}

} // namespace duckdb






namespace duckdb {

VirtualFileSystem::VirtualFileSystem() : default_fs(FileSystem::CreateLocal()) {
	RegisterSubSystem(FileCompressionType::GZIP, make_unique<GZipFileSystem>());
}

unique_ptr<FileHandle> VirtualFileSystem::OpenFile(const string &path, uint8_t flags, FileLockType lock,
                                                   FileCompressionType compression, FileOpener *opener) {
	if (compression == FileCompressionType::AUTO_DETECT) {
		// auto detect compression settings based on file name
		auto lower_path = StringUtil::Lower(path);
		if (StringUtil::EndsWith(lower_path, ".gz")) {
			compression = FileCompressionType::GZIP;
		} else if (StringUtil::EndsWith(lower_path, ".zst")) {
			compression = FileCompressionType::ZSTD;
		} else {
			compression = FileCompressionType::UNCOMPRESSED;
		}
	}
	// open the base file handle
	auto file_handle = FindFileSystem(path)->OpenFile(path, flags, lock, FileCompressionType::UNCOMPRESSED, opener);
	if (file_handle->GetType() == FileType::FILE_TYPE_FIFO) {
		file_handle = PipeFileSystem::OpenPipe(move(file_handle));
	} else if (compression != FileCompressionType::UNCOMPRESSED) {
		auto entry = compressed_fs.find(compression);
		if (entry == compressed_fs.end()) {
			throw NotImplementedException(
			    "Attempting to open a compressed file, but the compression type is not supported");
		}
		file_handle = entry->second->OpenCompressedFile(move(file_handle), flags & FileFlags::FILE_FLAGS_WRITE);
	}
	return file_handle;
}

} // namespace duckdb


namespace duckdb {

#ifdef DUCKDB_WINDOWS

std::wstring WindowsUtil::UTF8ToUnicode(const char *input) {
	idx_t result_size;

	result_size = MultiByteToWideChar(CP_UTF8, 0, input, -1, nullptr, 0);
	if (result_size == 0) {
		throw IOException("Failure in MultiByteToWideChar");
	}
	auto buffer = unique_ptr<wchar_t[]>(new wchar_t[result_size]);
	result_size = MultiByteToWideChar(CP_UTF8, 0, input, -1, buffer.get(), result_size);
	if (result_size == 0) {
		throw IOException("Failure in MultiByteToWideChar");
	}
	return std::wstring(buffer.get(), result_size);
}

static string WideCharToMultiByteWrapper(LPCWSTR input, uint32_t code_page) {
	idx_t result_size;

	result_size = WideCharToMultiByte(code_page, 0, input, -1, 0, 0, 0, 0);
	if (result_size == 0) {
		throw IOException("Failure in WideCharToMultiByte");
	}
	auto buffer = unique_ptr<char[]>(new char[result_size]);
	result_size = WideCharToMultiByte(code_page, 0, input, -1, buffer.get(), result_size, 0, 0);
	if (result_size == 0) {
		throw IOException("Failure in WideCharToMultiByte");
	}
	return string(buffer.get(), result_size - 1);
}

string WindowsUtil::UnicodeToUTF8(LPCWSTR input) {
	return WideCharToMultiByteWrapper(input, CP_UTF8);
}

static string WindowsUnicodeToMBCS(LPCWSTR unicode_text, int use_ansi) {
	uint32_t code_page = use_ansi ? CP_ACP : CP_OEMCP;
	return WideCharToMultiByteWrapper(unicode_text, code_page);
}

string WindowsUtil::UTF8ToMBCS(const char *input, bool use_ansi) {
	auto unicode = WindowsUtil::UTF8ToUnicode(input);
	return WindowsUnicodeToMBCS(unicode.c_str(), use_ansi);
}

#endif

} // namespace duckdb



#include <vector>

namespace duckdb {

AdaptiveFilter::AdaptiveFilter(const Expression &expr)
    : iteration_count(0), observe_interval(10), execute_interval(20), warmup(true) {
	auto &conj_expr = (const BoundConjunctionExpression &)expr;
	D_ASSERT(conj_expr.children.size() > 1);
	for (idx_t idx = 0; idx < conj_expr.children.size(); idx++) {
		permutation.push_back(idx);
		if (idx != conj_expr.children.size() - 1) {
			swap_likeliness.push_back(100);
		}
	}
	right_random_border = 100 * (conj_expr.children.size() - 1);
}

AdaptiveFilter::AdaptiveFilter(TableFilterSet *table_filters)
    : iteration_count(0), observe_interval(10), execute_interval(20), warmup(true) {
	for (auto &table_filter : table_filters->filters) {
		permutation.push_back(table_filter.first);
		swap_likeliness.push_back(100);
	}
	swap_likeliness.pop_back();
	right_random_border = 100 * (table_filters->filters.size() - 1);
}
void AdaptiveFilter::AdaptRuntimeStatistics(double duration) {
	iteration_count++;
	runtime_sum += duration;

	if (!warmup) {
		// the last swap was observed
		if (observe && iteration_count == observe_interval) {
			// keep swap if runtime decreased, else reverse swap
			if (prev_mean - (runtime_sum / iteration_count) <= 0) {
				// reverse swap because runtime didn't decrease
				std::swap(permutation[swap_idx], permutation[swap_idx + 1]);

				// decrease swap likeliness, but make sure there is always a small likeliness left
				if (swap_likeliness[swap_idx] > 1) {
					swap_likeliness[swap_idx] /= 2;
				}
			} else {
				// keep swap because runtime decreased, reset likeliness
				swap_likeliness[swap_idx] = 100;
			}
			observe = false;

			// reset values
			iteration_count = 0;
			runtime_sum = 0.0;
		} else if (!observe && iteration_count == execute_interval) {
			// save old mean to evaluate swap
			prev_mean = runtime_sum / iteration_count;

			// get swap index and swap likeliness
			std::uniform_int_distribution<int> distribution(1, right_random_border); // a <= i <= b
			idx_t random_number = distribution(generator) - 1;

			swap_idx = random_number / 100;                    // index to be swapped
			idx_t likeliness = random_number - 100 * swap_idx; // random number between [0, 100)

			// check if swap is going to happen
			if (swap_likeliness[swap_idx] > likeliness) { // always true for the first swap of an index
				// swap
				std::swap(permutation[swap_idx], permutation[swap_idx + 1]);

				// observe whether swap will be applied
				observe = true;
			}

			// reset values
			iteration_count = 0;
			runtime_sum = 0.0;
		}
	} else {
		if (iteration_count == 5) {
			// initially set all values
			iteration_count = 0;
			runtime_sum = 0.0;
			observe = false;
			warmup = false;
		}
	}
}

} // namespace duckdb














#include <cmath>

namespace duckdb {

using ValidityBytes = RowLayout::ValidityBytes;

GroupedAggregateHashTable::GroupedAggregateHashTable(Allocator &allocator, BufferManager &buffer_manager,
                                                     vector<LogicalType> group_types, vector<LogicalType> payload_types,
                                                     const vector<BoundAggregateExpression *> &bindings,
                                                     HtEntryType entry_type)
    : GroupedAggregateHashTable(allocator, buffer_manager, move(group_types), move(payload_types),
                                AggregateObject::CreateAggregateObjects(bindings), entry_type) {
}

GroupedAggregateHashTable::GroupedAggregateHashTable(Allocator &allocator, BufferManager &buffer_manager,
                                                     vector<LogicalType> group_types)
    : GroupedAggregateHashTable(allocator, buffer_manager, move(group_types), {}, vector<AggregateObject>()) {
}

GroupedAggregateHashTable::GroupedAggregateHashTable(Allocator &allocator, BufferManager &buffer_manager,
                                                     vector<LogicalType> group_types_p,
                                                     vector<LogicalType> payload_types_p,
                                                     vector<AggregateObject> aggregate_objects_p,
                                                     HtEntryType entry_type)
    : BaseAggregateHashTable(allocator, aggregate_objects_p, buffer_manager, move(payload_types_p)),
      entry_type(entry_type), capacity(0), entries(0), payload_page_offset(0), is_finalized(false),
      ht_offsets(LogicalTypeId::BIGINT), hash_salts(LogicalTypeId::SMALLINT),
      group_compare_vector(STANDARD_VECTOR_SIZE), no_match_vector(STANDARD_VECTOR_SIZE),
      empty_vector(STANDARD_VECTOR_SIZE) {

	// Append hash column to the end and initialise the row layout
	group_types_p.emplace_back(LogicalType::HASH);
	layout.Initialize(move(group_types_p), move(aggregate_objects_p));

	// HT layout
	hash_offset = layout.GetOffsets()[layout.ColumnCount() - 1];

	tuple_size = layout.GetRowWidth();

	D_ASSERT(tuple_size <= Storage::BLOCK_SIZE);
	tuples_per_block = Storage::BLOCK_SIZE / tuple_size;
	hashes_hdl = buffer_manager.Allocate(Storage::BLOCK_SIZE);
	hashes_hdl_ptr = hashes_hdl.Ptr();

	switch (entry_type) {
	case HtEntryType::HT_WIDTH_64: {
		hash_prefix_shift = (HASH_WIDTH - sizeof(aggr_ht_entry_64::salt)) * 8;
		Resize<aggr_ht_entry_64>(STANDARD_VECTOR_SIZE * 2);
		break;
	}
	case HtEntryType::HT_WIDTH_32: {
		hash_prefix_shift = (HASH_WIDTH - sizeof(aggr_ht_entry_32::salt)) * 8;
		Resize<aggr_ht_entry_32>(STANDARD_VECTOR_SIZE * 2);
		break;
	}
	default:
		throw InternalException("Unknown HT entry width");
	}

	// create additional hash tables for distinct aggrs
	auto &aggregates = layout.GetAggregates();
	distinct_hashes.resize(aggregates.size());

	idx_t payload_idx = 0;
	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto &aggr = aggregates[i];
		if (aggr.distinct) {
			// layout types minus hash column plus aggr return type
			vector<LogicalType> distinct_group_types(layout.GetTypes());
			(void)distinct_group_types.pop_back();
			for (idx_t child_idx = 0; child_idx < aggr.child_count; child_idx++) {
				distinct_group_types.push_back(payload_types[payload_idx + child_idx]);
			}
			distinct_hashes[i] =
			    make_unique<GroupedAggregateHashTable>(allocator, buffer_manager, distinct_group_types);
		}
		payload_idx += aggr.child_count;
	}
	predicates.resize(layout.ColumnCount() - 1, ExpressionType::COMPARE_EQUAL);
	string_heap = make_unique<RowDataCollection>(buffer_manager, (idx_t)Storage::BLOCK_SIZE, 1, true);
}

GroupedAggregateHashTable::~GroupedAggregateHashTable() {
	Destroy();
}

template <class FUNC>
void GroupedAggregateHashTable::PayloadApply(FUNC fun) {
	if (entries == 0) {
		return;
	}
	idx_t apply_entries = entries;
	idx_t page_nr = 0;
	idx_t page_offset = 0;

	for (auto &payload_chunk_ptr : payload_hds_ptrs) {
		auto this_entries = MinValue(tuples_per_block, apply_entries);
		page_offset = 0;
		for (data_ptr_t ptr = payload_chunk_ptr, end = payload_chunk_ptr + this_entries * tuple_size; ptr < end;
		     ptr += tuple_size) {
			fun(page_nr, page_offset++, ptr);
		}
		apply_entries -= this_entries;
		page_nr++;
	}
	D_ASSERT(apply_entries == 0);
}

void GroupedAggregateHashTable::NewBlock() {
	auto pin = buffer_manager.Allocate(Storage::BLOCK_SIZE);
	payload_hds.push_back(move(pin));
	payload_hds_ptrs.push_back(payload_hds.back().Ptr());
	payload_page_offset = 0;
}

void GroupedAggregateHashTable::Destroy() {
	// check if there is a destructor
	bool has_destructor = false;
	for (auto &aggr : layout.GetAggregates()) {
		if (aggr.function.destructor) {
			has_destructor = true;
		}
	}
	if (!has_destructor) {
		return;
	}
	// there are aggregates with destructors: loop over the hash table
	// and call the destructor method for each of the aggregates
	data_ptr_t data_pointers[STANDARD_VECTOR_SIZE];
	Vector state_vector(LogicalType::POINTER, (data_ptr_t)data_pointers);
	idx_t count = 0;

	PayloadApply([&](idx_t page_nr, idx_t page_offset, data_ptr_t ptr) {
		data_pointers[count++] = ptr;
		if (count == STANDARD_VECTOR_SIZE) {
			RowOperations::DestroyStates(layout, state_vector, count);
			count = 0;
		}
	});
	RowOperations::DestroyStates(layout, state_vector, count);
}

template <class ENTRY>
void GroupedAggregateHashTable::VerifyInternal() {
	auto hashes_ptr = (ENTRY *)hashes_hdl_ptr;
	D_ASSERT(payload_hds.size() == payload_hds_ptrs.size());
	idx_t count = 0;
	for (idx_t i = 0; i < capacity; i++) {
		if (hashes_ptr[i].page_nr > 0) {
			D_ASSERT(hashes_ptr[i].page_offset < tuples_per_block);
			D_ASSERT(hashes_ptr[i].page_nr <= payload_hds.size());
			auto ptr = payload_hds_ptrs[hashes_ptr[i].page_nr - 1] + ((hashes_ptr[i].page_offset) * tuple_size);
			auto hash = Load<hash_t>(ptr + hash_offset);
			D_ASSERT((hashes_ptr[i].salt) == (hash >> hash_prefix_shift));

			count++;
		}
	}
	D_ASSERT(count == entries);
}

idx_t GroupedAggregateHashTable::MaxCapacity() {
	idx_t max_pages = 0;
	idx_t max_tuples = 0;

	switch (entry_type) {
	case HtEntryType::HT_WIDTH_32:
		max_pages = NumericLimits<uint8_t>::Maximum();
		max_tuples = NumericLimits<uint16_t>::Maximum();
		break;
	default:
		D_ASSERT(entry_type == HtEntryType::HT_WIDTH_64);
		max_pages = NumericLimits<uint32_t>::Maximum();
		max_tuples = NumericLimits<uint16_t>::Maximum();
		break;
	}

	return max_pages * MinValue(max_tuples, (idx_t)Storage::BLOCK_SIZE / tuple_size);
}

void GroupedAggregateHashTable::Verify() {
#ifdef DEBUG
	switch (entry_type) {
	case HtEntryType::HT_WIDTH_32:
		VerifyInternal<aggr_ht_entry_32>();
		break;
	case HtEntryType::HT_WIDTH_64:
		VerifyInternal<aggr_ht_entry_64>();
		break;
	}
#endif
}

template <class ENTRY>
void GroupedAggregateHashTable::Resize(idx_t size) {
	Verify();

	D_ASSERT(!is_finalized);

	if (size <= capacity) {
		throw InternalException("Cannot downsize a hash table!");
	}
	D_ASSERT(size >= STANDARD_VECTOR_SIZE);

	// size needs to be a power of 2
	D_ASSERT((size & (size - 1)) == 0);
	bitmask = size - 1;

	auto byte_size = size * sizeof(ENTRY);
	if (byte_size > (idx_t)Storage::BLOCK_SIZE) {
		hashes_hdl = buffer_manager.Allocate(byte_size);
		hashes_hdl_ptr = hashes_hdl.Ptr();
	}
	memset(hashes_hdl_ptr, 0, byte_size);
	hashes_end_ptr = hashes_hdl_ptr + byte_size;
	capacity = size;

	auto hashes_arr = (ENTRY *)hashes_hdl_ptr;

	PayloadApply([&](idx_t page_nr, idx_t page_offset, data_ptr_t ptr) {
		auto hash = Load<hash_t>(ptr + hash_offset);
		D_ASSERT((hash & bitmask) == (hash % capacity));
		auto entry_idx = (idx_t)hash & bitmask;
		while (hashes_arr[entry_idx].page_nr > 0) {
			entry_idx++;
			if (entry_idx >= capacity) {
				entry_idx = 0;
			}
		}

		D_ASSERT(!hashes_arr[entry_idx].page_nr);
		D_ASSERT(hash >> hash_prefix_shift <= NumericLimits<uint16_t>::Maximum());

		hashes_arr[entry_idx].salt = hash >> hash_prefix_shift;
		hashes_arr[entry_idx].page_nr = page_nr + 1;
		hashes_arr[entry_idx].page_offset = page_offset;
	});

	Verify();
}

idx_t GroupedAggregateHashTable::AddChunk(DataChunk &groups, DataChunk &payload) {
	Vector hashes(LogicalType::HASH);
	groups.Hash(hashes);

	return AddChunk(groups, hashes, payload);
}

idx_t GroupedAggregateHashTable::AddChunk(DataChunk &groups, Vector &group_hashes, DataChunk &payload) {
	D_ASSERT(!is_finalized);

	if (groups.size() == 0) {
		return 0;
	}
	// dummy
	SelectionVector new_groups(STANDARD_VECTOR_SIZE);

	D_ASSERT(groups.ColumnCount() + 1 == layout.ColumnCount());
	for (idx_t i = 0; i < groups.ColumnCount(); i++) {
		D_ASSERT(groups.GetTypes()[i] == layout.GetTypes()[i]);
	}

	Vector addresses(LogicalType::POINTER);
	auto new_group_count = FindOrCreateGroups(groups, group_hashes, addresses, new_groups);
	VectorOperations::AddInPlace(addresses, layout.GetAggrOffset(), payload.size());

	// now every cell has an entry
	// update the aggregates
	idx_t payload_idx = 0;

	auto &aggregates = layout.GetAggregates();
	for (idx_t aggr_idx = 0; aggr_idx < aggregates.size(); aggr_idx++) {
		// for any entries for which a group was found, update the aggregate
		auto &aggr = aggregates[aggr_idx];
		if (aggr.distinct) {
			// construct chunk for secondary hash table probing
			vector<LogicalType> probe_types(groups.GetTypes());
			for (idx_t i = 0; i < aggr.child_count; i++) {
				probe_types.push_back(payload_types[payload_idx + i]);
			}
			DataChunk probe_chunk;
			probe_chunk.Initialize(Allocator::DefaultAllocator(), probe_types);
			for (idx_t group_idx = 0; group_idx < groups.ColumnCount(); group_idx++) {
				probe_chunk.data[group_idx].Reference(groups.data[group_idx]);
			}
			for (idx_t i = 0; i < aggr.child_count; i++) {
				probe_chunk.data[groups.ColumnCount() + i].Reference(payload.data[payload_idx + i]);
			}
			probe_chunk.SetCardinality(groups);
			probe_chunk.Verify();

			Vector dummy_addresses(LogicalType::POINTER);
			// this is the actual meat, find out which groups plus payload
			// value have not been seen yet
			idx_t new_group_count =
			    distinct_hashes[aggr_idx]->FindOrCreateGroups(probe_chunk, dummy_addresses, new_groups);
			if (new_group_count > 0) {
				// now fix up the payload and addresses accordingly by creating
				// a selection vector
				DataChunk distinct_payload;
				distinct_payload.Initialize(Allocator::DefaultAllocator(), payload.GetTypes());
				distinct_payload.Slice(payload, new_groups, new_group_count);
				distinct_payload.Verify();

				Vector distinct_addresses(addresses, new_groups, new_group_count);
				distinct_addresses.Verify(new_group_count);

				if (aggr.filter) {
					distinct_addresses.Normalify(new_group_count);
					RowOperations::UpdateFilteredStates(filter_set.GetFilterData(aggr_idx), aggr, distinct_addresses,
					                                    distinct_payload, payload_idx);
				} else {
					RowOperations::UpdateStates(aggr, distinct_addresses, distinct_payload, payload_idx,
					                            new_group_count);
				}
			}
		} else if (aggr.filter) {
			RowOperations::UpdateFilteredStates(filter_set.GetFilterData(aggr_idx), aggr, addresses, payload,
			                                    payload_idx);
		} else {
			RowOperations::UpdateStates(aggr, addresses, payload, payload_idx, payload.size());
		}

		// move to the next aggregate
		payload_idx += aggr.child_count;
		VectorOperations::AddInPlace(addresses, aggr.payload_size, payload.size());
	}

	Verify();
	return new_group_count;
}

void GroupedAggregateHashTable::FetchAggregates(DataChunk &groups, DataChunk &result) {
	groups.Verify();
	D_ASSERT(groups.ColumnCount() + 1 == layout.ColumnCount());
	for (idx_t i = 0; i < result.ColumnCount(); i++) {
		D_ASSERT(result.data[i].GetType() == payload_types[i]);
	}
	result.SetCardinality(groups);
	if (groups.size() == 0) {
		return;
	}
	// find the groups associated with the addresses
	// FIXME: this should not use the FindOrCreateGroups, creating them is unnecessary
	Vector addresses(LogicalType::POINTER);
	FindOrCreateGroups(groups, addresses);
	// now fetch the aggregates
	RowOperations::FinalizeStates(layout, addresses, result, 0);
}

template <class ENTRY>
idx_t GroupedAggregateHashTable::FindOrCreateGroupsInternal(DataChunk &groups, Vector &group_hashes, Vector &addresses,
                                                            SelectionVector &new_groups_out) {
	D_ASSERT(!is_finalized);

	if (entries + groups.size() > MaxCapacity()) {
		throw InternalException("Hash table capacity reached");
	}

	// resize at 50% capacity, also need to fit the entire vector
	if (capacity - entries <= groups.size() || entries > capacity / LOAD_FACTOR) {
		Resize<ENTRY>(capacity * 2);
	}

	D_ASSERT(capacity - entries >= groups.size());
	D_ASSERT(groups.ColumnCount() + 1 == layout.ColumnCount());
	// we need to be able to fit at least one vector of data
	D_ASSERT(capacity - entries >= groups.size());
	D_ASSERT(group_hashes.GetType() == LogicalType::HASH);

	group_hashes.Normalify(groups.size());
	auto group_hashes_ptr = FlatVector::GetData<hash_t>(group_hashes);

	D_ASSERT(ht_offsets.GetVectorType() == VectorType::FLAT_VECTOR);
	D_ASSERT(ht_offsets.GetType() == LogicalType::BIGINT);

	D_ASSERT(addresses.GetType() == LogicalType::POINTER);
	addresses.Normalify(groups.size());
	auto addresses_ptr = FlatVector::GetData<data_ptr_t>(addresses);

	// now compute the entry in the table based on the hash using a modulo
	UnaryExecutor::Execute<hash_t, uint64_t>(group_hashes, ht_offsets, groups.size(), [&](hash_t element) {
		D_ASSERT((element & bitmask) == (element % capacity));
		return (element & bitmask);
	});
	auto ht_offsets_ptr = FlatVector::GetData<uint64_t>(ht_offsets);

	// precompute the hash salts for faster comparison below
	D_ASSERT(hash_salts.GetType() == LogicalType::SMALLINT);
	UnaryExecutor::Execute<hash_t, uint16_t>(group_hashes, hash_salts, groups.size(),
	                                         [&](hash_t element) { return (element >> hash_prefix_shift); });
	auto hash_salts_ptr = FlatVector::GetData<uint16_t>(hash_salts);

	// we start out with all entries [0, 1, 2, ..., groups.size()]
	const SelectionVector *sel_vector = FlatVector::IncrementalSelectionVector();

	idx_t remaining_entries = groups.size();

	// make a chunk that references the groups and the hashes
	DataChunk group_chunk;
	group_chunk.InitializeEmpty(layout.GetTypes());
	for (idx_t grp_idx = 0; grp_idx < groups.ColumnCount(); grp_idx++) {
		group_chunk.data[grp_idx].Reference(groups.data[grp_idx]);
	}
	group_chunk.data[groups.ColumnCount()].Reference(group_hashes);
	group_chunk.SetCardinality(groups);

	// orrify all the groups
	auto group_data = group_chunk.Orrify();

	idx_t new_group_count = 0;
	while (remaining_entries > 0) {
		idx_t new_entry_count = 0;
		idx_t need_compare_count = 0;
		idx_t no_match_count = 0;

		// first figure out for each remaining whether or not it belongs to a full or empty group
		for (idx_t i = 0; i < remaining_entries; i++) {
			const idx_t index = sel_vector->get_index(i);
			const auto ht_entry_ptr = ((ENTRY *)this->hashes_hdl_ptr) + ht_offsets_ptr[index];
			if (ht_entry_ptr->page_nr == 0) { // we use page number 0 as a "unused marker"
				// cell is empty; setup the new entry
				if (payload_page_offset == tuples_per_block || payload_hds.empty()) {
					NewBlock();
				}

				auto entry_payload_ptr = payload_hds_ptrs.back() + (payload_page_offset * tuple_size);

				D_ASSERT(group_hashes_ptr[index] >> hash_prefix_shift <= NumericLimits<uint16_t>::Maximum());
				D_ASSERT(payload_page_offset < tuples_per_block);
				D_ASSERT(payload_hds.size() < NumericLimits<uint32_t>::Maximum());
				D_ASSERT(payload_page_offset + 1 < NumericLimits<uint16_t>::Maximum());

				ht_entry_ptr->salt = group_hashes_ptr[index] >> hash_prefix_shift;

				// page numbers start at one so we can use 0 as empty flag
				// GetPtr undoes this
				ht_entry_ptr->page_nr = payload_hds.size();
				ht_entry_ptr->page_offset = payload_page_offset++;

				// update selection lists for outer loops
				empty_vector.set_index(new_entry_count++, index);
				new_groups_out.set_index(new_group_count++, index);
				entries++;

				addresses_ptr[index] = entry_payload_ptr;

			} else {
				// cell is occupied: add to check list
				// only need to check if hash salt in ptr == prefix of hash in payload
				if (ht_entry_ptr->salt == hash_salts_ptr[index]) {
					group_compare_vector.set_index(need_compare_count++, index);

					auto page_ptr = payload_hds_ptrs[ht_entry_ptr->page_nr - 1];
					auto page_offset = ht_entry_ptr->page_offset * tuple_size;
					addresses_ptr[index] = page_ptr + page_offset;

				} else {
					no_match_vector.set_index(no_match_count++, index);
				}
			}
		}

		// for each of the locations that are empty, serialize the group columns to the locations
		RowOperations::Scatter(group_chunk, group_data.get(), layout, addresses, *string_heap, empty_vector,
		                       new_entry_count);
		RowOperations::InitializeStates(layout, addresses, empty_vector, new_entry_count);

		// now we have only the tuples remaining that might match to an existing group
		// start performing comparisons with each of the groups
		RowOperations::Match(group_chunk, group_data.get(), layout, addresses, predicates, group_compare_vector,
		                     need_compare_count, &no_match_vector, no_match_count);

		// each of the entries that do not match we move them to the next entry in the HT
		for (idx_t i = 0; i < no_match_count; i++) {
			idx_t index = no_match_vector.get_index(i);
			ht_offsets_ptr[index]++;
			if (ht_offsets_ptr[index] >= capacity) {
				ht_offsets_ptr[index] = 0;
			}
		}
		sel_vector = &no_match_vector;
		remaining_entries = no_match_count;
	}

	return new_group_count;
}

// this is to support distinct aggregations where we need to record whether we
// have already seen a value for a group
idx_t GroupedAggregateHashTable::FindOrCreateGroups(DataChunk &groups, Vector &group_hashes, Vector &addresses_out,
                                                    SelectionVector &new_groups_out) {
	switch (entry_type) {
	case HtEntryType::HT_WIDTH_64:
		return FindOrCreateGroupsInternal<aggr_ht_entry_64>(groups, group_hashes, addresses_out, new_groups_out);
	case HtEntryType::HT_WIDTH_32:
		return FindOrCreateGroupsInternal<aggr_ht_entry_32>(groups, group_hashes, addresses_out, new_groups_out);
	default:
		throw InternalException("Unknown HT entry width");
	}
}

void GroupedAggregateHashTable::FindOrCreateGroups(DataChunk &groups, Vector &addresses) {
	// create a dummy new_groups sel vector
	SelectionVector new_groups(STANDARD_VECTOR_SIZE);
	FindOrCreateGroups(groups, addresses, new_groups);
}

idx_t GroupedAggregateHashTable::FindOrCreateGroups(DataChunk &groups, Vector &addresses_out,
                                                    SelectionVector &new_groups_out) {
	Vector hashes(LogicalType::HASH);
	groups.Hash(hashes);
	return FindOrCreateGroups(groups, hashes, addresses_out, new_groups_out);
}

struct FlushMoveState {
	FlushMoveState(Allocator &allocator, RowLayout &layout)
	    : new_groups(STANDARD_VECTOR_SIZE), group_addresses(LogicalType::POINTER),
	      new_groups_sel(STANDARD_VECTOR_SIZE) {
		vector<LogicalType> group_types(layout.GetTypes().begin(), layout.GetTypes().end() - 1);
		groups.Initialize(allocator, group_types);
	}

	DataChunk groups;
	SelectionVector new_groups;
	Vector group_addresses;
	SelectionVector new_groups_sel;
};

void GroupedAggregateHashTable::FlushMove(FlushMoveState &state, Vector &source_addresses, Vector &source_hashes,
                                          idx_t count) {
	D_ASSERT(source_addresses.GetType() == LogicalType::POINTER);
	D_ASSERT(source_hashes.GetType() == LogicalType::HASH);

	state.groups.Reset();
	state.groups.SetCardinality(count);
	for (idx_t i = 0; i < state.groups.ColumnCount(); i++) {
		auto &column = state.groups.data[i];
		const auto col_offset = layout.GetOffsets()[i];
		RowOperations::Gather(source_addresses, *FlatVector::IncrementalSelectionVector(), column,
		                      *FlatVector::IncrementalSelectionVector(), count, col_offset, i);
	}

	FindOrCreateGroups(state.groups, source_hashes, state.group_addresses, state.new_groups_sel);

	RowOperations::CombineStates(layout, source_addresses, state.group_addresses, count);
}

void GroupedAggregateHashTable::Combine(GroupedAggregateHashTable &other) {
	D_ASSERT(!is_finalized);

	D_ASSERT(other.layout.GetAggrWidth() == layout.GetAggrWidth());
	D_ASSERT(other.layout.GetDataWidth() == layout.GetDataWidth());
	D_ASSERT(other.layout.GetRowWidth() == layout.GetRowWidth());
	D_ASSERT(other.tuples_per_block == tuples_per_block);

	if (other.entries == 0) {
		return;
	}

	Vector addresses(LogicalType::POINTER);
	auto addresses_ptr = FlatVector::GetData<data_ptr_t>(addresses);

	Vector hashes(LogicalType::HASH);
	auto hashes_ptr = FlatVector::GetData<hash_t>(hashes);

	idx_t group_idx = 0;

	FlushMoveState state(allocator, layout);
	other.PayloadApply([&](idx_t page_nr, idx_t page_offset, data_ptr_t ptr) {
		auto hash = Load<hash_t>(ptr + hash_offset);

		hashes_ptr[group_idx] = hash;
		addresses_ptr[group_idx] = ptr;
		group_idx++;
		if (group_idx == STANDARD_VECTOR_SIZE) {
			FlushMove(state, addresses, hashes, group_idx);
			group_idx = 0;
		}
	});
	FlushMove(state, addresses, hashes, group_idx);
	string_heap->Merge(*other.string_heap);
	Verify();
}

struct PartitionInfo {
	PartitionInfo() : addresses(LogicalType::POINTER), hashes(LogicalType::HASH), group_count(0) {
		addresses_ptr = FlatVector::GetData<data_ptr_t>(addresses);
		hashes_ptr = FlatVector::GetData<hash_t>(hashes);
	};
	Vector addresses;
	Vector hashes;
	idx_t group_count;
	data_ptr_t *addresses_ptr;
	hash_t *hashes_ptr;
};

void GroupedAggregateHashTable::Partition(vector<GroupedAggregateHashTable *> &partition_hts, hash_t mask,
                                          idx_t shift) {
	D_ASSERT(partition_hts.size() > 1);
	vector<PartitionInfo> partition_info(partition_hts.size());

	FlushMoveState state(allocator, layout);
	PayloadApply([&](idx_t page_nr, idx_t page_offset, data_ptr_t ptr) {
		auto hash = Load<hash_t>(ptr + hash_offset);

		idx_t partition = (hash & mask) >> shift;
		D_ASSERT(partition < partition_hts.size());

		auto &info = partition_info[partition];

		info.hashes_ptr[info.group_count] = hash;
		info.addresses_ptr[info.group_count] = ptr;
		info.group_count++;
		if (info.group_count == STANDARD_VECTOR_SIZE) {
			D_ASSERT(partition_hts[partition]);
			partition_hts[partition]->FlushMove(state, info.addresses, info.hashes, info.group_count);
			info.group_count = 0;
		}
	});

	idx_t info_idx = 0;
	idx_t total_count = 0;
	for (auto &partition_entry : partition_hts) {
		auto &info = partition_info[info_idx++];
		partition_entry->FlushMove(state, info.addresses, info.hashes, info.group_count);

		partition_entry->string_heap->Merge(*string_heap);
		partition_entry->Verify();
		total_count += partition_entry->Size();
	}
	D_ASSERT(total_count == entries);
}

idx_t GroupedAggregateHashTable::Scan(idx_t &scan_position, DataChunk &result) {
	Vector addresses(LogicalType::POINTER);
	auto data_pointers = FlatVector::GetData<data_ptr_t>(addresses);

	auto remaining = entries - scan_position;
	if (remaining == 0) {
		return 0;
	}
	auto this_n = MinValue((idx_t)STANDARD_VECTOR_SIZE, remaining);

	auto chunk_idx = scan_position / tuples_per_block;
	auto chunk_offset = (scan_position % tuples_per_block) * tuple_size;
	D_ASSERT(chunk_offset + tuple_size <= Storage::BLOCK_SIZE);

	auto read_ptr = payload_hds_ptrs[chunk_idx++];
	for (idx_t i = 0; i < this_n; i++) {
		data_pointers[i] = read_ptr + chunk_offset;
		chunk_offset += tuple_size;
		if (chunk_offset >= tuples_per_block * tuple_size) {
			read_ptr = payload_hds_ptrs[chunk_idx++];
			chunk_offset = 0;
		}
	}

	result.SetCardinality(this_n);
	// fetch the group columns (ignoring the final hash column
	const auto group_cols = layout.ColumnCount() - 1;
	for (idx_t i = 0; i < group_cols; i++) {
		auto &column = result.data[i];
		const auto col_offset = layout.GetOffsets()[i];
		RowOperations::Gather(addresses, *FlatVector::IncrementalSelectionVector(), column,
		                      *FlatVector::IncrementalSelectionVector(), result.size(), col_offset, i);
	}

	RowOperations::FinalizeStates(layout, addresses, result, group_cols);

	scan_position += this_n;
	return this_n;
}

void GroupedAggregateHashTable::Finalize() {
	if (is_finalized) {
		return;
	}

	// early release hashes, not needed for partition/scan
	hashes_hdl.Destroy();
	is_finalized = true;
}

} // namespace duckdb



namespace duckdb {

BaseAggregateHashTable::BaseAggregateHashTable(Allocator &allocator, const vector<AggregateObject> &aggregates,
                                               BufferManager &buffer_manager, vector<LogicalType> payload_types_p)
    : allocator(allocator), buffer_manager(buffer_manager), payload_types(move(payload_types_p)) {
	filter_set.Initialize(allocator, aggregates, payload_types);
}

} // namespace duckdb












namespace duckdb {

ColumnBindingResolver::ColumnBindingResolver() {
}

void ColumnBindingResolver::VisitOperator(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN || op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		// special case: comparison join
		auto &comp_join = (LogicalComparisonJoin &)op;
		// first get the bindings of the LHS and resolve the LHS expressions
		VisitOperator(*comp_join.children[0]);
		for (auto &cond : comp_join.conditions) {
			VisitExpression(&cond.left);
		}
		if (op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
			// visit the duplicate eliminated columns on the LHS, if any
			auto &delim_join = (LogicalDelimJoin &)op;
			for (auto &expr : delim_join.duplicate_eliminated_columns) {
				VisitExpression(&expr);
			}
		}
		// then get the bindings of the RHS and resolve the RHS expressions
		VisitOperator(*comp_join.children[1]);
		for (auto &cond : comp_join.conditions) {
			VisitExpression(&cond.right);
		}
		// finally update the bindings with the result bindings of the join
		bindings = op.GetColumnBindings();
		return;
	} else if (op.type == LogicalOperatorType::LOGICAL_ANY_JOIN) {
		// ANY join, this join is different because we evaluate the expression on the bindings of BOTH join sides at
		// once i.e. we set the bindings first to the bindings of the entire join, and then resolve the expressions of
		// this operator
		VisitOperatorChildren(op);
		bindings = op.GetColumnBindings();
		VisitOperatorExpressions(op);
		return;
	} else if (op.type == LogicalOperatorType::LOGICAL_CREATE_INDEX) {
		// CREATE INDEX statement, add the columns of the table with table index 0 to the binding set
		// afterwards bind the expressions of the CREATE INDEX statement
		auto &create_index = (LogicalCreateIndex &)op;
		bindings = LogicalOperator::GenerateColumnBindings(0, create_index.table.columns.size());
		VisitOperatorExpressions(op);
		return;
	} else if (op.type == LogicalOperatorType::LOGICAL_GET) {
		//! We first need to update the current set of bindings and then visit operator expressions
		bindings = op.GetColumnBindings();
		VisitOperatorExpressions(op);
		return;
	}
	// general case
	// first visit the children of this operator
	VisitOperatorChildren(op);
	// now visit the expressions of this operator to resolve any bound column references
	VisitOperatorExpressions(op);
	// finally update the current set of bindings to the current set of column bindings
	bindings = op.GetColumnBindings();
}

unique_ptr<Expression> ColumnBindingResolver::VisitReplace(BoundColumnRefExpression &expr,
                                                           unique_ptr<Expression> *expr_ptr) {
	D_ASSERT(expr.depth == 0);
	// check the current set of column bindings to see which index corresponds to the column reference
	for (idx_t i = 0; i < bindings.size(); i++) {
		if (expr.binding == bindings[i]) {
			return make_unique<BoundReferenceExpression>(expr.alias, expr.return_type, i);
		}
	}
	// LCOV_EXCL_START
	// could not bind the column reference, this should never happen and indicates a bug in the code
	// generate an error message
	string bound_columns = "[";
	for (idx_t i = 0; i < bindings.size(); i++) {
		if (i != 0) {
			bound_columns += " ";
		}
		bound_columns += to_string(bindings[i].table_index) + "." + to_string(bindings[i].column_index);
	}
	bound_columns += "]";

	throw InternalException("Failed to bind column reference \"%s\" [%d.%d] (bindings: %s)", expr.alias,
	                        expr.binding.table_index, expr.binding.column_index, bound_columns);
	// LCOV_EXCL_STOP
}

} // namespace duckdb






namespace duckdb {

struct BothInclusiveBetweenOperator {
	template <class T>
	static inline bool Operation(T input, T lower, T upper) {
		return GreaterThanEquals::Operation<T>(input, lower) && LessThanEquals::Operation<T>(input, upper);
	}
};

struct LowerInclusiveBetweenOperator {
	template <class T>
	static inline bool Operation(T input, T lower, T upper) {
		return GreaterThanEquals::Operation<T>(input, lower) && LessThan::Operation<T>(input, upper);
	}
};

struct UpperInclusiveBetweenOperator {
	template <class T>
	static inline bool Operation(T input, T lower, T upper) {
		return GreaterThan::Operation<T>(input, lower) && LessThanEquals::Operation<T>(input, upper);
	}
};

struct ExclusiveBetweenOperator {
	template <class T>
	static inline bool Operation(T input, T lower, T upper) {
		return GreaterThan::Operation<T>(input, lower) && LessThan::Operation<T>(input, upper);
	}
};

template <class OP>
static idx_t BetweenLoopTypeSwitch(Vector &input, Vector &lower, Vector &upper, const SelectionVector *sel, idx_t count,
                                   SelectionVector *true_sel, SelectionVector *false_sel) {
	switch (input.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return TernaryExecutor::Select<int8_t, int8_t, int8_t, OP>(input, lower, upper, sel, count, true_sel,
		                                                           false_sel);
	case PhysicalType::INT16:
		return TernaryExecutor::Select<int16_t, int16_t, int16_t, OP>(input, lower, upper, sel, count, true_sel,
		                                                              false_sel);
	case PhysicalType::INT32:
		return TernaryExecutor::Select<int32_t, int32_t, int32_t, OP>(input, lower, upper, sel, count, true_sel,
		                                                              false_sel);
	case PhysicalType::INT64:
		return TernaryExecutor::Select<int64_t, int64_t, int64_t, OP>(input, lower, upper, sel, count, true_sel,
		                                                              false_sel);
	case PhysicalType::INT128:
		return TernaryExecutor::Select<hugeint_t, hugeint_t, hugeint_t, OP>(input, lower, upper, sel, count, true_sel,
		                                                                    false_sel);
	case PhysicalType::UINT8:
		return TernaryExecutor::Select<uint8_t, uint8_t, uint8_t, OP>(input, lower, upper, sel, count, true_sel,
		                                                              false_sel);
	case PhysicalType::UINT16:
		return TernaryExecutor::Select<uint16_t, uint16_t, uint16_t, OP>(input, lower, upper, sel, count, true_sel,
		                                                                 false_sel);
	case PhysicalType::UINT32:
		return TernaryExecutor::Select<uint32_t, uint32_t, uint32_t, OP>(input, lower, upper, sel, count, true_sel,
		                                                                 false_sel);
	case PhysicalType::UINT64:
		return TernaryExecutor::Select<uint64_t, uint64_t, uint64_t, OP>(input, lower, upper, sel, count, true_sel,
		                                                                 false_sel);
	case PhysicalType::FLOAT:
		return TernaryExecutor::Select<float, float, float, OP>(input, lower, upper, sel, count, true_sel, false_sel);
	case PhysicalType::DOUBLE:
		return TernaryExecutor::Select<double, double, double, OP>(input, lower, upper, sel, count, true_sel,
		                                                           false_sel);
	case PhysicalType::VARCHAR:
		return TernaryExecutor::Select<string_t, string_t, string_t, OP>(input, lower, upper, sel, count, true_sel,
		                                                                 false_sel);
	default:
		throw InvalidTypeException(input.GetType(), "Invalid type for BETWEEN");
	}
}

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(const BoundBetweenExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_unique<ExpressionState>(expr, root);
	result->AddChild(expr.input.get());
	result->AddChild(expr.lower.get());
	result->AddChild(expr.upper.get());
	result->Finalize();
	return result;
}

void ExpressionExecutor::Execute(const BoundBetweenExpression &expr, ExpressionState *state, const SelectionVector *sel,
                                 idx_t count, Vector &result) {
	// resolve the children
	state->intermediate_chunk.Reset();

	auto &input = state->intermediate_chunk.data[0];
	auto &lower = state->intermediate_chunk.data[1];
	auto &upper = state->intermediate_chunk.data[2];

	Execute(*expr.input, state->child_states[0].get(), sel, count, input);
	Execute(*expr.lower, state->child_states[1].get(), sel, count, lower);
	Execute(*expr.upper, state->child_states[2].get(), sel, count, upper);

	Vector intermediate1(LogicalType::BOOLEAN);
	Vector intermediate2(LogicalType::BOOLEAN);

	if (expr.upper_inclusive && expr.lower_inclusive) {
		VectorOperations::GreaterThanEquals(input, lower, intermediate1, count);
		VectorOperations::LessThanEquals(input, upper, intermediate2, count);
	} else if (expr.lower_inclusive) {
		VectorOperations::GreaterThanEquals(input, lower, intermediate1, count);
		VectorOperations::LessThan(input, upper, intermediate2, count);
	} else if (expr.upper_inclusive) {
		VectorOperations::GreaterThan(input, lower, intermediate1, count);
		VectorOperations::LessThanEquals(input, upper, intermediate2, count);
	} else {
		VectorOperations::GreaterThan(input, lower, intermediate1, count);
		VectorOperations::LessThan(input, upper, intermediate2, count);
	}
	VectorOperations::And(intermediate1, intermediate2, result, count);
}

idx_t ExpressionExecutor::Select(const BoundBetweenExpression &expr, ExpressionState *state, const SelectionVector *sel,
                                 idx_t count, SelectionVector *true_sel, SelectionVector *false_sel) {
	// resolve the children
	Vector input(state->intermediate_chunk.data[0]);
	Vector lower(state->intermediate_chunk.data[1]);
	Vector upper(state->intermediate_chunk.data[2]);

	Execute(*expr.input, state->child_states[0].get(), sel, count, input);
	Execute(*expr.lower, state->child_states[1].get(), sel, count, lower);
	Execute(*expr.upper, state->child_states[2].get(), sel, count, upper);

	if (expr.upper_inclusive && expr.lower_inclusive) {
		return BetweenLoopTypeSwitch<BothInclusiveBetweenOperator>(input, lower, upper, sel, count, true_sel,
		                                                           false_sel);
	} else if (expr.lower_inclusive) {
		return BetweenLoopTypeSwitch<LowerInclusiveBetweenOperator>(input, lower, upper, sel, count, true_sel,
		                                                            false_sel);
	} else if (expr.upper_inclusive) {
		return BetweenLoopTypeSwitch<UpperInclusiveBetweenOperator>(input, lower, upper, sel, count, true_sel,
		                                                            false_sel);
	} else {
		return BetweenLoopTypeSwitch<ExclusiveBetweenOperator>(input, lower, upper, sel, count, true_sel, false_sel);
	}
}

} // namespace duckdb




namespace duckdb {

struct CaseExpressionState : public ExpressionState {
	CaseExpressionState(const Expression &expr, ExpressionExecutorState &root)
	    : ExpressionState(expr, root), true_sel(STANDARD_VECTOR_SIZE), false_sel(STANDARD_VECTOR_SIZE) {
	}

	SelectionVector true_sel;
	SelectionVector false_sel;
};

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(const BoundCaseExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_unique<CaseExpressionState>(expr, root);
	for (auto &case_check : expr.case_checks) {
		result->AddChild(case_check.when_expr.get());
		result->AddChild(case_check.then_expr.get());
	}
	result->AddChild(expr.else_expr.get());
	result->Finalize();
	return move(result);
}

void ExpressionExecutor::Execute(const BoundCaseExpression &expr, ExpressionState *state_p, const SelectionVector *sel,
                                 idx_t count, Vector &result) {
	auto state = (CaseExpressionState *)state_p;

	state->intermediate_chunk.Reset();

	// first execute the check expression
	auto current_true_sel = &state->true_sel;
	auto current_false_sel = &state->false_sel;
	auto current_sel = sel;
	idx_t current_count = count;
	for (idx_t i = 0; i < expr.case_checks.size(); i++) {
		auto &case_check = expr.case_checks[i];
		auto &intermediate_result = state->intermediate_chunk.data[i * 2 + 1];
		auto check_state = state->child_states[i * 2].get();
		auto then_state = state->child_states[i * 2 + 1].get();

		idx_t tcount =
		    Select(*case_check.when_expr, check_state, current_sel, current_count, current_true_sel, current_false_sel);
		if (tcount == 0) {
			// everything is false: do nothing
			continue;
		}
		idx_t fcount = current_count - tcount;
		if (fcount == 0 && current_count == count) {
			// everything is true in the first CHECK statement
			// we can skip the entire case and only execute the TRUE side
			Execute(*case_check.then_expr, then_state, sel, count, result);
			return;
		} else {
			// we need to execute and then fill in the desired tuples in the result
			Execute(*case_check.then_expr, then_state, current_true_sel, tcount, intermediate_result);
			FillSwitch(intermediate_result, result, *current_true_sel, tcount);
		}
		// continue with the false tuples
		current_sel = current_false_sel;
		current_count = fcount;
		if (fcount == 0) {
			// everything is true: we are done
			break;
		}
	}
	if (current_count > 0) {
		auto else_state = state->child_states.back().get();
		if (current_count == count) {
			// everything was false, we can just evaluate the else expression directly
			Execute(*expr.else_expr, else_state, sel, count, result);
			return;
		} else {
			auto &intermediate_result = state->intermediate_chunk.data[expr.case_checks.size() * 2];

			D_ASSERT(current_sel);
			Execute(*expr.else_expr, else_state, current_sel, current_count, intermediate_result);
			FillSwitch(intermediate_result, result, *current_sel, current_count);
		}
	}
	if (sel) {
		result.Slice(*sel, count);
	}
}

template <class T>
void TemplatedFillLoop(Vector &vector, Vector &result, const SelectionVector &sel, sel_t count) {
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto res = FlatVector::GetData<T>(result);
	auto &result_mask = FlatVector::Validity(result);
	if (vector.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		auto data = ConstantVector::GetData<T>(vector);
		if (ConstantVector::IsNull(vector)) {
			for (idx_t i = 0; i < count; i++) {
				result_mask.SetInvalid(sel.get_index(i));
			}
		} else {
			for (idx_t i = 0; i < count; i++) {
				res[sel.get_index(i)] = *data;
			}
		}
	} else {
		VectorData vdata;
		vector.Orrify(count, vdata);
		auto data = (T *)vdata.data;
		for (idx_t i = 0; i < count; i++) {
			auto source_idx = vdata.sel->get_index(i);
			auto res_idx = sel.get_index(i);

			res[res_idx] = data[source_idx];
			result_mask.Set(res_idx, vdata.validity.RowIsValid(source_idx));
		}
	}
}

void ValidityFillLoop(Vector &vector, Vector &result, const SelectionVector &sel, sel_t count) {
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto &result_mask = FlatVector::Validity(result);
	if (vector.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		if (ConstantVector::IsNull(vector)) {
			for (idx_t i = 0; i < count; i++) {
				result_mask.SetInvalid(sel.get_index(i));
			}
		}
	} else {
		VectorData vdata;
		vector.Orrify(count, vdata);
		if (vdata.validity.AllValid()) {
			return;
		}
		for (idx_t i = 0; i < count; i++) {
			auto source_idx = vdata.sel->get_index(i);
			if (!vdata.validity.RowIsValid(source_idx)) {
				result_mask.SetInvalid(sel.get_index(i));
			}
		}
	}
}

void ExpressionExecutor::FillSwitch(Vector &vector, Vector &result, const SelectionVector &sel, sel_t count) {
	switch (result.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedFillLoop<int8_t>(vector, result, sel, count);
		break;
	case PhysicalType::INT16:
		TemplatedFillLoop<int16_t>(vector, result, sel, count);
		break;
	case PhysicalType::INT32:
		TemplatedFillLoop<int32_t>(vector, result, sel, count);
		break;
	case PhysicalType::INT64:
		TemplatedFillLoop<int64_t>(vector, result, sel, count);
		break;
	case PhysicalType::UINT8:
		TemplatedFillLoop<uint8_t>(vector, result, sel, count);
		break;
	case PhysicalType::UINT16:
		TemplatedFillLoop<uint16_t>(vector, result, sel, count);
		break;
	case PhysicalType::UINT32:
		TemplatedFillLoop<uint32_t>(vector, result, sel, count);
		break;
	case PhysicalType::UINT64:
		TemplatedFillLoop<uint64_t>(vector, result, sel, count);
		break;
	case PhysicalType::INT128:
		TemplatedFillLoop<hugeint_t>(vector, result, sel, count);
		break;
	case PhysicalType::FLOAT:
		TemplatedFillLoop<float>(vector, result, sel, count);
		break;
	case PhysicalType::DOUBLE:
		TemplatedFillLoop<double>(vector, result, sel, count);
		break;
	case PhysicalType::INTERVAL:
		TemplatedFillLoop<interval_t>(vector, result, sel, count);
		break;
	case PhysicalType::VARCHAR:
		TemplatedFillLoop<string_t>(vector, result, sel, count);
		StringVector::AddHeapReference(result, vector);
		break;
	case PhysicalType::STRUCT: {
		auto &vector_entries = StructVector::GetEntries(vector);
		auto &result_entries = StructVector::GetEntries(result);
		ValidityFillLoop(vector, result, sel, count);
		D_ASSERT(vector_entries.size() == result_entries.size());
		for (idx_t i = 0; i < vector_entries.size(); i++) {
			FillSwitch(*vector_entries[i], *result_entries[i], sel, count);
		}
		break;
	}
	case PhysicalType::LIST: {
		idx_t offset = ListVector::GetListSize(result);
		auto &list_child = ListVector::GetEntry(vector);
		ListVector::Append(result, list_child, ListVector::GetListSize(vector));

		// all the false offsets need to be incremented by true_child.count
		TemplatedFillLoop<list_entry_t>(vector, result, sel, count);
		if (offset == 0) {
			break;
		}

		auto result_data = FlatVector::GetData<list_entry_t>(result);
		for (idx_t i = 0; i < count; i++) {
			auto result_idx = sel.get_index(i);
			result_data[result_idx].offset += offset;
		}

		Vector::Verify(result, sel, count);
		break;
	}
	default:
		throw NotImplementedException("Unimplemented type for case expression: %s", result.GetType().ToString());
	}
}

} // namespace duckdb




namespace duckdb {

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(const BoundCastExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_unique<ExpressionState>(expr, root);
	result->AddChild(expr.child.get());
	result->Finalize();
	return result;
}

void ExpressionExecutor::Execute(const BoundCastExpression &expr, ExpressionState *state, const SelectionVector *sel,
                                 idx_t count, Vector &result) {
	// resolve the child
	state->intermediate_chunk.Reset();

	auto &child = state->intermediate_chunk.data[0];
	auto child_state = state->child_states[0].get();

	Execute(*expr.child, child_state, sel, count, child);
	if (expr.try_cast) {
		string error_message;
		VectorOperations::TryCast(child, result, count, &error_message);
	} else {
		// cast it to the type specified by the cast expression
		D_ASSERT(result.GetType() == expr.return_type);
		VectorOperations::Cast(child, result, count);
	}
}

} // namespace duckdb






#include <algorithm>

namespace duckdb {

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(const BoundComparisonExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_unique<ExpressionState>(expr, root);
	result->AddChild(expr.left.get());
	result->AddChild(expr.right.get());
	result->Finalize();
	return result;
}

void ExpressionExecutor::Execute(const BoundComparisonExpression &expr, ExpressionState *state,
                                 const SelectionVector *sel, idx_t count, Vector &result) {
	// resolve the children
	state->intermediate_chunk.Reset();
	auto &left = state->intermediate_chunk.data[0];
	auto &right = state->intermediate_chunk.data[1];

	Execute(*expr.left, state->child_states[0].get(), sel, count, left);
	Execute(*expr.right, state->child_states[1].get(), sel, count, right);

	switch (expr.type) {
	case ExpressionType::COMPARE_EQUAL:
		VectorOperations::Equals(left, right, result, count);
		break;
	case ExpressionType::COMPARE_NOTEQUAL:
		VectorOperations::NotEquals(left, right, result, count);
		break;
	case ExpressionType::COMPARE_LESSTHAN:
		VectorOperations::LessThan(left, right, result, count);
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		VectorOperations::GreaterThan(left, right, result, count);
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		VectorOperations::LessThanEquals(left, right, result, count);
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		VectorOperations::GreaterThanEquals(left, right, result, count);
		break;
	case ExpressionType::COMPARE_DISTINCT_FROM:
		VectorOperations::DistinctFrom(left, right, result, count);
		break;
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		VectorOperations::NotDistinctFrom(left, right, result, count);
		break;
	default:
		throw InternalException("Unknown comparison type!");
	}
}

template <typename OP>
static idx_t NestedSelectOperation(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                   SelectionVector *true_sel, SelectionVector *false_sel);

template <class OP>
static idx_t TemplatedSelectOperation(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                      SelectionVector *true_sel, SelectionVector *false_sel) {
	// the inplace loops take the result as the last parameter
	switch (left.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return BinaryExecutor::Select<int8_t, int8_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::INT16:
		return BinaryExecutor::Select<int16_t, int16_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::INT32:
		return BinaryExecutor::Select<int32_t, int32_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::INT64:
		return BinaryExecutor::Select<int64_t, int64_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::UINT8:
		return BinaryExecutor::Select<uint8_t, uint8_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::UINT16:
		return BinaryExecutor::Select<uint16_t, uint16_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::UINT32:
		return BinaryExecutor::Select<uint32_t, uint32_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::UINT64:
		return BinaryExecutor::Select<uint64_t, uint64_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::INT128:
		return BinaryExecutor::Select<hugeint_t, hugeint_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::FLOAT:
		return BinaryExecutor::Select<float, float, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::DOUBLE:
		return BinaryExecutor::Select<double, double, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::INTERVAL:
		return BinaryExecutor::Select<interval_t, interval_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::VARCHAR:
		return BinaryExecutor::Select<string_t, string_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::LIST:
	case PhysicalType::MAP:
	case PhysicalType::STRUCT:
		return NestedSelectOperation<OP>(left, right, sel, count, true_sel, false_sel);
	default:
		throw InternalException("Invalid type for comparison");
	}
}

struct NestedSelector {
	// Select the matching rows for the values of a nested type that are not both NULL.
	// Those semantics are the same as the corresponding non-distinct comparator
	template <typename OP>
	static idx_t Select(Vector &left, Vector &right, const SelectionVector &sel, idx_t count, SelectionVector *true_sel,
	                    SelectionVector *false_sel) {
		throw InvalidTypeException(left.GetType(), "Invalid operation for nested SELECT");
	}
};

template <>
idx_t NestedSelector::Select<duckdb::Equals>(Vector &left, Vector &right, const SelectionVector &sel, idx_t count,
                                             SelectionVector *true_sel, SelectionVector *false_sel) {
	return VectorOperations::NestedEquals(left, right, sel, count, true_sel, false_sel);
}

template <>
idx_t NestedSelector::Select<duckdb::NotEquals>(Vector &left, Vector &right, const SelectionVector &sel, idx_t count,
                                                SelectionVector *true_sel, SelectionVector *false_sel) {
	return VectorOperations::NestedNotEquals(left, right, sel, count, true_sel, false_sel);
}

template <>
idx_t NestedSelector::Select<duckdb::LessThan>(Vector &left, Vector &right, const SelectionVector &sel, idx_t count,
                                               SelectionVector *true_sel, SelectionVector *false_sel) {
	return VectorOperations::DistinctLessThan(left, right, &sel, count, true_sel, false_sel);
}

template <>
idx_t NestedSelector::Select<duckdb::LessThanEquals>(Vector &left, Vector &right, const SelectionVector &sel,
                                                     idx_t count, SelectionVector *true_sel,
                                                     SelectionVector *false_sel) {
	return VectorOperations::DistinctLessThanEquals(left, right, &sel, count, true_sel, false_sel);
}

template <>
idx_t NestedSelector::Select<duckdb::GreaterThan>(Vector &left, Vector &right, const SelectionVector &sel, idx_t count,
                                                  SelectionVector *true_sel, SelectionVector *false_sel) {
	return VectorOperations::DistinctGreaterThan(left, right, &sel, count, true_sel, false_sel);
}

template <>
idx_t NestedSelector::Select<duckdb::GreaterThanEquals>(Vector &left, Vector &right, const SelectionVector &sel,
                                                        idx_t count, SelectionVector *true_sel,
                                                        SelectionVector *false_sel) {
	return VectorOperations::DistinctGreaterThanEquals(left, right, &sel, count, true_sel, false_sel);
}

static inline idx_t SelectNotNull(Vector &left, Vector &right, const idx_t count, const SelectionVector &sel,
                                  SelectionVector &maybe_vec, OptionalSelection &false_opt) {

	VectorData lvdata, rvdata;
	left.Orrify(count, lvdata);
	right.Orrify(count, rvdata);

	auto &lmask = lvdata.validity;
	auto &rmask = rvdata.validity;

	// For top-level comparisons, NULL semantics are in effect,
	// so filter out any NULLs
	idx_t remaining = 0;
	if (lmask.AllValid() && rmask.AllValid()) {
		//	None are NULL, distinguish values.
		for (idx_t i = 0; i < count; ++i) {
			const auto idx = sel.get_index(i);
			maybe_vec.set_index(remaining++, idx);
		}
		return remaining;
	}

	// Slice the Vectors down to the rows that are not determined (i.e., neither is NULL)
	SelectionVector slicer(count);
	idx_t false_count = 0;
	for (idx_t i = 0; i < count; ++i) {
		const auto result_idx = sel.get_index(i);
		const auto lidx = lvdata.sel->get_index(i);
		const auto ridx = rvdata.sel->get_index(i);
		if (!lmask.RowIsValid(lidx) || !rmask.RowIsValid(ridx)) {
			false_opt.Append(false_count, result_idx);
		} else {
			//	Neither is NULL, distinguish values.
			slicer.set_index(remaining, i);
			maybe_vec.set_index(remaining++, result_idx);
		}
	}
	false_opt.Advance(false_count);

	if (remaining && remaining < count) {
		left.Slice(slicer, remaining);
		right.Slice(slicer, remaining);
	}

	return remaining;
}

static void ScatterSelection(SelectionVector *target, const idx_t count, const SelectionVector &dense_vec) {
	if (target) {
		for (idx_t i = 0; i < count; ++i) {
			target->set_index(i, dense_vec.get_index(i));
		}
	}
}

template <typename OP>
static idx_t NestedSelectOperation(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                   SelectionVector *true_sel, SelectionVector *false_sel) {
	// The Select operations all use a dense pair of input vectors to partition
	// a selection vector in a single pass. But to implement progressive comparisons,
	// we have to make multiple passes, so we need to keep track of the original input positions
	// and then scatter the output selections when we are done.
	if (!sel) {
		sel = FlatVector::IncrementalSelectionVector();
	}

	// Make buffered selections for progressive comparisons
	// TODO: Remove unnecessary allocations
	SelectionVector true_vec(count);
	OptionalSelection true_opt(&true_vec);

	SelectionVector false_vec(count);
	OptionalSelection false_opt(&false_vec);

	SelectionVector maybe_vec(count);

	// Handle NULL nested values
	Vector l_not_null(left);
	Vector r_not_null(right);

	auto match_count = SelectNotNull(l_not_null, r_not_null, count, *sel, maybe_vec, false_opt);
	auto no_match_count = count - match_count;
	count = match_count;

	//	Now that we have handled the NULLs, we can use the recursive nested comparator for the rest.
	match_count = NestedSelector::Select<OP>(l_not_null, r_not_null, maybe_vec, count, true_opt, false_opt);
	no_match_count += (count - match_count);

	// Copy the buffered selections to the output selections
	ScatterSelection(true_sel, match_count, true_vec);
	ScatterSelection(false_sel, no_match_count, false_vec);

	return match_count;
}

idx_t VectorOperations::Equals(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                               SelectionVector *true_sel, SelectionVector *false_sel) {
	return TemplatedSelectOperation<duckdb::Equals>(left, right, sel, count, true_sel, false_sel);
}

idx_t VectorOperations::NotEquals(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                  SelectionVector *true_sel, SelectionVector *false_sel) {
	return TemplatedSelectOperation<duckdb::NotEquals>(left, right, sel, count, true_sel, false_sel);
}

idx_t VectorOperations::GreaterThan(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                    SelectionVector *true_sel, SelectionVector *false_sel) {
	return TemplatedSelectOperation<duckdb::GreaterThan>(left, right, sel, count, true_sel, false_sel);
}

idx_t VectorOperations::GreaterThanEquals(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                          SelectionVector *true_sel, SelectionVector *false_sel) {
	return TemplatedSelectOperation<duckdb::GreaterThanEquals>(left, right, sel, count, true_sel, false_sel);
}

idx_t VectorOperations::LessThan(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                 SelectionVector *true_sel, SelectionVector *false_sel) {
	return TemplatedSelectOperation<duckdb::LessThan>(left, right, sel, count, true_sel, false_sel);
}

idx_t VectorOperations::LessThanEquals(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                       SelectionVector *true_sel, SelectionVector *false_sel) {
	return TemplatedSelectOperation<duckdb::LessThanEquals>(left, right, sel, count, true_sel, false_sel);
}

idx_t ExpressionExecutor::Select(const BoundComparisonExpression &expr, ExpressionState *state,
                                 const SelectionVector *sel, idx_t count, SelectionVector *true_sel,
                                 SelectionVector *false_sel) {
	// resolve the children
	state->intermediate_chunk.Reset();
	auto &left = state->intermediate_chunk.data[0];
	auto &right = state->intermediate_chunk.data[1];

	Execute(*expr.left, state->child_states[0].get(), sel, count, left);
	Execute(*expr.right, state->child_states[1].get(), sel, count, right);

	switch (expr.type) {
	case ExpressionType::COMPARE_EQUAL:
		return VectorOperations::Equals(left, right, sel, count, true_sel, false_sel);
	case ExpressionType::COMPARE_NOTEQUAL:
		return VectorOperations::NotEquals(left, right, sel, count, true_sel, false_sel);
	case ExpressionType::COMPARE_LESSTHAN:
		return VectorOperations::LessThan(left, right, sel, count, true_sel, false_sel);
	case ExpressionType::COMPARE_GREATERTHAN:
		return VectorOperations::GreaterThan(left, right, sel, count, true_sel, false_sel);
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return VectorOperations::LessThanEquals(left, right, sel, count, true_sel, false_sel);
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return VectorOperations::GreaterThanEquals(left, right, sel, count, true_sel, false_sel);
	case ExpressionType::COMPARE_DISTINCT_FROM:
		return VectorOperations::DistinctFrom(left, right, sel, count, true_sel, false_sel);
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		return VectorOperations::NotDistinctFrom(left, right, sel, count, true_sel, false_sel);
	default:
		throw InternalException("Unknown comparison type!");
	}
}

} // namespace duckdb






#include <random>

namespace duckdb {

struct ConjunctionState : public ExpressionState {
	ConjunctionState(const Expression &expr, ExpressionExecutorState &root) : ExpressionState(expr, root) {
		adaptive_filter = make_unique<AdaptiveFilter>(expr);
	}
	unique_ptr<AdaptiveFilter> adaptive_filter;
};

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(const BoundConjunctionExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_unique<ConjunctionState>(expr, root);
	for (auto &child : expr.children) {
		result->AddChild(child.get());
	}
	result->Finalize();
	return move(result);
}

void ExpressionExecutor::Execute(const BoundConjunctionExpression &expr, ExpressionState *state,
                                 const SelectionVector *sel, idx_t count, Vector &result) {
	// execute the children
	state->intermediate_chunk.Reset();
	for (idx_t i = 0; i < expr.children.size(); i++) {
		auto &current_result = state->intermediate_chunk.data[i];
		Execute(*expr.children[i], state->child_states[i].get(), sel, count, current_result);
		if (i == 0) {
			// move the result
			result.Reference(current_result);
		} else {
			Vector intermediate(LogicalType::BOOLEAN);
			// AND/OR together
			switch (expr.type) {
			case ExpressionType::CONJUNCTION_AND:
				VectorOperations::And(current_result, result, intermediate, count);
				break;
			case ExpressionType::CONJUNCTION_OR:
				VectorOperations::Or(current_result, result, intermediate, count);
				break;
			default:
				throw InternalException("Unknown conjunction type!");
			}
			result.Reference(intermediate);
		}
	}
}

idx_t ExpressionExecutor::Select(const BoundConjunctionExpression &expr, ExpressionState *state_p,
                                 const SelectionVector *sel, idx_t count, SelectionVector *true_sel,
                                 SelectionVector *false_sel) {
	auto state = (ConjunctionState *)state_p;

	if (expr.type == ExpressionType::CONJUNCTION_AND) {
		// get runtime statistics
		auto start_time = high_resolution_clock::now();

		const SelectionVector *current_sel = sel;
		idx_t current_count = count;
		idx_t false_count = 0;

		unique_ptr<SelectionVector> temp_true, temp_false;
		if (false_sel) {
			temp_false = make_unique<SelectionVector>(STANDARD_VECTOR_SIZE);
		}
		if (!true_sel) {
			temp_true = make_unique<SelectionVector>(STANDARD_VECTOR_SIZE);
			true_sel = temp_true.get();
		}
		for (idx_t i = 0; i < expr.children.size(); i++) {
			idx_t tcount = Select(*expr.children[state->adaptive_filter->permutation[i]],
			                      state->child_states[state->adaptive_filter->permutation[i]].get(), current_sel,
			                      current_count, true_sel, temp_false.get());
			idx_t fcount = current_count - tcount;
			if (fcount > 0 && false_sel) {
				// move failing tuples into the false_sel
				// tuples passed, move them into the actual result vector
				for (idx_t i = 0; i < fcount; i++) {
					false_sel->set_index(false_count++, temp_false->get_index(i));
				}
			}
			current_count = tcount;
			if (current_count == 0) {
				break;
			}
			if (current_count < count) {
				// tuples were filtered out: move on to using the true_sel to only evaluate passing tuples in subsequent
				// iterations
				current_sel = true_sel;
			}
		}

		// adapt runtime statistics
		auto end_time = high_resolution_clock::now();
		state->adaptive_filter->AdaptRuntimeStatistics(duration_cast<duration<double>>(end_time - start_time).count());
		return current_count;
	} else {
		// get runtime statistics
		auto start_time = high_resolution_clock::now();

		const SelectionVector *current_sel = sel;
		idx_t current_count = count;
		idx_t result_count = 0;

		unique_ptr<SelectionVector> temp_true, temp_false;
		if (true_sel) {
			temp_true = make_unique<SelectionVector>(STANDARD_VECTOR_SIZE);
		}
		if (!false_sel) {
			temp_false = make_unique<SelectionVector>(STANDARD_VECTOR_SIZE);
			false_sel = temp_false.get();
		}
		for (idx_t i = 0; i < expr.children.size(); i++) {
			idx_t tcount = Select(*expr.children[state->adaptive_filter->permutation[i]],
			                      state->child_states[state->adaptive_filter->permutation[i]].get(), current_sel,
			                      current_count, temp_true.get(), false_sel);
			if (tcount > 0) {
				if (true_sel) {
					// tuples passed, move them into the actual result vector
					for (idx_t i = 0; i < tcount; i++) {
						true_sel->set_index(result_count++, temp_true->get_index(i));
					}
				}
				// now move on to check only the non-passing tuples
				current_count -= tcount;
				current_sel = false_sel;
			}
		}

		// adapt runtime statistics
		auto end_time = high_resolution_clock::now();
		state->adaptive_filter->AdaptRuntimeStatistics(duration_cast<duration<double>>(end_time - start_time).count());
		return result_count;
	}
}

} // namespace duckdb




namespace duckdb {

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(const BoundConstantExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_unique<ExpressionState>(expr, root);
	result->Finalize();
	return result;
}

void ExpressionExecutor::Execute(const BoundConstantExpression &expr, ExpressionState *state,
                                 const SelectionVector *sel, idx_t count, Vector &result) {
	D_ASSERT(expr.value.type() == expr.return_type);
	result.Reference(expr.value);
}

} // namespace duckdb



namespace duckdb {

ExecuteFunctionState::ExecuteFunctionState(const Expression &expr, ExpressionExecutorState &root)
    : ExpressionState(expr, root) {
}

ExecuteFunctionState::~ExecuteFunctionState() {
}

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(const BoundFunctionExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_unique<ExecuteFunctionState>(expr, root);
	for (auto &child : expr.children) {
		result->AddChild(child.get());
	}
	result->Finalize();
	if (expr.function.init_local_state) {
		result->local_state = expr.function.init_local_state(expr, expr.bind_info.get());
	}
	return move(result);
}

void ExpressionExecutor::Execute(const BoundFunctionExpression &expr, ExpressionState *state,
                                 const SelectionVector *sel, idx_t count, Vector &result) {
	state->intermediate_chunk.Reset();
	auto &arguments = state->intermediate_chunk;
	if (!state->types.empty()) {
		for (idx_t i = 0; i < expr.children.size(); i++) {
			D_ASSERT(state->types[i] == expr.children[i]->return_type);
			Execute(*expr.children[i], state->child_states[i].get(), sel, count, arguments.data[i]);
#ifdef DEBUG
			if (expr.children[i]->return_type.id() == LogicalTypeId::VARCHAR) {
				arguments.data[i].UTFVerify(count);
			}
#endif
		}
		arguments.Verify();
	}
	arguments.SetCardinality(count);
	state->profiler.BeginSample();
	expr.function.function(arguments, *state, result);
	state->profiler.EndSample(count);
	D_ASSERT(result.GetType() == expr.return_type);
}

} // namespace duckdb




namespace duckdb {

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(const BoundOperatorExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_unique<ExpressionState>(expr, root);
	for (auto &child : expr.children) {
		result->AddChild(child.get());
	}
	result->Finalize();
	return result;
}

void ExpressionExecutor::Execute(const BoundOperatorExpression &expr, ExpressionState *state,
                                 const SelectionVector *sel, idx_t count, Vector &result) {
	// special handling for special snowflake 'IN'
	// IN has n children
	if (expr.type == ExpressionType::COMPARE_IN || expr.type == ExpressionType::COMPARE_NOT_IN) {
		if (expr.children.size() < 2) {
			throw Exception("IN needs at least two children");
		}

		Vector left(expr.children[0]->return_type);
		// eval left side
		Execute(*expr.children[0], state->child_states[0].get(), sel, count, left);

		// init result to false
		Vector intermediate(LogicalType::BOOLEAN);
		Value false_val = Value::BOOLEAN(false);
		intermediate.Reference(false_val);

		// in rhs is a list of constants
		// for every child, OR the result of the comparision with the left
		// to get the overall result.
		for (idx_t child = 1; child < expr.children.size(); child++) {
			Vector vector_to_check(expr.children[child]->return_type);
			Vector comp_res(LogicalType::BOOLEAN);

			Execute(*expr.children[child], state->child_states[child].get(), sel, count, vector_to_check);
			VectorOperations::Equals(left, vector_to_check, comp_res, count);

			if (child == 1) {
				// first child: move to result
				intermediate.Reference(comp_res);
			} else {
				// otherwise OR together
				Vector new_result(LogicalType::BOOLEAN, true, false);
				VectorOperations::Or(intermediate, comp_res, new_result, count);
				intermediate.Reference(new_result);
			}
		}
		if (expr.type == ExpressionType::COMPARE_NOT_IN) {
			// NOT IN: invert result
			VectorOperations::Not(intermediate, result, count);
		} else {
			// directly use the result
			result.Reference(intermediate);
		}
	} else if (expr.type == ExpressionType::OPERATOR_COALESCE) {
		SelectionVector sel_a(count);
		SelectionVector sel_b(count);
		SelectionVector slice_sel(count);
		SelectionVector result_sel(count);
		SelectionVector *next_sel = &sel_a;
		const SelectionVector *current_sel = sel;
		idx_t remaining_count = count;
		idx_t next_count;
		for (idx_t child = 0; child < expr.children.size(); child++) {
			Vector vector_to_check(expr.children[child]->return_type);
			Execute(*expr.children[child], state->child_states[child].get(), current_sel, remaining_count,
			        vector_to_check);

			VectorData vdata;
			vector_to_check.Orrify(remaining_count, vdata);

			idx_t result_count = 0;
			next_count = 0;
			for (idx_t i = 0; i < remaining_count; i++) {
				auto base_idx = current_sel ? current_sel->get_index(i) : i;
				auto idx = vdata.sel->get_index(i);
				if (vdata.validity.RowIsValid(idx)) {
					slice_sel.set_index(result_count, i);
					result_sel.set_index(result_count++, base_idx);
				} else {
					next_sel->set_index(next_count++, base_idx);
				}
			}
			if (result_count > 0) {
				vector_to_check.Slice(slice_sel, result_count);
				FillSwitch(vector_to_check, result, result_sel, result_count);
			}
			current_sel = next_sel;
			next_sel = next_sel == &sel_a ? &sel_b : &sel_a;
			remaining_count = next_count;
			if (next_count == 0) {
				break;
			}
		}
		if (remaining_count > 0) {
			for (idx_t i = 0; i < remaining_count; i++) {
				FlatVector::SetNull(result, current_sel->get_index(i), true);
			}
		}
		if (sel) {
			result.Slice(*sel, count);
		} else if (count == 1) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
		}
	} else if (expr.children.size() == 1) {
		state->intermediate_chunk.Reset();
		auto &child = state->intermediate_chunk.data[0];

		Execute(*expr.children[0], state->child_states[0].get(), sel, count, child);
		switch (expr.type) {
		case ExpressionType::OPERATOR_NOT: {
			VectorOperations::Not(child, result, count);
			break;
		}
		case ExpressionType::OPERATOR_IS_NULL: {
			VectorOperations::IsNull(child, result, count);
			break;
		}
		case ExpressionType::OPERATOR_IS_NOT_NULL: {
			VectorOperations::IsNotNull(child, result, count);
			break;
		}
		default:
			throw NotImplementedException("Unsupported operator type with 1 child!");
		}
	} else {
		throw NotImplementedException("operator");
	}
}

} // namespace duckdb




namespace duckdb {

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(const BoundParameterExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_unique<ExpressionState>(expr, root);
	result->Finalize();
	return result;
}

void ExpressionExecutor::Execute(const BoundParameterExpression &expr, ExpressionState *state,
                                 const SelectionVector *sel, idx_t count, Vector &result) {
	D_ASSERT(expr.value);
	D_ASSERT(expr.value->type() == expr.return_type);
	result.Reference(*expr.value);
}

} // namespace duckdb



namespace duckdb {

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(const BoundReferenceExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_unique<ExpressionState>(expr, root);
	result->Finalize();
	return result;
}

void ExpressionExecutor::Execute(const BoundReferenceExpression &expr, ExpressionState *state,
                                 const SelectionVector *sel, idx_t count, Vector &result) {
	D_ASSERT(expr.index != DConstants::INVALID_INDEX);
	D_ASSERT(expr.index < chunk->ColumnCount());
	if (sel) {
		result.Slice(chunk->data[expr.index], *sel, count);
	} else {
		result.Reference(chunk->data[expr.index]);
	}
}

} // namespace duckdb






namespace duckdb {

ExpressionExecutor::ExpressionExecutor(Allocator &allocator) : allocator(allocator) {
}

ExpressionExecutor::ExpressionExecutor(Allocator &allocator, const Expression *expression)
    : ExpressionExecutor(allocator) {
	D_ASSERT(expression);
	AddExpression(*expression);
}

ExpressionExecutor::ExpressionExecutor(Allocator &allocator, const Expression &expression)
    : ExpressionExecutor(allocator) {
	AddExpression(expression);
}

ExpressionExecutor::ExpressionExecutor(Allocator &allocator, const vector<unique_ptr<Expression>> &exprs)
    : ExpressionExecutor(allocator) {
	D_ASSERT(exprs.size() > 0);
	for (auto &expr : exprs) {
		AddExpression(*expr);
	}
}

void ExpressionExecutor::AddExpression(const Expression &expr) {
	expressions.push_back(&expr);
	auto state = make_unique<ExpressionExecutorState>(expr.ToString());
	Initialize(expr, *state);
	states.push_back(move(state));
}

void ExpressionExecutor::Initialize(const Expression &expression, ExpressionExecutorState &state) {
	state.executor = this;
	state.root_state = InitializeState(expression, state);
}

void ExpressionExecutor::Execute(DataChunk *input, DataChunk &result) {
	SetChunk(input);
	D_ASSERT(expressions.size() == result.ColumnCount());
	D_ASSERT(!expressions.empty());

	for (idx_t i = 0; i < expressions.size(); i++) {
		ExecuteExpression(i, result.data[i]);
	}
	result.SetCardinality(input ? input->size() : 1);
	result.Verify();
}

void ExpressionExecutor::ExecuteExpression(DataChunk &input, Vector &result) {
	SetChunk(&input);
	ExecuteExpression(result);
}

idx_t ExpressionExecutor::SelectExpression(DataChunk &input, SelectionVector &sel) {
	D_ASSERT(expressions.size() == 1);
	SetChunk(&input);
	states[0]->profiler.BeginSample();
	idx_t selected_tuples = Select(*expressions[0], states[0]->root_state.get(), nullptr, input.size(), &sel, nullptr);
	states[0]->profiler.EndSample(chunk ? chunk->size() : 0);
	return selected_tuples;
}

void ExpressionExecutor::ExecuteExpression(Vector &result) {
	D_ASSERT(expressions.size() == 1);
	ExecuteExpression(0, result);
}

void ExpressionExecutor::ExecuteExpression(idx_t expr_idx, Vector &result) {
	D_ASSERT(expr_idx < expressions.size());
	D_ASSERT(result.GetType().id() == expressions[expr_idx]->return_type.id());
	states[expr_idx]->profiler.BeginSample();
	Execute(*expressions[expr_idx], states[expr_idx]->root_state.get(), nullptr, chunk ? chunk->size() : 1, result);
	states[expr_idx]->profiler.EndSample(chunk ? chunk->size() : 0);
}

Value ExpressionExecutor::EvaluateScalar(const Expression &expr) {
	D_ASSERT(expr.IsFoldable());
	D_ASSERT(expr.IsScalar());
	// use an ExpressionExecutor to execute the expression
	ExpressionExecutor executor(Allocator::DefaultAllocator(), expr);

	Vector result(expr.return_type);
	executor.ExecuteExpression(result);

	D_ASSERT(result.GetVectorType() == VectorType::CONSTANT_VECTOR);
	auto result_value = result.GetValue(0);
	D_ASSERT(result_value.type().InternalType() == expr.return_type.InternalType());
	return result_value;
}

bool ExpressionExecutor::TryEvaluateScalar(const Expression &expr, Value &result) {
	try {
		result = EvaluateScalar(expr);
		return true;
	} catch (...) {
		return false;
	}
}

void ExpressionExecutor::Verify(const Expression &expr, Vector &vector, idx_t count) {
	D_ASSERT(expr.return_type.id() == vector.GetType().id());
	vector.Verify(count);
	if (expr.verification_stats) {
		expr.verification_stats->Verify(vector, count);
	}
}

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(const Expression &expr,
                                                                ExpressionExecutorState &state) {
	switch (expr.expression_class) {
	case ExpressionClass::BOUND_REF:
		return InitializeState((const BoundReferenceExpression &)expr, state);
	case ExpressionClass::BOUND_BETWEEN:
		return InitializeState((const BoundBetweenExpression &)expr, state);
	case ExpressionClass::BOUND_CASE:
		return InitializeState((const BoundCaseExpression &)expr, state);
	case ExpressionClass::BOUND_CAST:
		return InitializeState((const BoundCastExpression &)expr, state);
	case ExpressionClass::BOUND_COMPARISON:
		return InitializeState((const BoundComparisonExpression &)expr, state);
	case ExpressionClass::BOUND_CONJUNCTION:
		return InitializeState((const BoundConjunctionExpression &)expr, state);
	case ExpressionClass::BOUND_CONSTANT:
		return InitializeState((const BoundConstantExpression &)expr, state);
	case ExpressionClass::BOUND_FUNCTION:
		return InitializeState((const BoundFunctionExpression &)expr, state);
	case ExpressionClass::BOUND_OPERATOR:
		return InitializeState((const BoundOperatorExpression &)expr, state);
	case ExpressionClass::BOUND_PARAMETER:
		return InitializeState((const BoundParameterExpression &)expr, state);
	default:
		throw InternalException("Attempting to initialize state of expression of unknown type!");
	}
}

void ExpressionExecutor::Execute(const Expression &expr, ExpressionState *state, const SelectionVector *sel,
                                 idx_t count, Vector &result) {
#ifdef DEBUG
	if (result.GetVectorType() == VectorType::FLAT_VECTOR) {
		D_ASSERT(FlatVector::Validity(result).CheckAllValid(count));
	}
#endif

	if (count == 0) {
		return;
	}
	switch (expr.expression_class) {
	case ExpressionClass::BOUND_BETWEEN:
		Execute((const BoundBetweenExpression &)expr, state, sel, count, result);
		break;
	case ExpressionClass::BOUND_REF:
		Execute((const BoundReferenceExpression &)expr, state, sel, count, result);
		break;
	case ExpressionClass::BOUND_CASE:
		Execute((const BoundCaseExpression &)expr, state, sel, count, result);
		break;
	case ExpressionClass::BOUND_CAST:
		Execute((const BoundCastExpression &)expr, state, sel, count, result);
		break;
	case ExpressionClass::BOUND_COMPARISON:
		Execute((const BoundComparisonExpression &)expr, state, sel, count, result);
		break;
	case ExpressionClass::BOUND_CONJUNCTION:
		Execute((const BoundConjunctionExpression &)expr, state, sel, count, result);
		break;
	case ExpressionClass::BOUND_CONSTANT:
		Execute((const BoundConstantExpression &)expr, state, sel, count, result);
		break;
	case ExpressionClass::BOUND_FUNCTION:
		Execute((const BoundFunctionExpression &)expr, state, sel, count, result);
		break;
	case ExpressionClass::BOUND_OPERATOR:
		Execute((const BoundOperatorExpression &)expr, state, sel, count, result);
		break;
	case ExpressionClass::BOUND_PARAMETER:
		Execute((const BoundParameterExpression &)expr, state, sel, count, result);
		break;
	default:
		throw InternalException("Attempting to execute expression of unknown type!");
	}
	Verify(expr, result, count);
}

idx_t ExpressionExecutor::Select(const Expression &expr, ExpressionState *state, const SelectionVector *sel,
                                 idx_t count, SelectionVector *true_sel, SelectionVector *false_sel) {
	if (count == 0) {
		return 0;
	}
	D_ASSERT(true_sel || false_sel);
	D_ASSERT(expr.return_type.id() == LogicalTypeId::BOOLEAN);
	switch (expr.expression_class) {
	case ExpressionClass::BOUND_BETWEEN:
		return Select((BoundBetweenExpression &)expr, state, sel, count, true_sel, false_sel);
	case ExpressionClass::BOUND_COMPARISON:
		return Select((BoundComparisonExpression &)expr, state, sel, count, true_sel, false_sel);
	case ExpressionClass::BOUND_CONJUNCTION:
		return Select((BoundConjunctionExpression &)expr, state, sel, count, true_sel, false_sel);
	default:
		return DefaultSelect(expr, state, sel, count, true_sel, false_sel);
	}
}

template <bool NO_NULL, bool HAS_TRUE_SEL, bool HAS_FALSE_SEL>
static inline idx_t DefaultSelectLoop(const SelectionVector *bsel, uint8_t *__restrict bdata, ValidityMask &mask,
                                      const SelectionVector *sel, idx_t count, SelectionVector *true_sel,
                                      SelectionVector *false_sel) {
	idx_t true_count = 0, false_count = 0;
	for (idx_t i = 0; i < count; i++) {
		auto bidx = bsel->get_index(i);
		auto result_idx = sel->get_index(i);
		if (bdata[bidx] > 0 && (NO_NULL || mask.RowIsValid(bidx))) {
			if (HAS_TRUE_SEL) {
				true_sel->set_index(true_count++, result_idx);
			}
		} else {
			if (HAS_FALSE_SEL) {
				false_sel->set_index(false_count++, result_idx);
			}
		}
	}
	if (HAS_TRUE_SEL) {
		return true_count;
	} else {
		return count - false_count;
	}
}

template <bool NO_NULL>
static inline idx_t DefaultSelectSwitch(VectorData &idata, const SelectionVector *sel, idx_t count,
                                        SelectionVector *true_sel, SelectionVector *false_sel) {
	if (true_sel && false_sel) {
		return DefaultSelectLoop<NO_NULL, true, true>(idata.sel, (uint8_t *)idata.data, idata.validity, sel, count,
		                                              true_sel, false_sel);
	} else if (true_sel) {
		return DefaultSelectLoop<NO_NULL, true, false>(idata.sel, (uint8_t *)idata.data, idata.validity, sel, count,
		                                               true_sel, false_sel);
	} else {
		D_ASSERT(false_sel);
		return DefaultSelectLoop<NO_NULL, false, true>(idata.sel, (uint8_t *)idata.data, idata.validity, sel, count,
		                                               true_sel, false_sel);
	}
}

idx_t ExpressionExecutor::DefaultSelect(const Expression &expr, ExpressionState *state, const SelectionVector *sel,
                                        idx_t count, SelectionVector *true_sel, SelectionVector *false_sel) {
	// generic selection of boolean expression:
	// resolve the true/false expression first
	// then use that to generate the selection vector
	bool intermediate_bools[STANDARD_VECTOR_SIZE];
	Vector intermediate(LogicalType::BOOLEAN, (data_ptr_t)intermediate_bools);
	Execute(expr, state, sel, count, intermediate);

	VectorData idata;
	intermediate.Orrify(count, idata);

	if (!sel) {
		sel = FlatVector::IncrementalSelectionVector();
	}
	if (!idata.validity.AllValid()) {
		return DefaultSelectSwitch<false>(idata, sel, count, true_sel, false_sel);
	} else {
		return DefaultSelectSwitch<true>(idata, sel, count, true_sel, false_sel);
	}
}

vector<unique_ptr<ExpressionExecutorState>> &ExpressionExecutor::GetStates() {
	return states;
}

} // namespace duckdb




namespace duckdb {

void ExpressionState::AddChild(Expression *expr) {
	types.push_back(expr->return_type);
	child_states.push_back(ExpressionExecutor::InitializeState(*expr, root));
}

void ExpressionState::Finalize() {
	if (!types.empty()) {
		intermediate_chunk.Initialize(root.executor->allocator, types);
	}
}
ExpressionState::ExpressionState(const Expression &expr, ExpressionExecutorState &root)
    : expr(expr), root(root), name(expr.ToString()) {
}

ExpressionExecutorState::ExpressionExecutorState(const string &name) : profiler(), name(name) {
}
} // namespace duckdb






#include <algorithm>
#include <cstring>
#include <ctgmath>

namespace duckdb {

ART::ART(const vector<column_t> &column_ids, const vector<unique_ptr<Expression>> &unbound_expressions,
         IndexConstraintType constraint_type)
    : Index(IndexType::ART, column_ids, unbound_expressions, constraint_type) {
	tree = nullptr;
	is_little_endian = Radix::IsLittleEndian();
	for (idx_t i = 0; i < types.size(); i++) {
		switch (types[i]) {
		case PhysicalType::BOOL:
		case PhysicalType::INT8:
		case PhysicalType::INT16:
		case PhysicalType::INT32:
		case PhysicalType::INT64:
		case PhysicalType::INT128:
		case PhysicalType::UINT8:
		case PhysicalType::UINT16:
		case PhysicalType::UINT32:
		case PhysicalType::UINT64:
		case PhysicalType::FLOAT:
		case PhysicalType::DOUBLE:
		case PhysicalType::VARCHAR:
			break;
		default:
			throw InvalidTypeException(logical_types[i], "Invalid type for index");
		}
	}
}

ART::~ART() {
}

bool ART::LeafMatches(Node *node, Key &key, unsigned depth) {
	auto leaf = static_cast<Leaf *>(node);
	Key &leaf_key = *leaf->value;
	for (idx_t i = depth; i < leaf_key.len; i++) {
		if (leaf_key[i] != key[i]) {
			return false;
		}
	}

	return true;
}

unique_ptr<IndexScanState> ART::InitializeScanSinglePredicate(Transaction &transaction, Value value,
                                                              ExpressionType expression_type) {
	auto result = make_unique<ARTIndexScanState>();
	result->values[0] = value;
	result->expressions[0] = expression_type;
	return move(result);
}

unique_ptr<IndexScanState> ART::InitializeScanTwoPredicates(Transaction &transaction, Value low_value,
                                                            ExpressionType low_expression_type, Value high_value,
                                                            ExpressionType high_expression_type) {
	auto result = make_unique<ARTIndexScanState>();
	result->values[0] = low_value;
	result->expressions[0] = low_expression_type;
	result->values[1] = high_value;
	result->expressions[1] = high_expression_type;
	return move(result);
}

//===--------------------------------------------------------------------===//
// Insert
//===--------------------------------------------------------------------===//
template <class T>
static void TemplatedGenerateKeys(Vector &input, idx_t count, vector<unique_ptr<Key>> &keys, bool is_little_endian) {
	VectorData idata;
	input.Orrify(count, idata);

	auto input_data = (T *)idata.data;
	for (idx_t i = 0; i < count; i++) {
		auto idx = idata.sel->get_index(i);
		if (idata.validity.RowIsValid(idx)) {
			keys.push_back(Key::CreateKey<T>(input_data[idx], is_little_endian));
		} else {
			keys.push_back(nullptr);
		}
	}
}

template <class T>
static void ConcatenateKeys(Vector &input, idx_t count, vector<unique_ptr<Key>> &keys, bool is_little_endian) {
	VectorData idata;
	input.Orrify(count, idata);

	auto input_data = (T *)idata.data;
	for (idx_t i = 0; i < count; i++) {
		auto idx = idata.sel->get_index(i);
		if (!idata.validity.RowIsValid(idx) || !keys[i]) {
			// either this column is NULL, or the previous column is NULL!
			keys[i] = nullptr;
		} else {
			// concatenate the keys
			auto old_key = move(keys[i]);
			auto new_key = Key::CreateKey<T>(input_data[idx], is_little_endian);
			auto key_len = old_key->len + new_key->len;
			auto compound_data = unique_ptr<data_t[]>(new data_t[key_len]);
			memcpy(compound_data.get(), old_key->data.get(), old_key->len);
			memcpy(compound_data.get() + old_key->len, new_key->data.get(), new_key->len);
			keys[i] = make_unique<Key>(move(compound_data), key_len);
		}
	}
}

void ART::GenerateKeys(DataChunk &input, vector<unique_ptr<Key>> &keys) {
	keys.reserve(STANDARD_VECTOR_SIZE);
	// generate keys for the first input column
	switch (input.data[0].GetType().InternalType()) {
	case PhysicalType::BOOL:
		TemplatedGenerateKeys<bool>(input.data[0], input.size(), keys, is_little_endian);
		break;
	case PhysicalType::INT8:
		TemplatedGenerateKeys<int8_t>(input.data[0], input.size(), keys, is_little_endian);
		break;
	case PhysicalType::INT16:
		TemplatedGenerateKeys<int16_t>(input.data[0], input.size(), keys, is_little_endian);
		break;
	case PhysicalType::INT32:
		TemplatedGenerateKeys<int32_t>(input.data[0], input.size(), keys, is_little_endian);
		break;
	case PhysicalType::INT64:
		TemplatedGenerateKeys<int64_t>(input.data[0], input.size(), keys, is_little_endian);
		break;
	case PhysicalType::INT128:
		TemplatedGenerateKeys<hugeint_t>(input.data[0], input.size(), keys, is_little_endian);
		break;
	case PhysicalType::UINT8:
		TemplatedGenerateKeys<uint8_t>(input.data[0], input.size(), keys, is_little_endian);
		break;
	case PhysicalType::UINT16:
		TemplatedGenerateKeys<uint16_t>(input.data[0], input.size(), keys, is_little_endian);
		break;
	case PhysicalType::UINT32:
		TemplatedGenerateKeys<uint32_t>(input.data[0], input.size(), keys, is_little_endian);
		break;
	case PhysicalType::UINT64:
		TemplatedGenerateKeys<uint64_t>(input.data[0], input.size(), keys, is_little_endian);
		break;
	case PhysicalType::FLOAT:
		TemplatedGenerateKeys<float>(input.data[0], input.size(), keys, is_little_endian);
		break;
	case PhysicalType::DOUBLE:
		TemplatedGenerateKeys<double>(input.data[0], input.size(), keys, is_little_endian);
		break;
	case PhysicalType::VARCHAR:
		TemplatedGenerateKeys<string_t>(input.data[0], input.size(), keys, is_little_endian);
		break;
	default:
		throw InternalException("Invalid type for index");
	}

	for (idx_t i = 1; i < input.ColumnCount(); i++) {
		// for each of the remaining columns, concatenate
		switch (input.data[i].GetType().InternalType()) {
		case PhysicalType::BOOL:
			ConcatenateKeys<bool>(input.data[i], input.size(), keys, is_little_endian);
			break;
		case PhysicalType::INT8:
			ConcatenateKeys<int8_t>(input.data[i], input.size(), keys, is_little_endian);
			break;
		case PhysicalType::INT16:
			ConcatenateKeys<int16_t>(input.data[i], input.size(), keys, is_little_endian);
			break;
		case PhysicalType::INT32:
			ConcatenateKeys<int32_t>(input.data[i], input.size(), keys, is_little_endian);
			break;
		case PhysicalType::INT64:
			ConcatenateKeys<int64_t>(input.data[i], input.size(), keys, is_little_endian);
			break;
		case PhysicalType::INT128:
			ConcatenateKeys<hugeint_t>(input.data[i], input.size(), keys, is_little_endian);
			break;
		case PhysicalType::UINT8:
			ConcatenateKeys<uint8_t>(input.data[i], input.size(), keys, is_little_endian);
			break;
		case PhysicalType::UINT16:
			ConcatenateKeys<uint16_t>(input.data[i], input.size(), keys, is_little_endian);
			break;
		case PhysicalType::UINT32:
			ConcatenateKeys<uint32_t>(input.data[i], input.size(), keys, is_little_endian);
			break;
		case PhysicalType::UINT64:
			ConcatenateKeys<uint64_t>(input.data[i], input.size(), keys, is_little_endian);
			break;
		case PhysicalType::FLOAT:
			ConcatenateKeys<float>(input.data[i], input.size(), keys, is_little_endian);
			break;
		case PhysicalType::DOUBLE:
			ConcatenateKeys<double>(input.data[i], input.size(), keys, is_little_endian);
			break;
		case PhysicalType::VARCHAR:
			ConcatenateKeys<string_t>(input.data[i], input.size(), keys, is_little_endian);
			break;
		default:
			throw InternalException("Invalid type for index");
		}
	}
}

bool ART::Insert(IndexLock &lock, DataChunk &input, Vector &row_ids) {
	D_ASSERT(row_ids.GetType().InternalType() == ROW_TYPE);
	D_ASSERT(logical_types[0] == input.data[0].GetType());

	// generate the keys for the given input
	vector<unique_ptr<Key>> keys;
	GenerateKeys(input, keys);

	// now insert the elements into the index
	row_ids.Normalify(input.size());
	auto row_identifiers = FlatVector::GetData<row_t>(row_ids);
	idx_t failed_index = DConstants::INVALID_INDEX;
	for (idx_t i = 0; i < input.size(); i++) {
		if (!keys[i]) {
			continue;
		}

		row_t row_id = row_identifiers[i];
		if (!Insert(tree, move(keys[i]), 0, row_id)) {
			// failed to insert because of constraint violation
			failed_index = i;
			break;
		}
	}
	if (failed_index != DConstants::INVALID_INDEX) {
		// failed to insert because of constraint violation: remove previously inserted entries
		// generate keys again
		keys.clear();
		GenerateKeys(input, keys);
		unique_ptr<Key> key;

		// now erase the entries
		for (idx_t i = 0; i < failed_index; i++) {
			if (!keys[i]) {
				continue;
			}
			row_t row_id = row_identifiers[i];
			Erase(tree, *keys[i], 0, row_id);
		}
		return false;
	}
	return true;
}

bool ART::Append(IndexLock &lock, DataChunk &appended_data, Vector &row_identifiers) {
	DataChunk expression_result;
	expression_result.Initialize(Allocator::DefaultAllocator(), logical_types);

	// first resolve the expressions for the index
	ExecuteExpressions(appended_data, expression_result);

	// now insert into the index
	return Insert(lock, expression_result, row_identifiers);
}

void ART::VerifyAppend(DataChunk &chunk) {
	VerifyExistence(chunk, VerifyExistenceType::APPEND);
}

void ART::VerifyAppendForeignKey(DataChunk &chunk, string *err_msg_ptr) {
	VerifyExistence(chunk, VerifyExistenceType::APPEND_FK, err_msg_ptr);
}

void ART::VerifyDeleteForeignKey(DataChunk &chunk, string *err_msg_ptr) {
	VerifyExistence(chunk, VerifyExistenceType::DELETE_FK, err_msg_ptr);
}

bool ART::InsertToLeaf(Leaf &leaf, row_t row_id) {
	if (IsUnique() && leaf.num_elements != 0) {
		return false;
	}
	leaf.Insert(row_id);
	return true;
}

bool ART::Insert(unique_ptr<Node> &node, unique_ptr<Key> value, unsigned depth, row_t row_id) {
	Key &key = *value;
	if (!node) {
		// node is currently empty, create a leaf here with the key
		node = make_unique<Leaf>(*this, move(value), row_id);
		return true;
	}

	if (node->type == NodeType::NLeaf) {
		// Replace leaf with Node4 and store both leaves in it
		auto leaf = static_cast<Leaf *>(node.get());

		Key &existing_key = *leaf->value;
		uint32_t new_prefix_length = 0;
		// Leaf node is already there, update row_id vector
		if (depth + new_prefix_length == existing_key.len && existing_key.len == key.len) {
			return InsertToLeaf(*leaf, row_id);
		}
		while (existing_key[depth + new_prefix_length] == key[depth + new_prefix_length]) {
			new_prefix_length++;
			// Leaf node is already there, update row_id vector
			if (depth + new_prefix_length == existing_key.len && existing_key.len == key.len) {
				return InsertToLeaf(*leaf, row_id);
			}
		}

		unique_ptr<Node> new_node = make_unique<Node4>(*this, new_prefix_length);
		new_node->prefix_length = new_prefix_length;
		memcpy(new_node->prefix.get(), &key[depth], new_prefix_length);
		Node4::Insert(*this, new_node, existing_key[depth + new_prefix_length], node);
		unique_ptr<Node> leaf_node = make_unique<Leaf>(*this, move(value), row_id);
		Node4::Insert(*this, new_node, key[depth + new_prefix_length], leaf_node);
		node = move(new_node);
		return true;
	}

	// Handle prefix of inner node
	if (node->prefix_length) {
		uint32_t mismatch_pos = Node::PrefixMismatch(*this, node.get(), key, depth);
		if (mismatch_pos != node->prefix_length) {
			// Prefix differs, create new node
			unique_ptr<Node> new_node = make_unique<Node4>(*this, mismatch_pos);
			new_node->prefix_length = mismatch_pos;
			memcpy(new_node->prefix.get(), node->prefix.get(), mismatch_pos);
			// Break up prefix
			auto node_ptr = node.get();
			Node4::Insert(*this, new_node, node->prefix[mismatch_pos], node);
			node_ptr->prefix_length -= (mismatch_pos + 1);
			memmove(node_ptr->prefix.get(), node_ptr->prefix.get() + mismatch_pos + 1, node_ptr->prefix_length);
			unique_ptr<Node> leaf_node = make_unique<Leaf>(*this, move(value), row_id);
			Node4::Insert(*this, new_node, key[depth + mismatch_pos], leaf_node);
			node = move(new_node);
			return true;
		}
		depth += node->prefix_length;
	}

	// Recurse
	idx_t pos = node->GetChildPos(key[depth]);
	if (pos != DConstants::INVALID_INDEX) {
		auto child = node->GetChild(pos);
		return Insert(*child, move(value), depth + 1, row_id);
	}
	unique_ptr<Node> new_node = make_unique<Leaf>(*this, move(value), row_id);
	Node::InsertLeaf(*this, node, key[depth], new_node);
	return true;
}

//===--------------------------------------------------------------------===//
// Delete
//===--------------------------------------------------------------------===//
void ART::Delete(IndexLock &state, DataChunk &input, Vector &row_ids) {
	DataChunk expression_result;
	expression_result.Initialize(Allocator::DefaultAllocator(), logical_types);

	// first resolve the expressions
	ExecuteExpressions(input, expression_result);

	// then generate the keys for the given input
	vector<unique_ptr<Key>> keys;
	GenerateKeys(expression_result, keys);

	// now erase the elements from the database
	row_ids.Normalify(input.size());
	auto row_identifiers = FlatVector::GetData<row_t>(row_ids);

	for (idx_t i = 0; i < input.size(); i++) {
		if (!keys[i]) {
			continue;
		}
		Erase(tree, *keys[i], 0, row_identifiers[i]);
#ifdef DEBUG
		auto node = Lookup(tree, *keys[i], 0);
		if (node) {
			auto leaf = static_cast<Leaf *>(node);
			for (idx_t k = 0; k < leaf->num_elements; k++) {
				D_ASSERT(leaf->GetRowId(k) != row_identifiers[i]);
			}
		}
#endif
	}
}

void ART::Erase(unique_ptr<Node> &node, Key &key, unsigned depth, row_t row_id) {
	if (!node) {
		return;
	}
	// Delete a leaf from a tree
	if (node->type == NodeType::NLeaf) {
		// Make sure we have the right leaf
		if (ART::LeafMatches(node.get(), key, depth)) {
			auto leaf = static_cast<Leaf *>(node.get());
			leaf->Remove(row_id);
			if (leaf->num_elements == 0) {
				node.reset();
			}
		}
		return;
	}

	// Handle prefix
	if (node->prefix_length) {
		if (Node::PrefixMismatch(*this, node.get(), key, depth) != node->prefix_length) {
			return;
		}
		depth += node->prefix_length;
	}
	idx_t pos = node->GetChildPos(key[depth]);
	if (pos != DConstants::INVALID_INDEX) {
		auto child = node->GetChild(pos);
		D_ASSERT(child);

		unique_ptr<Node> &child_ref = *child;
		if (child_ref->type == NodeType::NLeaf && LeafMatches(child_ref.get(), key, depth)) {
			// Leaf found, remove entry
			auto leaf = static_cast<Leaf *>(child_ref.get());
			leaf->Remove(row_id);
			if (leaf->num_elements == 0) {
				// Leaf is empty, delete leaf, decrement node counter and maybe shrink node
				Node::Erase(*this, node, pos);
			}
		} else {
			// Recurse
			Erase(*child, key, depth + 1, row_id);
		}
	}
}

//===--------------------------------------------------------------------===//
// Point Query
//===--------------------------------------------------------------------===//
static unique_ptr<Key> CreateKey(ART &art, PhysicalType type, Value &value) {
	D_ASSERT(type == value.type().InternalType());
	switch (type) {
	case PhysicalType::BOOL:
		return Key::CreateKey<bool>(value, art.is_little_endian);
	case PhysicalType::INT8:
		return Key::CreateKey<int8_t>(value, art.is_little_endian);
	case PhysicalType::INT16:
		return Key::CreateKey<int16_t>(value, art.is_little_endian);
	case PhysicalType::INT32:
		return Key::CreateKey<int32_t>(value, art.is_little_endian);
	case PhysicalType::INT64:
		return Key::CreateKey<int64_t>(value, art.is_little_endian);
	case PhysicalType::UINT8:
		return Key::CreateKey<uint8_t>(value, art.is_little_endian);
	case PhysicalType::UINT16:
		return Key::CreateKey<uint16_t>(value, art.is_little_endian);
	case PhysicalType::UINT32:
		return Key::CreateKey<uint32_t>(value, art.is_little_endian);
	case PhysicalType::UINT64:
		return Key::CreateKey<uint64_t>(value, art.is_little_endian);
	case PhysicalType::INT128:
		return Key::CreateKey<hugeint_t>(value, art.is_little_endian);
	case PhysicalType::FLOAT:
		return Key::CreateKey<float>(value, art.is_little_endian);
	case PhysicalType::DOUBLE:
		return Key::CreateKey<double>(value, art.is_little_endian);
	case PhysicalType::VARCHAR:
		return Key::CreateKey<string_t>(value, art.is_little_endian);
	default:
		throw InternalException("Invalid type for index");
	}
}

bool ART::SearchEqual(ARTIndexScanState *state, idx_t max_count, vector<row_t> &result_ids) {
	auto key = CreateKey(*this, types[0], state->values[0]);
	auto leaf = static_cast<Leaf *>(Lookup(tree, *key, 0));
	if (!leaf) {
		return true;
	}
	if (leaf->num_elements > max_count) {
		return false;
	}
	for (idx_t i = 0; i < leaf->num_elements; i++) {
		row_t row_id = leaf->GetRowId(i);
		result_ids.push_back(row_id);
	}
	return true;
}

void ART::SearchEqualJoinNoFetch(Value &equal_value, idx_t &result_size) {
	//! We need to look for a leaf
	auto key = CreateKey(*this, types[0], equal_value);
	auto leaf = static_cast<Leaf *>(Lookup(tree, *key, 0));
	if (!leaf) {
		return;
	}
	result_size = leaf->num_elements;
}

Node *ART::Lookup(unique_ptr<Node> &node, Key &key, unsigned depth) {
	auto node_val = node.get();

	while (node_val) {
		if (node_val->type == NodeType::NLeaf) {
			auto leaf = static_cast<Leaf *>(node_val);
			Key &leaf_key = *leaf->value;
			//! Check leaf
			for (idx_t i = depth; i < leaf_key.len; i++) {
				if (leaf_key[i] != key[i]) {
					return nullptr;
				}
			}
			return node_val;
		}
		if (node_val->prefix_length) {
			for (idx_t pos = 0; pos < node_val->prefix_length; pos++) {
				if (key[depth + pos] != node_val->prefix[pos]) {
					return nullptr;
				}
			}
			depth += node_val->prefix_length;
		}
		idx_t pos = node_val->GetChildPos(key[depth]);
		if (pos == DConstants::INVALID_INDEX) {
			return nullptr;
		}
		node_val = node_val->GetChild(pos)->get();
		D_ASSERT(node_val);

		depth++;
	}

	return nullptr;
}

//===--------------------------------------------------------------------===//
// Iterator scans
//===--------------------------------------------------------------------===//
template <bool HAS_BOUND, bool INCLUSIVE>
bool ART::IteratorScan(ARTIndexScanState *state, Iterator *it, Key *bound, idx_t max_count, vector<row_t> &result_ids) {
	bool has_next;
	do {
		if (HAS_BOUND) {
			D_ASSERT(bound);
			if (INCLUSIVE) {
				if (*it->node->value > *bound) {
					break;
				}
			} else {
				if (*it->node->value >= *bound) {
					break;
				}
			}
		}
		if (result_ids.size() + it->node->num_elements > max_count) {
			// adding these elements would exceed the max count
			return false;
		}
		for (idx_t i = 0; i < it->node->num_elements; i++) {
			row_t row_id = it->node->GetRowId(i);
			result_ids.push_back(row_id);
		}
		has_next = ART::IteratorNext(*it);
	} while (has_next);
	return true;
}

void Iterator::SetEntry(idx_t entry_depth, IteratorEntry entry) {
	if (stack.size() < entry_depth + 1) {
		stack.resize(MaxValue<idx_t>(8, MaxValue<idx_t>(entry_depth + 1, stack.size() * 2)));
	}
	stack[entry_depth] = entry;
}

bool ART::IteratorNext(Iterator &it) {
	// Skip leaf
	if ((it.depth) && ((it.stack[it.depth - 1].node)->type == NodeType::NLeaf)) {
		it.depth--;
	}

	// Look for the next leaf
	while (it.depth > 0) {
		auto &top = it.stack[it.depth - 1];
		Node *node = top.node;

		if (node->type == NodeType::NLeaf) {
			// found a leaf: move to next node
			it.node = (Leaf *)node;
			return true;
		}

		// Find next node
		top.pos = node->GetNextPos(top.pos);
		if (top.pos != DConstants::INVALID_INDEX) {
			// next node found: go there
			it.SetEntry(it.depth, IteratorEntry(node->GetChild(top.pos)->get(), DConstants::INVALID_INDEX));
			it.depth++;
		} else {
			// no node found: move up the tree
			it.depth--;
		}
	}
	return false;
}

//===--------------------------------------------------------------------===//
// Greater Than
// Returns: True (If found leaf >= key)
//          False (Otherwise)
//===--------------------------------------------------------------------===//
bool ART::Bound(unique_ptr<Node> &n, Key &key, Iterator &it, bool inclusive) {
	it.depth = 0;
	bool equal = false;
	if (!n) {
		return false;
	}
	Node *node = n.get();

	idx_t depth = 0;
	while (true) {
		it.SetEntry(it.depth, IteratorEntry(node, 0));
		auto &top = it.stack[it.depth];
		it.depth++;
		if (!equal) {
			while (node->type != NodeType::NLeaf) {
				node = node->GetChild(node->GetMin())->get();
				auto &c_top = it.stack[it.depth];
				c_top.node = node;
				it.depth++;
			}
		}
		if (node->type == NodeType::NLeaf) {
			// found a leaf node: check if it is bigger or equal than the current key
			auto leaf = static_cast<Leaf *>(node);
			it.node = leaf;
			// if the search is not inclusive the leaf node could still be equal to the current value
			// check if leaf is equal to the current key
			if (*leaf->value == key) {
				// if its not inclusive check if there is a next leaf
				if (!inclusive && !IteratorNext(it)) {
					return false;
				} else {
					return true;
				}
			}

			if (*leaf->value > key) {
				return true;
			}
			// Leaf is lower than key
			// Check if next leaf is still lower than key
			while (IteratorNext(it)) {
				if (*it.node->value == key) {
					// if its not inclusive check if there is a next leaf
					if (!inclusive && !IteratorNext(it)) {
						return false;
					} else {
						return true;
					}
				} else if (*it.node->value > key) {
					// if its not inclusive check if there is a next leaf
					return true;
				}
			}
			return false;
		}
		uint32_t mismatch_pos = Node::PrefixMismatch(*this, node, key, depth);
		if (mismatch_pos != node->prefix_length) {
			if (node->prefix[mismatch_pos] < key[depth + mismatch_pos]) {
				// Less
				it.depth--;
				return IteratorNext(it);
			} else {
				// Greater
				top.pos = DConstants::INVALID_INDEX;
				return IteratorNext(it);
			}
		}
		// prefix matches, search inside the child for the key
		depth += node->prefix_length;

		top.pos = node->GetChildGreaterEqual(key[depth], equal);
		if (top.pos == DConstants::INVALID_INDEX) {
			// Find min leaf
			top.pos = node->GetMin();
		}
		node = node->GetChild(top.pos)->get();
		//! This means all children of this node qualify as geq

		depth++;
	}
}

bool ART::SearchGreater(ARTIndexScanState *state, bool inclusive, idx_t max_count, vector<row_t> &result_ids) {
	Iterator *it = &state->iterator;
	auto key = CreateKey(*this, types[0], state->values[0]);

	// greater than scan: first set the iterator to the node at which we will start our scan by finding the lowest node
	// that satisfies our requirement
	if (!it->start) {
		bool found = ART::Bound(tree, *key, *it, inclusive);
		if (!found) {
			return true;
		}
		it->start = true;
	}
	// after that we continue the scan; we don't need to check the bounds as any value following this value is
	// automatically bigger and hence satisfies our predicate
	return IteratorScan<false, false>(state, it, nullptr, max_count, result_ids);
}

//===--------------------------------------------------------------------===//
// Less Than
//===--------------------------------------------------------------------===//
static Leaf &FindMinimum(Iterator &it, Node &node) {
	Node *next = nullptr;
	idx_t pos = 0;
	switch (node.type) {
	case NodeType::NLeaf:
		it.node = (Leaf *)&node;
		return (Leaf &)node;
	case NodeType::N4:
		next = ((Node4 &)node).child[0].get();
		break;
	case NodeType::N16:
		next = ((Node16 &)node).child[0].get();
		break;
	case NodeType::N48: {
		auto &n48 = (Node48 &)node;
		while (n48.child_index[pos] == Node::EMPTY_MARKER) {
			pos++;
		}
		next = n48.child[n48.child_index[pos]].get();
		break;
	}
	case NodeType::N256: {
		auto &n256 = (Node256 &)node;
		while (!n256.child[pos]) {
			pos++;
		}
		next = n256.child[pos].get();
		break;
	}
	}
	it.SetEntry(it.depth, IteratorEntry(&node, pos));
	it.depth++;
	return FindMinimum(it, *next);
}

bool ART::SearchLess(ARTIndexScanState *state, bool inclusive, idx_t max_count, vector<row_t> &result_ids) {
	if (!tree) {
		return true;
	}

	Iterator *it = &state->iterator;
	auto upper_bound = CreateKey(*this, types[0], state->values[0]);

	if (!it->start) {
		// first find the minimum value in the ART: we start scanning from this value
		auto &minimum = FindMinimum(state->iterator, *tree);
		// early out min value higher than upper bound query
		if (*minimum.value > *upper_bound) {
			return true;
		}
		it->start = true;
	}
	// now continue the scan until we reach the upper bound
	if (inclusive) {
		return IteratorScan<true, true>(state, it, upper_bound.get(), max_count, result_ids);
	} else {
		return IteratorScan<true, false>(state, it, upper_bound.get(), max_count, result_ids);
	}
}

//===--------------------------------------------------------------------===//
// Closed Range Query
//===--------------------------------------------------------------------===//
bool ART::SearchCloseRange(ARTIndexScanState *state, bool left_inclusive, bool right_inclusive, idx_t max_count,
                           vector<row_t> &result_ids) {
	auto lower_bound = CreateKey(*this, types[0], state->values[0]);
	auto upper_bound = CreateKey(*this, types[0], state->values[1]);
	Iterator *it = &state->iterator;
	// first find the first node that satisfies the left predicate
	if (!it->start) {
		bool found = ART::Bound(tree, *lower_bound, *it, left_inclusive);
		if (!found) {
			return true;
		}
		it->start = true;
	}
	// now continue the scan until we reach the upper bound
	if (right_inclusive) {
		return IteratorScan<true, true>(state, it, upper_bound.get(), max_count, result_ids);
	} else {
		return IteratorScan<true, false>(state, it, upper_bound.get(), max_count, result_ids);
	}
}

bool ART::Scan(Transaction &transaction, DataTable &table, IndexScanState &table_state, idx_t max_count,
               vector<row_t> &result_ids) {
	auto state = (ARTIndexScanState *)&table_state;

	D_ASSERT(state->values[0].type().InternalType() == types[0]);

	vector<row_t> row_ids;
	bool success = true;
	if (state->values[1].IsNull()) {
		lock_guard<mutex> l(lock);
		// single predicate
		switch (state->expressions[0]) {
		case ExpressionType::COMPARE_EQUAL:
			success = SearchEqual(state, max_count, row_ids);
			break;
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			success = SearchGreater(state, true, max_count, row_ids);
			break;
		case ExpressionType::COMPARE_GREATERTHAN:
			success = SearchGreater(state, false, max_count, row_ids);
			break;
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			success = SearchLess(state, true, max_count, row_ids);
			break;
		case ExpressionType::COMPARE_LESSTHAN:
			success = SearchLess(state, false, max_count, row_ids);
			break;
		default:
			throw InternalException("Operation not implemented");
		}
	} else {
		lock_guard<mutex> l(lock);
		// two predicates
		D_ASSERT(state->values[1].type().InternalType() == types[0]);
		bool left_inclusive = state->expressions[0] == ExpressionType ::COMPARE_GREATERTHANOREQUALTO;
		bool right_inclusive = state->expressions[1] == ExpressionType ::COMPARE_LESSTHANOREQUALTO;
		success = SearchCloseRange(state, left_inclusive, right_inclusive, max_count, row_ids);
	}
	if (!success) {
		return false;
	}
	if (row_ids.empty()) {
		return true;
	}
	// sort the row ids
	sort(row_ids.begin(), row_ids.end());
	// duplicate eliminate the row ids and append them to the row ids of the state
	result_ids.reserve(row_ids.size());

	result_ids.push_back(row_ids[0]);
	for (idx_t i = 1; i < row_ids.size(); i++) {
		if (row_ids[i] != row_ids[i - 1]) {
			result_ids.push_back(row_ids[i]);
		}
	}
	return true;
}

void ART::VerifyExistence(DataChunk &chunk, VerifyExistenceType verify_type, string *err_msg_ptr) {
	if (verify_type != VerifyExistenceType::DELETE_FK && !IsUnique()) {
		return;
	}

	DataChunk expression_result;
	expression_result.Initialize(Allocator::DefaultAllocator(), logical_types);

	// unique index, check
	lock_guard<mutex> l(lock);
	// first resolve the expressions for the index
	ExecuteExpressions(chunk, expression_result);

	// generate the keys for the given input
	vector<unique_ptr<Key>> keys;
	GenerateKeys(expression_result, keys);

	for (idx_t i = 0; i < chunk.size(); i++) {
		if (!keys[i]) {
			continue;
		}
		Node *node_ptr = Lookup(tree, *keys[i], 0);
		bool throw_exception =
		    verify_type == VerifyExistenceType::APPEND_FK ? node_ptr == nullptr : node_ptr != nullptr;
		if (!throw_exception) {
			continue;
		}
		string key_name;
		for (idx_t k = 0; k < expression_result.ColumnCount(); k++) {
			if (k > 0) {
				key_name += ", ";
			}
			key_name += unbound_expressions[k]->GetName() + ": " + expression_result.data[k].GetValue(i).ToString();
		}
		string exception_msg;
		switch (verify_type) {
		case VerifyExistenceType::APPEND: {
			// node already exists in tree
			string type = IsPrimary() ? "primary key" : "unique";
			exception_msg = "duplicate key \"" + key_name + "\" violates ";
			exception_msg += type + " constraint";
			break;
		}
		case VerifyExistenceType::APPEND_FK: {
			// found node no exists in tree
			exception_msg =
			    "violates foreign key constraint because key \"" + key_name + "\" does not exist in referenced table";
			break;
		}
		case VerifyExistenceType::DELETE_FK: {
			// found node exists in tree
			exception_msg =
			    "violates foreign key constraint because key \"" + key_name + "\" exist in table has foreign key";
			break;
		}
		}
		if (err_msg_ptr) {
			err_msg_ptr[i] = exception_msg;
		} else {
			throw ConstraintException(exception_msg);
		}
	}
}

} // namespace duckdb




namespace duckdb {

Key::Key(unique_ptr<data_t[]> data, idx_t len) : len(len), data(move(data)) {
}

template <>
unique_ptr<Key> Key::CreateKey(string_t value, bool is_little_endian) {
	idx_t len = value.GetSize() + 1;
	auto data = unique_ptr<data_t[]>(new data_t[len]);
	memcpy(data.get(), value.GetDataUnsafe(), len - 1);
	data[len - 1] = '\0';
	return make_unique<Key>(move(data), len);
}

template <>
unique_ptr<Key> Key::CreateKey(const char *value, bool is_little_endian) {
	return Key::CreateKey(string_t(value, strlen(value)), is_little_endian);
}

bool Key::operator>(const Key &k) const {
	for (idx_t i = 0; i < MinValue<idx_t>(len, k.len); i++) {
		if (data[i] > k.data[i]) {
			return true;
		} else if (data[i] < k.data[i]) {
			return false;
		}
	}
	return len > k.len;
}

bool Key::operator<(const Key &k) const {
	for (idx_t i = 0; i < MinValue<idx_t>(len, k.len); i++) {
		if (data[i] < k.data[i]) {
			return true;
		} else if (data[i] > k.data[i]) {
			return false;
		}
	}
	return len < k.len;
}

bool Key::operator>=(const Key &k) const {
	for (idx_t i = 0; i < MinValue<idx_t>(len, k.len); i++) {
		if (data[i] > k.data[i]) {
			return true;
		} else if (data[i] < k.data[i]) {
			return false;
		}
	}
	return len >= k.len;
}

bool Key::operator==(const Key &k) const {
	if (len != k.len) {
		return false;
	}
	for (idx_t i = 0; i < len; i++) {
		if (data[i] != k.data[i]) {
			return false;
		}
	}
	return true;
}
} // namespace duckdb



#include <cstring>

namespace duckdb {

Leaf::Leaf(ART &art, unique_ptr<Key> value, row_t row_id) : Node(art, NodeType::NLeaf, 0) {
	this->value = move(value);
	this->capacity = 1;
	this->row_ids = unique_ptr<row_t[]>(new row_t[this->capacity]);
	this->row_ids[0] = row_id;
	this->num_elements = 1;
}

void Leaf::Insert(row_t row_id) {
	// Grow array
	if (num_elements == capacity) {
		auto new_row_id = unique_ptr<row_t[]>(new row_t[capacity * 2]);
		memcpy(new_row_id.get(), row_ids.get(), capacity * sizeof(row_t));
		capacity *= 2;
		row_ids = move(new_row_id);
	}
	row_ids[num_elements++] = row_id;
}

void Leaf::Remove(row_t row_id) {
	idx_t entry_offset = DConstants::INVALID_INDEX;
	for (idx_t i = 0; i < num_elements; i++) {
		if (row_ids[i] == row_id) {
			entry_offset = i;
			break;
		}
	}
	if (entry_offset == DConstants::INVALID_INDEX) {
		return;
	}
	num_elements--;
	if (capacity > 2 && num_elements < capacity / 2) {
		// Shrink array, if less than half full
		auto new_row_id = unique_ptr<row_t[]>(new row_t[capacity / 2]);
		memcpy(new_row_id.get(), row_ids.get(), entry_offset * sizeof(row_t));
		memcpy(new_row_id.get() + entry_offset, row_ids.get() + entry_offset + 1,
		       (num_elements - entry_offset) * sizeof(row_t));
		capacity /= 2;
		row_ids = move(new_row_id);
	} else {
		// Copy the rest
		for (idx_t j = entry_offset; j < num_elements; j++) {
			row_ids[j] = row_ids[j + 1];
		}
	}
}

} // namespace duckdb




namespace duckdb {

Node::Node(ART &art, NodeType type, size_t compressed_prefix_size) : prefix_length(0), count(0), type(type) {
	this->prefix = unique_ptr<uint8_t[]>(new uint8_t[compressed_prefix_size]);
}

void Node::CopyPrefix(ART &art, Node *src, Node *dst) {
	dst->prefix_length = src->prefix_length;
	memcpy(dst->prefix.get(), src->prefix.get(), src->prefix_length);
}

// LCOV_EXCL_START
unique_ptr<Node> *Node::GetChild(idx_t pos) {
	D_ASSERT(0);
	return nullptr;
}

idx_t Node::GetMin() {
	D_ASSERT(0);
	return 0;
}
// LCOV_EXCL_STOP

uint32_t Node::PrefixMismatch(ART &art, Node *node, Key &key, uint64_t depth) {
	uint64_t pos;
	for (pos = 0; pos < node->prefix_length; pos++) {
		if (key[depth + pos] != node->prefix[pos]) {
			return pos;
		}
	}
	return pos;
}

void Node::InsertLeaf(ART &art, unique_ptr<Node> &node, uint8_t key, unique_ptr<Node> &new_node) {
	switch (node->type) {
	case NodeType::N4:
		Node4::Insert(art, node, key, new_node);
		break;
	case NodeType::N16:
		Node16::Insert(art, node, key, new_node);
		break;
	case NodeType::N48:
		Node48::Insert(art, node, key, new_node);
		break;
	case NodeType::N256:
		Node256::Insert(art, node, key, new_node);
		break;
	default:
		throw InternalException("Unrecognized leaf type for insert");
	}
}

void Node::Erase(ART &art, unique_ptr<Node> &node, idx_t pos) {
	switch (node->type) {
	case NodeType::N4: {
		Node4::Erase(art, node, pos);
		break;
	}
	case NodeType::N16: {
		Node16::Erase(art, node, pos);
		break;
	}
	case NodeType::N48: {
		Node48::Erase(art, node, pos);
		break;
	}
	case NodeType::N256:
		Node256::Erase(art, node, pos);
		break;
	default:
		throw InternalException("Unrecognized leaf type for erase");
	}
}

} // namespace duckdb




#include <cstring>

namespace duckdb {

Node16::Node16(ART &art, size_t compression_length) : Node(art, NodeType::N16, compression_length) {
	memset(key, 16, sizeof(key));
}

idx_t Node16::GetChildPos(uint8_t k) {
	for (idx_t pos = 0; pos < count; pos++) {
		if (key[pos] == k) {
			return pos;
		}
	}
	return Node::GetChildPos(k);
}

idx_t Node16::GetChildGreaterEqual(uint8_t k, bool &equal) {
	for (idx_t pos = 0; pos < count; pos++) {
		if (key[pos] >= k) {
			if (key[pos] == k) {
				equal = true;
			} else {
				equal = false;
			}

			return pos;
		}
	}
	return Node::GetChildGreaterEqual(k, equal);
}

idx_t Node16::GetNextPos(idx_t pos) {
	if (pos == DConstants::INVALID_INDEX) {
		return 0;
	}
	pos++;
	return pos < count ? pos : DConstants::INVALID_INDEX;
}

unique_ptr<Node> *Node16::GetChild(idx_t pos) {
	D_ASSERT(pos < count);
	return &child[pos];
}

idx_t Node16::GetMin() {
	return 0;
}

void Node16::Insert(ART &art, unique_ptr<Node> &node, uint8_t key_byte, unique_ptr<Node> &child) {
	Node16 *n = static_cast<Node16 *>(node.get());

	if (n->count < 16) {
		// Insert element
		idx_t pos = 0;
		while (pos < node->count && n->key[pos] < key_byte) {
			pos++;
		}
		if (n->child[pos] != nullptr) {
			for (idx_t i = n->count; i > pos; i--) {
				n->key[i] = n->key[i - 1];
				n->child[i] = move(n->child[i - 1]);
			}
		}
		n->key[pos] = key_byte;
		n->child[pos] = move(child);
		n->count++;
	} else {
		// Grow to Node48
		auto new_node = make_unique<Node48>(art, n->prefix_length);
		for (idx_t i = 0; i < node->count; i++) {
			new_node->child_index[n->key[i]] = i;
			new_node->child[i] = move(n->child[i]);
		}
		CopyPrefix(art, n, new_node.get());
		new_node->count = node->count;
		node = move(new_node);

		Node48::Insert(art, node, key_byte, child);
	}
}

void Node16::Erase(ART &art, unique_ptr<Node> &node, int pos) {
	Node16 *n = static_cast<Node16 *>(node.get());
	// erase the child and decrease the count
	n->child[pos].reset();
	n->count--;
	// potentially move any children backwards
	for (; pos < n->count; pos++) {
		n->key[pos] = n->key[pos + 1];
		n->child[pos] = move(n->child[pos + 1]);
	}
	if (node->count <= 3) {
		// Shrink node
		auto new_node = make_unique<Node4>(art, n->prefix_length);
		for (unsigned i = 0; i < n->count; i++) {
			new_node->key[new_node->count] = n->key[i];
			new_node->child[new_node->count++] = move(n->child[i]);
		}
		CopyPrefix(art, n, new_node.get());
		node = move(new_node);
	}
}

} // namespace duckdb



namespace duckdb {

Node256::Node256(ART &art, size_t compression_length) : Node(art, NodeType::N256, compression_length) {
}

idx_t Node256::GetChildPos(uint8_t k) {
	if (child[k]) {
		return k;
	} else {
		return DConstants::INVALID_INDEX;
	}
}

idx_t Node256::GetChildGreaterEqual(uint8_t k, bool &equal) {
	for (idx_t pos = k; pos < 256; pos++) {
		if (child[pos]) {
			if (pos == k) {
				equal = true;
			} else {
				equal = false;
			}
			return pos;
		}
	}
	return DConstants::INVALID_INDEX;
}

idx_t Node256::GetMin() {
	for (idx_t i = 0; i < 256; i++) {
		if (child[i]) {
			return i;
		}
	}
	return DConstants::INVALID_INDEX;
}

idx_t Node256::GetNextPos(idx_t pos) {
	for (pos == DConstants::INVALID_INDEX ? pos = 0 : pos++; pos < 256; pos++) {
		if (child[pos]) {
			return pos;
		}
	}
	return Node::GetNextPos(pos);
}

unique_ptr<Node> *Node256::GetChild(idx_t pos) {
	D_ASSERT(child[pos]);
	return &child[pos];
}

void Node256::Insert(ART &art, unique_ptr<Node> &node, uint8_t key_byte, unique_ptr<Node> &child) {
	Node256 *n = static_cast<Node256 *>(node.get());

	n->count++;
	n->child[key_byte] = move(child);
}

void Node256::Erase(ART &art, unique_ptr<Node> &node, int pos) {
	Node256 *n = static_cast<Node256 *>(node.get());

	n->child[pos].reset();
	n->count--;
	if (node->count <= 36) {
		auto new_node = make_unique<Node48>(art, n->prefix_length);
		CopyPrefix(art, n, new_node.get());
		for (idx_t i = 0; i < 256; i++) {
			if (n->child[i]) {
				new_node->child_index[i] = new_node->count;
				new_node->child[new_node->count] = move(n->child[i]);
				new_node->count++;
			}
		}
		node = move(new_node);
	}
}

} // namespace duckdb




namespace duckdb {

Node4::Node4(ART &art, size_t compression_length) : Node(art, NodeType::N4, compression_length) {
	memset(key, 0, sizeof(key));
}

idx_t Node4::GetChildPos(uint8_t k) {
	for (idx_t pos = 0; pos < count; pos++) {
		if (key[pos] == k) {
			return pos;
		}
	}
	return Node::GetChildPos(k);
}

idx_t Node4::GetChildGreaterEqual(uint8_t k, bool &equal) {
	for (idx_t pos = 0; pos < count; pos++) {
		if (key[pos] >= k) {
			if (key[pos] == k) {
				equal = true;
			} else {
				equal = false;
			}
			return pos;
		}
	}
	return Node::GetChildGreaterEqual(k, equal);
}

idx_t Node4::GetMin() {
	return 0;
}

idx_t Node4::GetNextPos(idx_t pos) {
	if (pos == DConstants::INVALID_INDEX) {
		return 0;
	}
	pos++;
	return pos < count ? pos : DConstants::INVALID_INDEX;
}

unique_ptr<Node> *Node4::GetChild(idx_t pos) {
	D_ASSERT(pos < count);
	return &child[pos];
}

void Node4::Insert(ART &art, unique_ptr<Node> &node, uint8_t key_byte, unique_ptr<Node> &child) {
	Node4 *n = static_cast<Node4 *>(node.get());

	// Insert leaf into inner node
	if (node->count < 4) {
		// Insert element
		idx_t pos = 0;
		while ((pos < node->count) && (n->key[pos] < key_byte)) {
			pos++;
		}
		if (n->child[pos] != nullptr) {
			for (idx_t i = n->count; i > pos; i--) {
				n->key[i] = n->key[i - 1];
				n->child[i] = move(n->child[i - 1]);
			}
		}
		n->key[pos] = key_byte;
		n->child[pos] = move(child);
		n->count++;
	} else {
		// Grow to Node16
		auto new_node = make_unique<Node16>(art, n->prefix_length);
		new_node->count = 4;
		CopyPrefix(art, node.get(), new_node.get());
		for (idx_t i = 0; i < 4; i++) {
			new_node->key[i] = n->key[i];
			new_node->child[i] = move(n->child[i]);
		}
		node = move(new_node);
		Node16::Insert(art, node, key_byte, child);
	}
}

void Node4::Erase(ART &art, unique_ptr<Node> &node, int pos) {
	Node4 *n = static_cast<Node4 *>(node.get());
	D_ASSERT(pos < n->count);

	// erase the child and decrease the count
	n->child[pos].reset();
	n->count--;
	// potentially move any children backwards
	for (; pos < n->count; pos++) {
		n->key[pos] = n->key[pos + 1];
		n->child[pos] = move(n->child[pos + 1]);
	}

	// This is a one way node
	if (n->count == 1) {
		auto childref = n->child[0].get();
		//! concatenate prefixes
		auto new_length = node->prefix_length + childref->prefix_length + 1;
		//! have to allocate space in our prefix array
		unique_ptr<uint8_t[]> new_prefix = unique_ptr<uint8_t[]>(new uint8_t[new_length]);

		//! first move the existing prefix (if any)
		for (uint32_t i = 0; i < childref->prefix_length; i++) {
			new_prefix[new_length - (i + 1)] = childref->prefix[childref->prefix_length - (i + 1)];
		}
		//! now move the current key as part of the prefix
		new_prefix[node->prefix_length] = n->key[0];
		//! finally add the old prefix
		for (uint32_t i = 0; i < node->prefix_length; i++) {
			new_prefix[i] = node->prefix[i];
		}
		//! set new prefix and move the child
		childref->prefix = move(new_prefix);
		childref->prefix_length = new_length;
		node = move(n->child[0]);
	}
}

} // namespace duckdb




namespace duckdb {

Node48::Node48(ART &art, size_t compression_length) : Node(art, NodeType::N48, compression_length) {
	for (idx_t i = 0; i < 256; i++) {
		child_index[i] = Node::EMPTY_MARKER;
	}
}

idx_t Node48::GetChildPos(uint8_t k) {
	if (child_index[k] == Node::EMPTY_MARKER) {
		return DConstants::INVALID_INDEX;
	} else {
		return k;
	}
}

idx_t Node48::GetChildGreaterEqual(uint8_t k, bool &equal) {
	for (idx_t pos = k; pos < 256; pos++) {
		if (child_index[pos] != Node::EMPTY_MARKER) {
			if (pos == k) {
				equal = true;
			} else {
				equal = false;
			}
			return pos;
		}
	}
	return Node::GetChildGreaterEqual(k, equal);
}

idx_t Node48::GetNextPos(idx_t pos) {
	for (pos == DConstants::INVALID_INDEX ? pos = 0 : pos++; pos < 256; pos++) {
		if (child_index[pos] != Node::EMPTY_MARKER) {
			return pos;
		}
	}
	return Node::GetNextPos(pos);
}

unique_ptr<Node> *Node48::GetChild(idx_t pos) {
	D_ASSERT(child_index[pos] != Node::EMPTY_MARKER);
	return &child[child_index[pos]];
}

idx_t Node48::GetMin() {
	for (idx_t i = 0; i < 256; i++) {
		if (child_index[i] != Node::EMPTY_MARKER) {
			return i;
		}
	}
	return DConstants::INVALID_INDEX;
}

void Node48::Insert(ART &art, unique_ptr<Node> &node, uint8_t key_byte, unique_ptr<Node> &child) {
	Node48 *n = static_cast<Node48 *>(node.get());

	// Insert leaf into inner node
	if (node->count < 48) {
		// Insert element
		idx_t pos = n->count;
		if (n->child[pos]) {
			// find an empty position in the node list if the current position is occupied
			pos = 0;
			while (n->child[pos]) {
				pos++;
			}
		}
		n->child[pos] = move(child);
		n->child_index[key_byte] = pos;
		n->count++;
	} else {
		// Grow to Node256
		auto new_node = make_unique<Node256>(art, n->prefix_length);
		for (idx_t i = 0; i < 256; i++) {
			if (n->child_index[i] != Node::EMPTY_MARKER) {
				new_node->child[i] = move(n->child[n->child_index[i]]);
			}
		}
		new_node->count = n->count;
		CopyPrefix(art, n, new_node.get());
		node = move(new_node);
		Node256::Insert(art, node, key_byte, child);
	}
}

void Node48::Erase(ART &art, unique_ptr<Node> &node, int pos) {
	Node48 *n = static_cast<Node48 *>(node.get());

	n->child[n->child_index[pos]].reset();
	n->child_index[pos] = Node::EMPTY_MARKER;
	n->count--;
	if (node->count <= 12) {
		auto new_node = make_unique<Node16>(art, n->prefix_length);
		CopyPrefix(art, n, new_node.get());
		for (idx_t i = 0; i < 256; i++) {
			if (n->child_index[i] != Node::EMPTY_MARKER) {
				new_node->key[new_node->count] = i;
				new_node->child[new_node->count++] = move(n->child[n->child_index[i]]);
			}
		}
		node = move(new_node);
	}
}

} // namespace duckdb








namespace duckdb {

using ValidityBytes = JoinHashTable::ValidityBytes;
using ScanStructure = JoinHashTable::ScanStructure;

JoinHashTable::JoinHashTable(BufferManager &buffer_manager, const vector<JoinCondition> &conditions,
                             vector<LogicalType> btypes, JoinType type)
    : buffer_manager(buffer_manager), build_types(move(btypes)), entry_size(0), tuple_size(0),
      vfound(Value::BOOLEAN(false)), join_type(type), finalized(false), has_null(false) {
	for (auto &condition : conditions) {
		D_ASSERT(condition.left->return_type == condition.right->return_type);
		auto type = condition.left->return_type;
		if (condition.comparison == ExpressionType::COMPARE_EQUAL ||
		    condition.comparison == ExpressionType::COMPARE_NOT_DISTINCT_FROM ||
		    condition.comparison == ExpressionType::COMPARE_DISTINCT_FROM) {
			// all equality conditions should be at the front
			// all other conditions at the back
			// this assert checks that
			D_ASSERT(equality_types.size() == condition_types.size());
			equality_types.push_back(type);
		}

		predicates.push_back(condition.comparison);
		null_values_are_equal.push_back(condition.comparison == ExpressionType::COMPARE_DISTINCT_FROM ||
		                                condition.comparison == ExpressionType::COMPARE_NOT_DISTINCT_FROM);

		condition_types.push_back(type);
	}
	// at least one equality is necessary
	D_ASSERT(!equality_types.empty());

	// Types for the layout
	vector<LogicalType> layout_types(condition_types);
	layout_types.insert(layout_types.end(), build_types.begin(), build_types.end());
	if (IsRightOuterJoin(join_type)) {
		// full/right outer joins need an extra bool to keep track of whether or not a tuple has found a matching entry
		// we place the bool before the NEXT pointer
		layout_types.emplace_back(LogicalType::BOOLEAN);
	}
	layout_types.emplace_back(LogicalType::HASH);
	layout.Initialize(layout_types, false);

	const auto &offsets = layout.GetOffsets();
	tuple_size = offsets[condition_types.size() + build_types.size()];
	pointer_offset = offsets.back();
	entry_size = layout.GetRowWidth();

	// compute the per-block capacity of this HT
	idx_t block_capacity = MaxValue<idx_t>(STANDARD_VECTOR_SIZE, (Storage::BLOCK_SIZE / entry_size) + 1);
	block_collection = make_unique<RowDataCollection>(buffer_manager, block_capacity, entry_size);
	string_heap = make_unique<RowDataCollection>(buffer_manager, (idx_t)Storage::BLOCK_SIZE, 1, true);
}

JoinHashTable::~JoinHashTable() {
}

void JoinHashTable::ApplyBitmask(Vector &hashes, idx_t count) {
	if (hashes.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		D_ASSERT(!ConstantVector::IsNull(hashes));
		auto indices = ConstantVector::GetData<hash_t>(hashes);
		*indices = *indices & bitmask;
	} else {
		hashes.Normalify(count);
		auto indices = FlatVector::GetData<hash_t>(hashes);
		for (idx_t i = 0; i < count; i++) {
			indices[i] &= bitmask;
		}
	}
}

void JoinHashTable::ApplyBitmask(Vector &hashes, const SelectionVector &sel, idx_t count, Vector &pointers) {
	VectorData hdata;
	hashes.Orrify(count, hdata);

	auto hash_data = (hash_t *)hdata.data;
	auto result_data = FlatVector::GetData<data_ptr_t *>(pointers);
	auto main_ht = (data_ptr_t *)hash_map.Ptr();
	for (idx_t i = 0; i < count; i++) {
		auto rindex = sel.get_index(i);
		auto hindex = hdata.sel->get_index(rindex);
		auto hash = hash_data[hindex];
		result_data[rindex] = main_ht + (hash & bitmask);
	}
}

void JoinHashTable::Hash(DataChunk &keys, const SelectionVector &sel, idx_t count, Vector &hashes) {
	if (count == keys.size()) {
		// no null values are filtered: use regular hash functions
		VectorOperations::Hash(keys.data[0], hashes, keys.size());
		for (idx_t i = 1; i < equality_types.size(); i++) {
			VectorOperations::CombineHash(hashes, keys.data[i], keys.size());
		}
	} else {
		// null values were filtered: use selection vector
		VectorOperations::Hash(keys.data[0], hashes, sel, count);
		for (idx_t i = 1; i < equality_types.size(); i++) {
			VectorOperations::CombineHash(hashes, keys.data[i], sel, count);
		}
	}
}

static idx_t FilterNullValues(VectorData &vdata, const SelectionVector &sel, idx_t count, SelectionVector &result) {
	idx_t result_count = 0;
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		auto key_idx = vdata.sel->get_index(idx);
		if (vdata.validity.RowIsValid(key_idx)) {
			result.set_index(result_count++, idx);
		}
	}
	return result_count;
}

idx_t JoinHashTable::PrepareKeys(DataChunk &keys, unique_ptr<VectorData[]> &key_data,
                                 const SelectionVector *&current_sel, SelectionVector &sel, bool build_side) {
	key_data = keys.Orrify();

	// figure out which keys are NULL, and create a selection vector out of them
	current_sel = FlatVector::IncrementalSelectionVector();
	idx_t added_count = keys.size();
	if (build_side && IsRightOuterJoin(join_type)) {
		// in case of a right or full outer join, we cannot remove NULL keys from the build side
		return added_count;
	}
	for (idx_t i = 0; i < keys.ColumnCount(); i++) {
		if (!null_values_are_equal[i]) {
			if (key_data[i].validity.AllValid()) {
				continue;
			}
			added_count = FilterNullValues(key_data[i], *current_sel, added_count, sel);
			// null values are NOT equal for this column, filter them out
			current_sel = &sel;
		}
	}
	return added_count;
}

void JoinHashTable::Build(DataChunk &keys, DataChunk &payload) {
	D_ASSERT(!finalized);
	D_ASSERT(keys.size() == payload.size());
	if (keys.size() == 0) {
		return;
	}
	// special case: correlated mark join
	if (join_type == JoinType::MARK && !correlated_mark_join_info.correlated_types.empty()) {
		auto &info = correlated_mark_join_info;
		lock_guard<mutex> mj_lock(info.mj_lock);
		// Correlated MARK join
		// for the correlated mark join we need to keep track of COUNT(*) and COUNT(COLUMN) for each of the correlated
		// columns push into the aggregate hash table
		D_ASSERT(info.correlated_counts);
		info.group_chunk.SetCardinality(keys);
		for (idx_t i = 0; i < info.correlated_types.size(); i++) {
			info.group_chunk.data[i].Reference(keys.data[i]);
		}
		if (info.correlated_payload.data.empty()) {
			vector<LogicalType> types;
			types.push_back(keys.data[info.correlated_types.size()].GetType());
			info.correlated_payload.InitializeEmpty(types);
		}
		info.correlated_payload.SetCardinality(keys);
		info.correlated_payload.data[0].Reference(keys.data[info.correlated_types.size()]);
		info.correlated_counts->AddChunk(info.group_chunk, info.correlated_payload);
	}

	// prepare the keys for processing
	unique_ptr<VectorData[]> key_data;
	const SelectionVector *current_sel;
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	idx_t added_count = PrepareKeys(keys, key_data, current_sel, sel, true);
	if (added_count < keys.size()) {
		has_null = true;
	}
	if (added_count == 0) {
		return;
	}

	// build out the buffer space
	Vector addresses(LogicalType::POINTER);
	auto key_locations = FlatVector::GetData<data_ptr_t>(addresses);
	auto handles = block_collection->Build(added_count, key_locations, nullptr, current_sel);

	// hash the keys and obtain an entry in the list
	// note that we only hash the keys used in the equality comparison
	Vector hash_values(LogicalType::HASH);
	Hash(keys, *current_sel, added_count, hash_values);

	// build a chunk so we can handle nested types that need more than Orrification
	DataChunk source_chunk;
	source_chunk.InitializeEmpty(layout.GetTypes());

	vector<VectorData> source_data;
	source_data.reserve(layout.ColumnCount());

	// serialize the keys to the key locations
	for (idx_t i = 0; i < keys.ColumnCount(); i++) {
		source_chunk.data[i].Reference(keys.data[i]);
		source_data.emplace_back(move(key_data[i]));
	}
	// now serialize the payload
	D_ASSERT(build_types.size() == payload.ColumnCount());
	for (idx_t i = 0; i < payload.ColumnCount(); i++) {
		source_chunk.data[source_data.size()].Reference(payload.data[i]);
		VectorData pdata;
		payload.data[i].Orrify(payload.size(), pdata);
		source_data.emplace_back(move(pdata));
	}
	if (IsRightOuterJoin(join_type)) {
		// for FULL/RIGHT OUTER joins initialize the "found" boolean to false
		source_chunk.data[source_data.size()].Reference(vfound);
		VectorData fdata;
		vfound.Orrify(keys.size(), fdata);
		source_data.emplace_back(move(fdata));
	}

	// serialise the hashes at the end
	source_chunk.data[source_data.size()].Reference(hash_values);
	VectorData hdata;
	hash_values.Orrify(keys.size(), hdata);
	source_data.emplace_back(move(hdata));

	source_chunk.SetCardinality(keys);

	RowOperations::Scatter(source_chunk, source_data.data(), layout, addresses, *string_heap, *current_sel,
	                       added_count);
}

void JoinHashTable::InsertHashes(Vector &hashes, idx_t count, data_ptr_t key_locations[]) {
	D_ASSERT(hashes.GetType().id() == LogicalType::HASH);

	// use bitmask to get position in array
	ApplyBitmask(hashes, count);

	hashes.Normalify(count);

	D_ASSERT(hashes.GetVectorType() == VectorType::FLAT_VECTOR);
	auto pointers = (data_ptr_t *)hash_map.Ptr();
	auto indices = FlatVector::GetData<hash_t>(hashes);
	for (idx_t i = 0; i < count; i++) {
		auto index = indices[i];
		// set prev in current key to the value (NOTE: this will be nullptr if
		// there is none)
		Store<data_ptr_t>(pointers[index], key_locations[i] + pointer_offset);

		// set pointer to current tuple
		pointers[index] = key_locations[i];
	}
}

void JoinHashTable::Finalize() {
	// the build has finished, now iterate over all the nodes and construct the final hash table
	// select a HT that has at least 50% empty space
	idx_t capacity = NextPowerOfTwo(MaxValue<idx_t>(Count() * 2, (Storage::BLOCK_SIZE / sizeof(data_ptr_t)) + 1));
	// size needs to be a power of 2
	D_ASSERT((capacity & (capacity - 1)) == 0);
	bitmask = capacity - 1;

	// allocate the HT and initialize it with all-zero entries
	hash_map = buffer_manager.Allocate(capacity * sizeof(data_ptr_t));
	memset(hash_map.Ptr(), 0, capacity * sizeof(data_ptr_t));

	Vector hashes(LogicalType::HASH);
	auto hash_data = FlatVector::GetData<hash_t>(hashes);
	data_ptr_t key_locations[STANDARD_VECTOR_SIZE];
	// now construct the actual hash table; scan the nodes
	// as we can the nodes we pin all the blocks of the HT and keep them pinned until the HT is destroyed
	// this is so that we can keep pointers around to the blocks
	// FIXME: if we cannot keep everything pinned in memory, we could switch to an out-of-memory merge join or so
	for (auto &block : block_collection->blocks) {
		auto handle = buffer_manager.Pin(block.block);
		data_ptr_t dataptr = handle.Ptr();
		idx_t entry = 0;
		while (entry < block.count) {
			// fetch the next vector of entries from the blocks
			idx_t next = MinValue<idx_t>(STANDARD_VECTOR_SIZE, block.count - entry);
			for (idx_t i = 0; i < next; i++) {
				hash_data[i] = Load<hash_t>((data_ptr_t)(dataptr + pointer_offset));
				key_locations[i] = dataptr;
				dataptr += entry_size;
			}
			// now insert into the hash table
			InsertHashes(hashes, next, key_locations);

			entry += next;
		}
		pinned_handles.push_back(move(handle));
	}

	finalized = true;
}

unique_ptr<ScanStructure> JoinHashTable::Probe(DataChunk &keys) {
	D_ASSERT(Count() > 0); // should be handled before
	D_ASSERT(finalized);

	// set up the scan structure
	auto ss = make_unique<ScanStructure>(*this);

	if (join_type != JoinType::INNER) {
		ss->found_match = unique_ptr<bool[]>(new bool[STANDARD_VECTOR_SIZE]);
		memset(ss->found_match.get(), 0, sizeof(bool) * STANDARD_VECTOR_SIZE);
	}

	// first prepare the keys for probing
	const SelectionVector *current_sel;
	ss->count = PrepareKeys(keys, ss->key_data, current_sel, ss->sel_vector, false);
	if (ss->count == 0) {
		return ss;
	}

	// hash all the keys
	Vector hashes(LogicalType::HASH);
	Hash(keys, *current_sel, ss->count, hashes);

	// now initialize the pointers of the scan structure based on the hashes
	ApplyBitmask(hashes, *current_sel, ss->count, ss->pointers);

	// create the selection vector linking to only non-empty entries
	idx_t count = 0;
	auto pointers = FlatVector::GetData<data_ptr_t>(ss->pointers);
	for (idx_t i = 0; i < ss->count; i++) {
		auto idx = current_sel->get_index(i);
		pointers[idx] = Load<data_ptr_t>(pointers[idx]);
		if (pointers[idx]) {
			ss->sel_vector.set_index(count++, idx);
		}
	}
	ss->count = count;
	return ss;
}

ScanStructure::ScanStructure(JoinHashTable &ht)
    : pointers(LogicalType::POINTER), sel_vector(STANDARD_VECTOR_SIZE), ht(ht), finished(false) {
}

void ScanStructure::Next(DataChunk &keys, DataChunk &left, DataChunk &result) {
	if (finished) {
		return;
	}

	switch (ht.join_type) {
	case JoinType::INNER:
	case JoinType::RIGHT:
		NextInnerJoin(keys, left, result);
		break;
	case JoinType::SEMI:
		NextSemiJoin(keys, left, result);
		break;
	case JoinType::MARK:
		NextMarkJoin(keys, left, result);
		break;
	case JoinType::ANTI:
		NextAntiJoin(keys, left, result);
		break;
	case JoinType::OUTER:
	case JoinType::LEFT:
		NextLeftJoin(keys, left, result);
		break;
	case JoinType::SINGLE:
		NextSingleJoin(keys, left, result);
		break;
	default:
		throw InternalException("Unhandled join type in JoinHashTable");
	}
}

idx_t ScanStructure::ResolvePredicates(DataChunk &keys, SelectionVector &match_sel, SelectionVector *no_match_sel) {
	// Start with the scan selection
	for (idx_t i = 0; i < this->count; ++i) {
		match_sel.set_index(i, this->sel_vector.get_index(i));
	}
	idx_t no_match_count = 0;

	return RowOperations::Match(keys, key_data.get(), ht.layout, pointers, ht.predicates, match_sel, this->count,
	                            no_match_sel, no_match_count);
}

idx_t ScanStructure::ScanInnerJoin(DataChunk &keys, SelectionVector &result_vector) {
	while (true) {
		// resolve the predicates for this set of keys
		idx_t result_count = ResolvePredicates(keys, result_vector, nullptr);

		// after doing all the comparisons set the found_match vector
		if (found_match) {
			for (idx_t i = 0; i < result_count; i++) {
				auto idx = result_vector.get_index(i);
				found_match[idx] = true;
			}
		}
		if (result_count > 0) {
			return result_count;
		}
		// no matches found: check the next set of pointers
		AdvancePointers();
		if (this->count == 0) {
			return 0;
		}
	}
}

void ScanStructure::AdvancePointers(const SelectionVector &sel, idx_t sel_count) {
	// now for all the pointers, we move on to the next set of pointers
	idx_t new_count = 0;
	auto ptrs = FlatVector::GetData<data_ptr_t>(this->pointers);
	for (idx_t i = 0; i < sel_count; i++) {
		auto idx = sel.get_index(i);
		ptrs[idx] = Load<data_ptr_t>(ptrs[idx] + ht.pointer_offset);
		if (ptrs[idx]) {
			this->sel_vector.set_index(new_count++, idx);
		}
	}
	this->count = new_count;
}

void ScanStructure::AdvancePointers() {
	AdvancePointers(this->sel_vector, this->count);
}

void ScanStructure::GatherResult(Vector &result, const SelectionVector &result_vector,
                                 const SelectionVector &sel_vector, const idx_t count, const idx_t col_no) {
	const auto col_offset = ht.layout.GetOffsets()[col_no];
	RowOperations::Gather(pointers, sel_vector, result, result_vector, count, col_offset, col_no);
}

void ScanStructure::GatherResult(Vector &result, const SelectionVector &sel_vector, const idx_t count,
                                 const idx_t col_idx) {
	GatherResult(result, *FlatVector::IncrementalSelectionVector(), sel_vector, count, col_idx);
}

void ScanStructure::NextInnerJoin(DataChunk &keys, DataChunk &left, DataChunk &result) {
	D_ASSERT(result.ColumnCount() == left.ColumnCount() + ht.build_types.size());
	if (this->count == 0) {
		// no pointers left to chase
		return;
	}

	SelectionVector result_vector(STANDARD_VECTOR_SIZE);

	idx_t result_count = ScanInnerJoin(keys, result_vector);
	if (result_count > 0) {
		if (IsRightOuterJoin(ht.join_type)) {
			// full/right outer join: mark join matches as FOUND in the HT
			auto ptrs = FlatVector::GetData<data_ptr_t>(pointers);
			for (idx_t i = 0; i < result_count; i++) {
				auto idx = result_vector.get_index(i);
				// NOTE: threadsan reports this as a data race because this can be set concurrently by separate threads
				// Technically it is, but it does not matter, since the only value that can be written is "true"
				Store<bool>(true, ptrs[idx] + ht.tuple_size);
			}
		}
		// matches were found
		// construct the result
		// on the LHS, we create a slice using the result vector
		result.Slice(left, result_vector, result_count);

		// on the RHS, we need to fetch the data from the hash table
		for (idx_t i = 0; i < ht.build_types.size(); i++) {
			auto &vector = result.data[left.ColumnCount() + i];
			D_ASSERT(vector.GetType() == ht.build_types[i]);
			GatherResult(vector, result_vector, result_count, i + ht.condition_types.size());
		}
		AdvancePointers();
	}
}

void ScanStructure::ScanKeyMatches(DataChunk &keys) {
	// the semi-join, anti-join and mark-join we handle a differently from the inner join
	// since there can be at most STANDARD_VECTOR_SIZE results
	// we handle the entire chunk in one call to Next().
	// for every pointer, we keep chasing pointers and doing comparisons.
	// this results in a boolean array indicating whether or not the tuple has a match
	SelectionVector match_sel(STANDARD_VECTOR_SIZE), no_match_sel(STANDARD_VECTOR_SIZE);
	while (this->count > 0) {
		// resolve the predicates for the current set of pointers
		idx_t match_count = ResolvePredicates(keys, match_sel, &no_match_sel);
		idx_t no_match_count = this->count - match_count;

		// mark each of the matches as found
		for (idx_t i = 0; i < match_count; i++) {
			found_match[match_sel.get_index(i)] = true;
		}
		// continue searching for the ones where we did not find a match yet
		AdvancePointers(no_match_sel, no_match_count);
	}
}

template <bool MATCH>
void ScanStructure::NextSemiOrAntiJoin(DataChunk &keys, DataChunk &left, DataChunk &result) {
	D_ASSERT(left.ColumnCount() == result.ColumnCount());
	D_ASSERT(keys.size() == left.size());
	// create the selection vector from the matches that were found
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	idx_t result_count = 0;
	for (idx_t i = 0; i < keys.size(); i++) {
		if (found_match[i] == MATCH) {
			// part of the result
			sel.set_index(result_count++, i);
		}
	}
	// construct the final result
	if (result_count > 0) {
		// we only return the columns on the left side
		// reference the columns of the left side from the result
		result.Slice(left, sel, result_count);
	} else {
		D_ASSERT(result.size() == 0);
	}
}

void ScanStructure::NextSemiJoin(DataChunk &keys, DataChunk &left, DataChunk &result) {
	// first scan for key matches
	ScanKeyMatches(keys);
	// then construct the result from all tuples with a match
	NextSemiOrAntiJoin<true>(keys, left, result);

	finished = true;
}

void ScanStructure::NextAntiJoin(DataChunk &keys, DataChunk &left, DataChunk &result) {
	// first scan for key matches
	ScanKeyMatches(keys);
	// then construct the result from all tuples that did not find a match
	NextSemiOrAntiJoin<false>(keys, left, result);

	finished = true;
}

void ScanStructure::ConstructMarkJoinResult(DataChunk &join_keys, DataChunk &child, DataChunk &result) {
	// for the initial set of columns we just reference the left side
	result.SetCardinality(child);
	for (idx_t i = 0; i < child.ColumnCount(); i++) {
		result.data[i].Reference(child.data[i]);
	}
	auto &mark_vector = result.data.back();
	mark_vector.SetVectorType(VectorType::FLAT_VECTOR);
	// first we set the NULL values from the join keys
	// if there is any NULL in the keys, the result is NULL
	auto bool_result = FlatVector::GetData<bool>(mark_vector);
	auto &mask = FlatVector::Validity(mark_vector);
	for (idx_t col_idx = 0; col_idx < join_keys.ColumnCount(); col_idx++) {
		if (ht.null_values_are_equal[col_idx]) {
			continue;
		}
		VectorData jdata;
		join_keys.data[col_idx].Orrify(join_keys.size(), jdata);
		if (!jdata.validity.AllValid()) {
			for (idx_t i = 0; i < join_keys.size(); i++) {
				auto jidx = jdata.sel->get_index(i);
				mask.Set(i, jdata.validity.RowIsValidUnsafe(jidx));
			}
		}
	}
	// now set the remaining entries to either true or false based on whether a match was found
	if (found_match) {
		for (idx_t i = 0; i < child.size(); i++) {
			bool_result[i] = found_match[i];
		}
	} else {
		memset(bool_result, 0, sizeof(bool) * child.size());
	}
	// if the right side contains NULL values, the result of any FALSE becomes NULL
	if (ht.has_null) {
		for (idx_t i = 0; i < child.size(); i++) {
			if (!bool_result[i]) {
				mask.SetInvalid(i);
			}
		}
	}
}

void ScanStructure::NextMarkJoin(DataChunk &keys, DataChunk &input, DataChunk &result) {
	D_ASSERT(result.ColumnCount() == input.ColumnCount() + 1);
	D_ASSERT(result.data.back().GetType() == LogicalType::BOOLEAN);
	// this method should only be called for a non-empty HT
	D_ASSERT(ht.Count() > 0);

	ScanKeyMatches(keys);
	if (ht.correlated_mark_join_info.correlated_types.empty()) {
		ConstructMarkJoinResult(keys, input, result);
	} else {
		auto &info = ht.correlated_mark_join_info;
		// there are correlated columns
		// first we fetch the counts from the aggregate hashtable corresponding to these entries
		D_ASSERT(keys.ColumnCount() == info.group_chunk.ColumnCount() + 1);
		info.group_chunk.SetCardinality(keys);
		for (idx_t i = 0; i < info.group_chunk.ColumnCount(); i++) {
			info.group_chunk.data[i].Reference(keys.data[i]);
		}
		info.correlated_counts->FetchAggregates(info.group_chunk, info.result_chunk);

		// for the initial set of columns we just reference the left side
		result.SetCardinality(input);
		for (idx_t i = 0; i < input.ColumnCount(); i++) {
			result.data[i].Reference(input.data[i]);
		}
		// create the result matching vector
		auto &last_key = keys.data.back();
		auto &result_vector = result.data.back();
		// first set the nullmask based on whether or not there were NULL values in the join key
		result_vector.SetVectorType(VectorType::FLAT_VECTOR);
		auto bool_result = FlatVector::GetData<bool>(result_vector);
		auto &mask = FlatVector::Validity(result_vector);
		switch (last_key.GetVectorType()) {
		case VectorType::CONSTANT_VECTOR:
			if (ConstantVector::IsNull(last_key)) {
				mask.SetAllInvalid(input.size());
			}
			break;
		case VectorType::FLAT_VECTOR:
			mask.Copy(FlatVector::Validity(last_key), input.size());
			break;
		default: {
			VectorData kdata;
			last_key.Orrify(keys.size(), kdata);
			for (idx_t i = 0; i < input.size(); i++) {
				auto kidx = kdata.sel->get_index(i);
				mask.Set(i, kdata.validity.RowIsValid(kidx));
			}
			break;
		}
		}

		auto count_star = FlatVector::GetData<int64_t>(info.result_chunk.data[0]);
		auto count = FlatVector::GetData<int64_t>(info.result_chunk.data[1]);
		// set the entries to either true or false based on whether a match was found
		for (idx_t i = 0; i < input.size(); i++) {
			D_ASSERT(count_star[i] >= count[i]);
			bool_result[i] = found_match ? found_match[i] : false;
			if (!bool_result[i] && count_star[i] > count[i]) {
				// RHS has NULL value and result is false: set to null
				mask.SetInvalid(i);
			}
			if (count_star[i] == 0) {
				// count == 0, set nullmask to false (we know the result is false now)
				mask.SetValid(i);
			}
		}
	}
	finished = true;
}

void ScanStructure::NextLeftJoin(DataChunk &keys, DataChunk &left, DataChunk &result) {
	// a LEFT OUTER JOIN is identical to an INNER JOIN except all tuples that do
	// not have a match must return at least one tuple (with the right side set
	// to NULL in every column)
	NextInnerJoin(keys, left, result);
	if (result.size() == 0) {
		// no entries left from the normal join
		// fill in the result of the remaining left tuples
		// together with NULL values on the right-hand side
		idx_t remaining_count = 0;
		SelectionVector sel(STANDARD_VECTOR_SIZE);
		for (idx_t i = 0; i < left.size(); i++) {
			if (!found_match[i]) {
				sel.set_index(remaining_count++, i);
			}
		}
		if (remaining_count > 0) {
			// have remaining tuples
			// slice the left side with tuples that did not find a match
			result.Slice(left, sel, remaining_count);

			// now set the right side to NULL
			for (idx_t i = left.ColumnCount(); i < result.ColumnCount(); i++) {
				Vector &vec = result.data[i];
				vec.SetVectorType(VectorType::CONSTANT_VECTOR);
				ConstantVector::SetNull(vec, true);
			}
		}
		finished = true;
	}
}

void ScanStructure::NextSingleJoin(DataChunk &keys, DataChunk &input, DataChunk &result) {
	// single join
	// this join is similar to the semi join except that
	// (1) we actually return data from the RHS and
	// (2) we return NULL for that data if there is no match
	idx_t result_count = 0;
	SelectionVector result_sel(STANDARD_VECTOR_SIZE);
	SelectionVector match_sel(STANDARD_VECTOR_SIZE), no_match_sel(STANDARD_VECTOR_SIZE);
	while (this->count > 0) {
		// resolve the predicates for the current set of pointers
		idx_t match_count = ResolvePredicates(keys, match_sel, &no_match_sel);
		idx_t no_match_count = this->count - match_count;

		// mark each of the matches as found
		for (idx_t i = 0; i < match_count; i++) {
			// found a match for this index
			auto index = match_sel.get_index(i);
			found_match[index] = true;
			result_sel.set_index(result_count++, index);
		}
		// continue searching for the ones where we did not find a match yet
		AdvancePointers(no_match_sel, no_match_count);
	}
	// reference the columns of the left side from the result
	D_ASSERT(input.ColumnCount() > 0);
	for (idx_t i = 0; i < input.ColumnCount(); i++) {
		result.data[i].Reference(input.data[i]);
	}
	// now fetch the data from the RHS
	for (idx_t i = 0; i < ht.build_types.size(); i++) {
		auto &vector = result.data[input.ColumnCount() + i];
		// set NULL entries for every entry that was not found
		auto &mask = FlatVector::Validity(vector);
		mask.SetAllInvalid(input.size());
		for (idx_t j = 0; j < result_count; j++) {
			mask.SetValid(result_sel.get_index(j));
		}
		// for the remaining values we fetch the values
		GatherResult(vector, result_sel, result_sel, result_count, i + ht.condition_types.size());
	}
	result.SetCardinality(input.size());

	// like the SEMI, ANTI and MARK join types, the SINGLE join only ever does one pass over the HT per input chunk
	finished = true;
}

void JoinHashTable::ScanFullOuter(DataChunk &result, JoinHTScanState &state) {
	// scan the HT starting from the current position and check which rows from the build side did not find a match
	Vector addresses(LogicalType::POINTER);
	auto key_locations = FlatVector::GetData<data_ptr_t>(addresses);
	idx_t found_entries = 0;
	{
		lock_guard<mutex> state_lock(state.lock);
		for (; state.block_position < block_collection->blocks.size(); state.block_position++, state.position = 0) {
			auto &block = block_collection->blocks[state.block_position];
			auto &handle = pinned_handles[state.block_position];
			auto baseptr = handle.Ptr();
			for (; state.position < block.count; state.position++) {
				auto tuple_base = baseptr + state.position * entry_size;
				auto found_match = Load<bool>(tuple_base + tuple_size);
				if (!found_match) {
					key_locations[found_entries++] = tuple_base;
					if (found_entries == STANDARD_VECTOR_SIZE) {
						state.position++;
						break;
					}
				}
			}
			if (found_entries == STANDARD_VECTOR_SIZE) {
				break;
			}
		}
	}
	result.SetCardinality(found_entries);
	if (found_entries > 0) {
		idx_t left_column_count = result.ColumnCount() - build_types.size();
		const auto &sel_vector = *FlatVector::IncrementalSelectionVector();
		// set the left side as a constant NULL
		for (idx_t i = 0; i < left_column_count; i++) {
			Vector &vec = result.data[i];
			vec.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(vec, true);
		}
		// gather the values from the RHS
		for (idx_t i = 0; i < build_types.size(); i++) {
			auto &vector = result.data[left_column_count + i];
			D_ASSERT(vector.GetType() == build_types[i]);
			const auto col_no = condition_types.size() + i;
			const auto col_offset = layout.GetOffsets()[col_no];
			RowOperations::Gather(addresses, sel_vector, vector, sel_vector, found_entries, col_offset, col_no);
		}
	}
}

idx_t JoinHashTable::FillWithHTOffsets(data_ptr_t *key_locations, JoinHTScanState &state) {

	// iterate over blocks
	idx_t key_count = 0;
	while (state.block_position < block_collection->blocks.size()) {
		auto &block = block_collection->blocks[state.block_position];
		auto handle = buffer_manager.Pin(block.block);
		auto base_ptr = handle.Ptr();
		// go through all the tuples within this block
		while (state.position < block.count) {
			auto tuple_base = base_ptr + state.position * entry_size;
			// store its locations
			key_locations[key_count++] = tuple_base;
			state.position++;
		}
		state.block_position++;
		state.position = 0;
	}
	return key_count;
}
} // namespace duckdb



namespace duckdb {

template <class OP>
struct ComparisonOperationWrapper {
	template <class T>
	static inline bool Operation(T left, T right, bool left_is_null, bool right_is_null) {
		if (left_is_null || right_is_null) {
			return false;
		}
		return OP::Operation(left, right);
	}
};

struct InitialNestedLoopJoin {
	template <class T, class OP>
	static idx_t Operation(Vector &left, Vector &right, idx_t left_size, idx_t right_size, idx_t &lpos, idx_t &rpos,
	                       SelectionVector &lvector, SelectionVector &rvector, idx_t current_match_count) {
		// initialize phase of nested loop join
		// fill lvector and rvector with matches from the base vectors
		VectorData left_data, right_data;
		left.Orrify(left_size, left_data);
		right.Orrify(right_size, right_data);

		auto ldata = (T *)left_data.data;
		auto rdata = (T *)right_data.data;
		idx_t result_count = 0;
		for (; rpos < right_size; rpos++) {
			idx_t right_position = right_data.sel->get_index(rpos);
			bool right_is_valid = right_data.validity.RowIsValid(right_position);
			for (; lpos < left_size; lpos++) {
				if (result_count == STANDARD_VECTOR_SIZE) {
					// out of space!
					return result_count;
				}
				idx_t left_position = left_data.sel->get_index(lpos);
				bool left_is_valid = left_data.validity.RowIsValid(left_position);
				if (OP::Operation(ldata[left_position], rdata[right_position], !left_is_valid, !right_is_valid)) {
					// emit tuple
					lvector.set_index(result_count, lpos);
					rvector.set_index(result_count, rpos);
					result_count++;
				}
			}
			lpos = 0;
		}
		return result_count;
	}
};

struct RefineNestedLoopJoin {
	template <class T, class OP>
	static idx_t Operation(Vector &left, Vector &right, idx_t left_size, idx_t right_size, idx_t &lpos, idx_t &rpos,
	                       SelectionVector &lvector, SelectionVector &rvector, idx_t current_match_count) {
		VectorData left_data, right_data;
		left.Orrify(left_size, left_data);
		right.Orrify(right_size, right_data);

		// refine phase of the nested loop join
		// refine lvector and rvector based on matches of subsequent conditions (in case there are multiple conditions
		// in the join)
		D_ASSERT(current_match_count > 0);
		auto ldata = (T *)left_data.data;
		auto rdata = (T *)right_data.data;
		idx_t result_count = 0;
		for (idx_t i = 0; i < current_match_count; i++) {
			auto lidx = lvector.get_index(i);
			auto ridx = rvector.get_index(i);
			auto left_idx = left_data.sel->get_index(lidx);
			auto right_idx = right_data.sel->get_index(ridx);
			bool left_is_valid = left_data.validity.RowIsValid(left_idx);
			bool right_is_valid = right_data.validity.RowIsValid(right_idx);
			if (OP::Operation(ldata[left_idx], rdata[right_idx], !left_is_valid, !right_is_valid)) {
				lvector.set_index(result_count, lidx);
				rvector.set_index(result_count, ridx);
				result_count++;
			}
		}
		return result_count;
	}
};

template <class NLTYPE, class OP>
static idx_t NestedLoopJoinTypeSwitch(Vector &left, Vector &right, idx_t left_size, idx_t right_size, idx_t &lpos,
                                      idx_t &rpos, SelectionVector &lvector, SelectionVector &rvector,
                                      idx_t current_match_count) {
	switch (left.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return NLTYPE::template Operation<int8_t, OP>(left, right, left_size, right_size, lpos, rpos, lvector, rvector,
		                                              current_match_count);
	case PhysicalType::INT16:
		return NLTYPE::template Operation<int16_t, OP>(left, right, left_size, right_size, lpos, rpos, lvector, rvector,
		                                               current_match_count);
	case PhysicalType::INT32:
		return NLTYPE::template Operation<int32_t, OP>(left, right, left_size, right_size, lpos, rpos, lvector, rvector,
		                                               current_match_count);
	case PhysicalType::INT64:
		return NLTYPE::template Operation<int64_t, OP>(left, right, left_size, right_size, lpos, rpos, lvector, rvector,
		                                               current_match_count);
	case PhysicalType::UINT8:
		return NLTYPE::template Operation<uint8_t, OP>(left, right, left_size, right_size, lpos, rpos, lvector, rvector,
		                                               current_match_count);
	case PhysicalType::UINT16:
		return NLTYPE::template Operation<uint16_t, OP>(left, right, left_size, right_size, lpos, rpos, lvector,
		                                                rvector, current_match_count);
	case PhysicalType::UINT32:
		return NLTYPE::template Operation<uint32_t, OP>(left, right, left_size, right_size, lpos, rpos, lvector,
		                                                rvector, current_match_count);
	case PhysicalType::UINT64:
		return NLTYPE::template Operation<uint64_t, OP>(left, right, left_size, right_size, lpos, rpos, lvector,
		                                                rvector, current_match_count);
	case PhysicalType::INT128:
		return NLTYPE::template Operation<hugeint_t, OP>(left, right, left_size, right_size, lpos, rpos, lvector,
		                                                 rvector, current_match_count);
	case PhysicalType::FLOAT:
		return NLTYPE::template Operation<float, OP>(left, right, left_size, right_size, lpos, rpos, lvector, rvector,
		                                             current_match_count);
	case PhysicalType::DOUBLE:
		return NLTYPE::template Operation<double, OP>(left, right, left_size, right_size, lpos, rpos, lvector, rvector,
		                                              current_match_count);
	case PhysicalType::INTERVAL:
		return NLTYPE::template Operation<interval_t, OP>(left, right, left_size, right_size, lpos, rpos, lvector,
		                                                  rvector, current_match_count);
	case PhysicalType::VARCHAR:
		return NLTYPE::template Operation<string_t, OP>(left, right, left_size, right_size, lpos, rpos, lvector,
		                                                rvector, current_match_count);
	default:
		throw InternalException("Unimplemented type for join!");
	}
}

template <class NLTYPE>
idx_t NestedLoopJoinComparisonSwitch(Vector &left, Vector &right, idx_t left_size, idx_t right_size, idx_t &lpos,
                                     idx_t &rpos, SelectionVector &lvector, SelectionVector &rvector,
                                     idx_t current_match_count, ExpressionType comparison_type) {
	D_ASSERT(left.GetType() == right.GetType());
	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		return NestedLoopJoinTypeSwitch<NLTYPE, ComparisonOperationWrapper<duckdb::Equals>>(
		    left, right, left_size, right_size, lpos, rpos, lvector, rvector, current_match_count);
	case ExpressionType::COMPARE_NOTEQUAL:
		return NestedLoopJoinTypeSwitch<NLTYPE, ComparisonOperationWrapper<duckdb::NotEquals>>(
		    left, right, left_size, right_size, lpos, rpos, lvector, rvector, current_match_count);
	case ExpressionType::COMPARE_LESSTHAN:
		return NestedLoopJoinTypeSwitch<NLTYPE, ComparisonOperationWrapper<duckdb::LessThan>>(
		    left, right, left_size, right_size, lpos, rpos, lvector, rvector, current_match_count);
	case ExpressionType::COMPARE_GREATERTHAN:
		return NestedLoopJoinTypeSwitch<NLTYPE, ComparisonOperationWrapper<duckdb::GreaterThan>>(
		    left, right, left_size, right_size, lpos, rpos, lvector, rvector, current_match_count);
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return NestedLoopJoinTypeSwitch<NLTYPE, ComparisonOperationWrapper<duckdb::LessThanEquals>>(
		    left, right, left_size, right_size, lpos, rpos, lvector, rvector, current_match_count);
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return NestedLoopJoinTypeSwitch<NLTYPE, ComparisonOperationWrapper<duckdb::GreaterThanEquals>>(
		    left, right, left_size, right_size, lpos, rpos, lvector, rvector, current_match_count);
	case ExpressionType::COMPARE_DISTINCT_FROM:
		return NestedLoopJoinTypeSwitch<NLTYPE, duckdb::DistinctFrom>(left, right, left_size, right_size, lpos, rpos,
		                                                              lvector, rvector, current_match_count);
	default:
		throw NotImplementedException("Unimplemented comparison type for join!");
	}
}

idx_t NestedLoopJoinInner::Perform(idx_t &lpos, idx_t &rpos, DataChunk &left_conditions, DataChunk &right_conditions,
                                   SelectionVector &lvector, SelectionVector &rvector,
                                   const vector<JoinCondition> &conditions) {
	D_ASSERT(left_conditions.ColumnCount() == right_conditions.ColumnCount());
	if (lpos >= left_conditions.size() || rpos >= right_conditions.size()) {
		return 0;
	}
	// for the first condition, lvector and rvector are not set yet
	// we initialize them using the InitialNestedLoopJoin
	idx_t match_count = NestedLoopJoinComparisonSwitch<InitialNestedLoopJoin>(
	    left_conditions.data[0], right_conditions.data[0], left_conditions.size(), right_conditions.size(), lpos, rpos,
	    lvector, rvector, 0, conditions[0].comparison);
	// now resolve the rest of the conditions
	for (idx_t i = 1; i < conditions.size(); i++) {
		// check if we have run out of tuples to compare
		if (match_count == 0) {
			return 0;
		}
		// if not, get the vectors to compare
		Vector &l = left_conditions.data[i];
		Vector &r = right_conditions.data[i];
		// then we refine the currently obtained results using the RefineNestedLoopJoin
		match_count = NestedLoopJoinComparisonSwitch<RefineNestedLoopJoin>(
		    l, r, left_conditions.size(), right_conditions.size(), lpos, rpos, lvector, rvector, match_count,
		    conditions[i].comparison);
	}
	return match_count;
}

} // namespace duckdb




namespace duckdb {

template <class T, class OP>
static void TemplatedMarkJoin(Vector &left, Vector &right, idx_t lcount, idx_t rcount, bool found_match[]) {
	VectorData left_data, right_data;
	left.Orrify(lcount, left_data);
	right.Orrify(rcount, right_data);

	auto ldata = (T *)left_data.data;
	auto rdata = (T *)right_data.data;
	for (idx_t i = 0; i < lcount; i++) {
		if (found_match[i]) {
			continue;
		}
		auto lidx = left_data.sel->get_index(i);
		if (!left_data.validity.RowIsValid(lidx)) {
			continue;
		}
		for (idx_t j = 0; j < rcount; j++) {
			auto ridx = right_data.sel->get_index(j);
			if (!right_data.validity.RowIsValid(ridx)) {
				continue;
			}
			if (OP::Operation(ldata[lidx], rdata[ridx])) {
				found_match[i] = true;
				break;
			}
		}
	}
}

template <class OP>
static void MarkJoinSwitch(Vector &left, Vector &right, idx_t lcount, idx_t rcount, bool found_match[]) {
	switch (left.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return TemplatedMarkJoin<int8_t, OP>(left, right, lcount, rcount, found_match);
	case PhysicalType::INT16:
		return TemplatedMarkJoin<int16_t, OP>(left, right, lcount, rcount, found_match);
	case PhysicalType::INT32:
		return TemplatedMarkJoin<int32_t, OP>(left, right, lcount, rcount, found_match);
	case PhysicalType::INT64:
		return TemplatedMarkJoin<int64_t, OP>(left, right, lcount, rcount, found_match);
	case PhysicalType::INT128:
		return TemplatedMarkJoin<hugeint_t, OP>(left, right, lcount, rcount, found_match);
	case PhysicalType::UINT8:
		return TemplatedMarkJoin<uint8_t, OP>(left, right, lcount, rcount, found_match);
	case PhysicalType::UINT16:
		return TemplatedMarkJoin<uint16_t, OP>(left, right, lcount, rcount, found_match);
	case PhysicalType::UINT32:
		return TemplatedMarkJoin<uint32_t, OP>(left, right, lcount, rcount, found_match);
	case PhysicalType::UINT64:
		return TemplatedMarkJoin<uint64_t, OP>(left, right, lcount, rcount, found_match);
	case PhysicalType::FLOAT:
		return TemplatedMarkJoin<float, OP>(left, right, lcount, rcount, found_match);
	case PhysicalType::DOUBLE:
		return TemplatedMarkJoin<double, OP>(left, right, lcount, rcount, found_match);
	case PhysicalType::VARCHAR:
		return TemplatedMarkJoin<string_t, OP>(left, right, lcount, rcount, found_match);
	default:
		throw NotImplementedException("Unimplemented type for mark join!");
	}
}

static void MarkJoinComparisonSwitch(Vector &left, Vector &right, idx_t lcount, idx_t rcount, bool found_match[],
                                     ExpressionType comparison_type) {
	D_ASSERT(left.GetType() == right.GetType());
	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		return MarkJoinSwitch<duckdb::Equals>(left, right, lcount, rcount, found_match);
	case ExpressionType::COMPARE_NOTEQUAL:
		return MarkJoinSwitch<duckdb::NotEquals>(left, right, lcount, rcount, found_match);
	case ExpressionType::COMPARE_LESSTHAN:
		return MarkJoinSwitch<duckdb::LessThan>(left, right, lcount, rcount, found_match);
	case ExpressionType::COMPARE_GREATERTHAN:
		return MarkJoinSwitch<duckdb::GreaterThan>(left, right, lcount, rcount, found_match);
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return MarkJoinSwitch<duckdb::LessThanEquals>(left, right, lcount, rcount, found_match);
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return MarkJoinSwitch<duckdb::GreaterThanEquals>(left, right, lcount, rcount, found_match);
	default:
		throw NotImplementedException("Unimplemented comparison type for join!");
	}
}

void NestedLoopJoinMark::Perform(DataChunk &left, ChunkCollection &right, bool found_match[],
                                 const vector<JoinCondition> &conditions) {
	// initialize a new temporary selection vector for the left chunk
	// loop over all chunks in the RHS
	for (idx_t chunk_idx = 0; chunk_idx < right.ChunkCount(); chunk_idx++) {
		DataChunk &right_chunk = right.GetChunk(chunk_idx);
		for (idx_t i = 0; i < conditions.size(); i++) {
			MarkJoinComparisonSwitch(left.data[i], right_chunk.data[i], left.size(), right_chunk.size(), found_match,
			                         conditions[i].comparison);
		}
	}
}

} // namespace duckdb



namespace duckdb {

AggregateObject::AggregateObject(AggregateFunction function, FunctionData *bind_data, idx_t child_count,
                                 idx_t payload_size, bool distinct, PhysicalType return_type, Expression *filter)
    : function(move(function)), bind_data(bind_data), child_count(child_count), payload_size(payload_size),
      distinct(distinct), return_type(return_type), filter(filter) {
}

AggregateObject::AggregateObject(BoundAggregateExpression *aggr)
    : AggregateObject(aggr->function, aggr->bind_info.get(), aggr->children.size(),
                      AlignValue(aggr->function.state_size()), aggr->distinct, aggr->return_type.InternalType(),
                      aggr->filter.get()) {
}

vector<AggregateObject> AggregateObject::CreateAggregateObjects(const vector<BoundAggregateExpression *> &bindings) {
	vector<AggregateObject> aggregates;
	aggregates.reserve(aggregates.size());
	for (auto &binding : bindings) {
		aggregates.emplace_back(binding);
	}
	return aggregates;
}

AggregateFilterData::AggregateFilterData(Allocator &allocator, Expression &filter_expr,
                                         const vector<LogicalType> &payload_types)
    : filter_executor(allocator, &filter_expr), true_sel(STANDARD_VECTOR_SIZE) {
	if (payload_types.empty()) {
		return;
	}
	filtered_payload.Initialize(allocator, payload_types);
}

idx_t AggregateFilterData::ApplyFilter(DataChunk &payload) {
	filtered_payload.Reset();

	auto count = filter_executor.SelectExpression(payload, true_sel);
	filtered_payload.Slice(payload, true_sel, count);
	return count;
}

AggregateFilterDataSet::AggregateFilterDataSet() {
}

void AggregateFilterDataSet::Initialize(Allocator &allocator, const vector<AggregateObject> &aggregates,
                                        const vector<LogicalType> &payload_types) {
	bool has_filters = false;
	for (auto &aggregate : aggregates) {
		if (aggregate.filter) {
			has_filters = true;
			break;
		}
	}
	if (!has_filters) {
		// no filters: nothing to do
		return;
	}
	filter_data.resize(aggregates.size());
	for (idx_t aggr_idx = 0; aggr_idx < aggregates.size(); aggr_idx++) {
		auto &aggr = aggregates[aggr_idx];
		if (aggr.filter) {
			filter_data[aggr_idx] = make_unique<AggregateFilterData>(allocator, *aggr.filter, payload_types);
		}
	}
}

AggregateFilterData &AggregateFilterDataSet::GetFilterData(idx_t aggr_idx) {
	D_ASSERT(aggr_idx < filter_data.size());
	D_ASSERT(filter_data[aggr_idx]);
	return *filter_data[aggr_idx];
}
} // namespace duckdb














namespace duckdb {

PhysicalHashAggregate::PhysicalHashAggregate(ClientContext &context, vector<LogicalType> types,
                                             vector<unique_ptr<Expression>> expressions, idx_t estimated_cardinality,
                                             PhysicalOperatorType type)
    : PhysicalHashAggregate(context, move(types), move(expressions), {}, estimated_cardinality, type) {
}

PhysicalHashAggregate::PhysicalHashAggregate(ClientContext &context, vector<LogicalType> types,
                                             vector<unique_ptr<Expression>> expressions,
                                             vector<unique_ptr<Expression>> groups_p, idx_t estimated_cardinality,
                                             PhysicalOperatorType type)
    : PhysicalHashAggregate(context, move(types), move(expressions), move(groups_p), {}, {}, estimated_cardinality,
                            type) {
}

PhysicalHashAggregate::PhysicalHashAggregate(ClientContext &context, vector<LogicalType> types,
                                             vector<unique_ptr<Expression>> expressions,
                                             vector<unique_ptr<Expression>> groups_p,
                                             vector<GroupingSet> grouping_sets_p,
                                             vector<vector<idx_t>> grouping_functions_p, idx_t estimated_cardinality,
                                             PhysicalOperatorType type)
    : PhysicalOperator(type, move(types), estimated_cardinality), groups(move(groups_p)),
      grouping_sets(move(grouping_sets_p)), grouping_functions(move(grouping_functions_p)), any_distinct(false) {
	// get a list of all aggregates to be computed
	for (auto &expr : groups) {
		group_types.push_back(expr->return_type);
	}
	if (grouping_sets.empty()) {
		GroupingSet set;
		for (idx_t i = 0; i < group_types.size(); i++) {
			set.insert(i);
		}
		grouping_sets.push_back(move(set));
	}
	vector<LogicalType> payload_types_filters;
	for (auto &expr : expressions) {
		D_ASSERT(expr->expression_class == ExpressionClass::BOUND_AGGREGATE);
		D_ASSERT(expr->IsAggregate());
		auto &aggr = (BoundAggregateExpression &)*expr;
		bindings.push_back(&aggr);

		if (aggr.distinct) {
			any_distinct = true;
		}

		aggregate_return_types.push_back(aggr.return_type);
		for (auto &child : aggr.children) {
			payload_types.push_back(child->return_type);
		}
		if (aggr.filter) {
			payload_types_filters.push_back(aggr.filter->return_type);
		}
		if (!aggr.function.combine) {
			throw InternalException("Aggregate function %s is missing a combine method", aggr.function.name);
		}
		aggregates.push_back(move(expr));
	}

	for (const auto &pay_filters : payload_types_filters) {
		payload_types.push_back(pay_filters);
	}

	// filter_indexes must be pre-built, not lazily instantiated in parallel...
	idx_t aggregate_input_idx = 0;
	for (auto &aggregate : aggregates) {
		auto &aggr = (BoundAggregateExpression &)*aggregate;
		aggregate_input_idx += aggr.children.size();
	}
	for (auto &aggregate : aggregates) {
		auto &aggr = (BoundAggregateExpression &)*aggregate;
		if (aggr.filter) {
			auto &bound_ref_expr = (BoundReferenceExpression &)*aggr.filter;
			auto it = filter_indexes.find(aggr.filter.get());
			if (it == filter_indexes.end()) {
				filter_indexes[aggr.filter.get()] = bound_ref_expr.index;
				bound_ref_expr.index = aggregate_input_idx++;
			} else {
				++aggregate_input_idx;
			}
		}
	}

	for (auto &grouping_set : grouping_sets) {
		radix_tables.emplace_back(grouping_set, *this);
	}
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class HashAggregateGlobalState : public GlobalSinkState {
public:
	HashAggregateGlobalState(const PhysicalHashAggregate &op, ClientContext &context) {
		radix_states.reserve(op.radix_tables.size());
		for (auto &rt : op.radix_tables) {
			radix_states.push_back(rt.GetGlobalSinkState(context));
		}
	}

	vector<unique_ptr<GlobalSinkState>> radix_states;
};

class HashAggregateLocalState : public LocalSinkState {
public:
	HashAggregateLocalState(const PhysicalHashAggregate &op, ExecutionContext &context) {
		if (!op.payload_types.empty()) {
			aggregate_input_chunk.InitializeEmpty(op.payload_types);
		}

		radix_states.reserve(op.radix_tables.size());
		for (auto &rt : op.radix_tables) {
			radix_states.push_back(rt.GetLocalSinkState(context));
		}
	}

	DataChunk aggregate_input_chunk;

	vector<unique_ptr<LocalSinkState>> radix_states;
};

void PhysicalHashAggregate::SetMultiScan(GlobalSinkState &state) {
	auto &gstate = (HashAggregateGlobalState &)state;
	for (auto &radix_state : gstate.radix_states) {
		RadixPartitionedHashTable::SetMultiScan(*radix_state);
	}
}

unique_ptr<GlobalSinkState> PhysicalHashAggregate::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<HashAggregateGlobalState>(*this, context);
}

unique_ptr<LocalSinkState> PhysicalHashAggregate::GetLocalSinkState(ExecutionContext &context) const {
	return make_unique<HashAggregateLocalState>(*this, context);
}

SinkResultType PhysicalHashAggregate::Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
                                           DataChunk &input) const {
	auto &llstate = (HashAggregateLocalState &)lstate;
	auto &gstate = (HashAggregateGlobalState &)state;

	DataChunk &aggregate_input_chunk = llstate.aggregate_input_chunk;

	idx_t aggregate_input_idx = 0;
	for (auto &aggregate : aggregates) {
		auto &aggr = (BoundAggregateExpression &)*aggregate;
		for (auto &child_expr : aggr.children) {
			D_ASSERT(child_expr->type == ExpressionType::BOUND_REF);
			auto &bound_ref_expr = (BoundReferenceExpression &)*child_expr;
			D_ASSERT(bound_ref_expr.index < input.data.size());
			aggregate_input_chunk.data[aggregate_input_idx++].Reference(input.data[bound_ref_expr.index]);
		}
	}
	for (auto &aggregate : aggregates) {
		auto &aggr = (BoundAggregateExpression &)*aggregate;
		if (aggr.filter) {
			auto it = filter_indexes.find(aggr.filter.get());
			D_ASSERT(it != filter_indexes.end());
			D_ASSERT(it->second < input.data.size());
			aggregate_input_chunk.data[aggregate_input_idx++].Reference(input.data[it->second]);
		}
	}

	aggregate_input_chunk.SetCardinality(input.size());
	aggregate_input_chunk.Verify();

	for (idx_t i = 0; i < radix_tables.size(); i++) {
		radix_tables[i].Sink(context, *gstate.radix_states[i], *llstate.radix_states[i], input, aggregate_input_chunk);
	}

	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalHashAggregate::Combine(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate) const {
	auto &gstate = (HashAggregateGlobalState &)state;
	auto &llstate = (HashAggregateLocalState &)lstate;

	for (idx_t i = 0; i < radix_tables.size(); i++) {
		radix_tables[i].Combine(context, *gstate.radix_states[i], *llstate.radix_states[i]);
	}
}

class HashAggregateFinalizeEvent : public Event {
public:
	HashAggregateFinalizeEvent(const PhysicalHashAggregate &op_p, HashAggregateGlobalState &gstate_p,
	                           Pipeline *pipeline_p)
	    : Event(pipeline_p->executor), op(op_p), gstate(gstate_p), pipeline(pipeline_p) {
	}

	const PhysicalHashAggregate &op;
	HashAggregateGlobalState &gstate;
	Pipeline *pipeline;

public:
	void Schedule() override {
		vector<unique_ptr<Task>> tasks;
		for (idx_t i = 0; i < op.radix_tables.size(); i++) {
			op.radix_tables[i].ScheduleTasks(pipeline->executor, shared_from_this(), *gstate.radix_states[i], tasks);
		}
		D_ASSERT(!tasks.empty());
		SetTasks(move(tasks));
	}
};

SinkFinalizeType PhysicalHashAggregate::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                 GlobalSinkState &gstate_p) const {
	auto &gstate = (HashAggregateGlobalState &)gstate_p;
	bool any_partitioned = false;
	for (idx_t i = 0; i < gstate.radix_states.size(); i++) {
		bool is_partitioned = radix_tables[i].Finalize(context, *gstate.radix_states[i]);
		if (is_partitioned) {
			any_partitioned = true;
		}
	}
	if (any_partitioned) {
		auto new_event = make_shared<HashAggregateFinalizeEvent>(*this, gstate, &pipeline);
		event.InsertEvent(move(new_event));
	}
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class PhysicalHashAggregateState : public GlobalSourceState {
public:
	explicit PhysicalHashAggregateState(ClientContext &context, const PhysicalHashAggregate &op) : scan_index(0) {
		for (auto &rt : op.radix_tables) {
			radix_states.push_back(rt.GetGlobalSourceState(context));
		}
	}

	idx_t scan_index;

	vector<unique_ptr<GlobalSourceState>> radix_states;
};

unique_ptr<GlobalSourceState> PhysicalHashAggregate::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<PhysicalHashAggregateState>(context, *this);
}

void PhysicalHashAggregate::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p,
                                    LocalSourceState &lstate) const {
	auto &gstate = (HashAggregateGlobalState &)*sink_state;
	auto &state = (PhysicalHashAggregateState &)gstate_p;
	while (state.scan_index < state.radix_states.size()) {
		radix_tables[state.scan_index].GetData(context, chunk, *gstate.radix_states[state.scan_index],
		                                       *state.radix_states[state.scan_index]);
		if (chunk.size() != 0) {
			return;
		}

		state.scan_index++;
	}
}

string PhysicalHashAggregate::ParamsToString() const {
	string result;
	for (idx_t i = 0; i < groups.size(); i++) {
		if (i > 0) {
			result += "\n";
		}
		result += groups[i]->GetName();
	}
	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto &aggregate = (BoundAggregateExpression &)*aggregates[i];
		if (i > 0 || !groups.empty()) {
			result += "\n";
		}
		result += aggregates[i]->GetName();
		if (aggregate.filter) {
			result += " Filter: " + aggregate.filter->GetName();
		}
	}
	return result;
}

} // namespace duckdb
