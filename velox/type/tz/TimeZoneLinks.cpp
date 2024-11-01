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

// This file is generated. Do not modify it manually. To re-generate it, run:
//
//  ./velox/type/tz/gen_timezone_links.py -f /tmp/backward \
//       > velox/type/tz/TimeZoneLinks.cpp
//
// The backward file should be the same one used in JODA,
// The latest is available here :
// https://github.com/JodaOrg/global-tz/blob/global-tz/backward.
// Presto Java currently uses the one found here:
// https://github.com/JodaOrg/global-tz/blob/2024agtz/backward
// To find the current version in Presto Java, check this file at the version of
// JODA Presto Java is using:
// https://github.com/JodaOrg/joda-time/blob/v2.12.7/src/changes/changes.xml
// @generated

#include <string>
#include <unordered_map>

namespace facebook::velox::tz {

const std::unordered_map<std::string, std::string>& getTimeZoneLinks() {
  static auto* tzLinks = new std::unordered_map<std::string, std::string>([] {
    // Work around clang compiler bug causing multi-hour compilation
    // with -fsanitize=fuzzer
    // https://github.com/llvm/llvm-project/issues/75666
    return std::unordered_map<std::string, std::string>{
        {"Australia/ACT", "Australia/Sydney"},
        {"Australia/LHI", "Australia/Lord_Howe"},
        {"Australia/NSW", "Australia/Sydney"},
        {"Australia/North", "Australia/Darwin"},
        {"Australia/Queensland", "Australia/Brisbane"},
        {"Australia/South", "Australia/Adelaide"},
        {"Australia/Tasmania", "Australia/Hobart"},
        {"Australia/Victoria", "Australia/Melbourne"},
        {"Australia/West", "Australia/Perth"},
        {"Australia/Yancowinna", "Australia/Broken_Hill"},
        {"Brazil/Acre", "America/Rio_Branco"},
        {"Brazil/DeNoronha", "America/Noronha"},
        {"Brazil/East", "America/Sao_Paulo"},
        {"Brazil/West", "America/Manaus"},
        {"Canada/Atlantic", "America/Halifax"},
        {"Canada/Central", "America/Winnipeg"},
        {"Canada/Eastern", "America/Toronto"},
        {"Canada/Mountain", "America/Edmonton"},
        {"Canada/Newfoundland", "America/St_Johns"},
        {"Canada/Pacific", "America/Vancouver"},
        {"Canada/Saskatchewan", "America/Regina"},
        {"Canada/Yukon", "America/Whitehorse"},
        {"Chile/Continental", "America/Santiago"},
        {"Chile/EasterIsland", "Pacific/Easter"},
        {"Cuba", "America/Havana"},
        {"Egypt", "Africa/Cairo"},
        {"Eire", "Europe/Dublin"},
        {"Etc/GMT+0", "UTC"},
        {"Etc/GMT-0", "UTC"},
        {"Etc/GMT0", "UTC"},
        {"Etc/Greenwich", "UTC"},
        {"Etc/UCT", "UTC"},
        {"Etc/Universal", "UTC"},
        {"Etc/Zulu", "UTC"},
        {"GB", "Europe/London"},
        {"GB-Eire", "Europe/London"},
        {"GMT+0", "UTC"},
        {"GMT-0", "UTC"},
        {"GMT0", "UTC"},
        {"Greenwich", "UTC"},
        {"Hongkong", "Asia/Hong_Kong"},
        {"Iceland", "Atlantic/Reykjavik"},
        {"Iran", "Asia/Tehran"},
        {"Israel", "Asia/Jerusalem"},
        {"Jamaica", "America/Jamaica"},
        {"Japan", "Asia/Tokyo"},
        {"Kwajalein", "Pacific/Kwajalein"},
        {"Libya", "Africa/Tripoli"},
        {"Mexico/BajaNorte", "America/Tijuana"},
        {"Mexico/BajaSur", "America/Mazatlan"},
        {"Mexico/General", "America/Mexico_City"},
        {"NZ", "Pacific/Auckland"},
        {"NZ-CHAT", "Pacific/Chatham"},
        {"Navajo", "America/Denver"},
        {"PRC", "Asia/Shanghai"},
        {"Poland", "Europe/Warsaw"},
        {"Portugal", "Europe/Lisbon"},
        {"ROC", "Asia/Taipei"},
        {"ROK", "Asia/Seoul"},
        {"Singapore", "Asia/Singapore"},
        {"Turkey", "Europe/Istanbul"},
        {"UCT", "UTC"},
        {"US/Alaska", "America/Anchorage"},
        {"US/Aleutian", "America/Adak"},
        {"US/Arizona", "America/Phoenix"},
        {"US/Central", "America/Chicago"},
        {"US/East-Indiana", "America/Indiana/Indianapolis"},
        {"US/Eastern", "America/New_York"},
        {"US/Hawaii", "Pacific/Honolulu"},
        {"US/Indiana-Starke", "America/Indiana/Knox"},
        {"US/Michigan", "America/Detroit"},
        {"US/Mountain", "America/Denver"},
        {"US/Pacific", "America/Los_Angeles"},
        {"US/Samoa", "Pacific/Pago_Pago"},
        {"UTC", "UTC"},
        {"Universal", "UTC"},
        {"W-SU", "Europe/Moscow"},
        {"Zulu", "UTC"},
        {"America/Buenos_Aires", "America/Argentina/Buenos_Aires"},
        {"America/Catamarca", "America/Argentina/Catamarca"},
        {"America/Cordoba", "America/Argentina/Cordoba"},
        {"America/Indianapolis", "America/Indiana/Indianapolis"},
        {"America/Jujuy", "America/Argentina/Jujuy"},
        {"America/Knox_IN", "America/Indiana/Knox"},
        {"America/Louisville", "America/Kentucky/Louisville"},
        {"America/Mendoza", "America/Argentina/Mendoza"},
        {"America/Virgin", "America/St_Thomas"},
        {"Pacific/Samoa", "Pacific/Pago_Pago"},
        {"Africa/Timbuktu", "Africa/Bamako"},
        {"America/Argentina/ComodRivadavia", "America/Argentina/Catamarca"},
        {"America/Atka", "America/Adak"},
        {"America/Coral_Harbour", "America/Atikokan"},
        {"America/Ensenada", "America/Tijuana"},
        {"America/Fort_Wayne", "America/Indiana/Indianapolis"},
        {"America/Montreal", "America/Toronto"},
        {"America/Nipigon", "America/Toronto"},
        {"America/Pangnirtung", "America/Iqaluit"},
        {"America/Porto_Acre", "America/Rio_Branco"},
        {"America/Rainy_River", "America/Winnipeg"},
        {"America/Rosario", "America/Argentina/Cordoba"},
        {"America/Santa_Isabel", "America/Tijuana"},
        {"America/Shiprock", "America/Denver"},
        {"America/Thunder_Bay", "America/Toronto"},
        {"America/Yellowknife", "America/Edmonton"},
        {"Antarctica/South_Pole", "Antarctica/McMurdo"},
        {"Asia/Chongqing", "Asia/Shanghai"},
        {"Asia/Harbin", "Asia/Shanghai"},
        {"Asia/Kashgar", "Asia/Urumqi"},
        {"Asia/Tel_Aviv", "Asia/Jerusalem"},
        {"Atlantic/Jan_Mayen", "Europe/Oslo"},
        {"Australia/Canberra", "Australia/Sydney"},
        {"Australia/Currie", "Australia/Hobart"},
        {"Europe/Belfast", "Europe/London"},
        {"Europe/Tiraspol", "Europe/Chisinau"},
        {"Europe/Uzhgorod", "Europe/Kyiv"},
        {"Europe/Zaporozhye", "Europe/Kyiv"},
        {"Pacific/Enderbury", "Pacific/Kanton"},
        {"Pacific/Johnston", "Pacific/Honolulu"},
        {"Pacific/Yap", "Pacific/Chuuk"},
        {"Africa/Asmera", "Africa/Asmara"},
        {"America/Godthab", "America/Nuuk"},
        {"Asia/Ashkhabad", "Asia/Ashgabat"},
        {"Asia/Calcutta", "Asia/Kolkata"},
        {"Asia/Chungking", "Asia/Shanghai"},
        {"Asia/Dacca", "Asia/Dhaka"},
        {"Asia/Istanbul", "Europe/Istanbul"},
        {"Asia/Katmandu", "Asia/Kathmandu"},
        {"Asia/Macao", "Asia/Macau"},
        {"Asia/Rangoon", "Asia/Yangon"},
        {"Asia/Saigon", "Asia/Ho_Chi_Minh"},
        {"Asia/Thimbu", "Asia/Thimphu"},
        {"Asia/Ujung_Pandang", "Asia/Makassar"},
        {"Asia/Ulan_Bator", "Asia/Ulaanbaatar"},
        {"Atlantic/Faeroe", "Atlantic/Faroe"},
        {"Europe/Kiev", "Europe/Kyiv"},
        {"Europe/Nicosia", "Asia/Nicosia"},
        {"Pacific/Ponape", "Pacific/Pohnpei"},
        {"Pacific/Truk", "Pacific/Chuuk"},
    };
  }());
  return *tzLinks;
}

} // namespace facebook::velox::tz
