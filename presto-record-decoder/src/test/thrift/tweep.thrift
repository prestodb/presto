namespace java com.facebook.presto.decoder.thrift.tweep

enum TweetType {
    TWEET,
    RETWEET = 2,
    DM = 0xa,
    REPLY
}

struct Location {
    1: required double latitude;
    2: required double longitude;
}

struct Tweet {
    1: required i32 userId;
    2: required string userName;
    3: required string text;
    4: optional Location loc;
    5: optional TweetType tweetType = TweetType.TWEET;
    6: optional bool isDeleted = false;
    7: optional byte b;
    8: optional i16 age;
    9: optional i64 fullId;
    10: optional binary pic;
    11: optional map<string,string> attr;
    12: optional list<string> items;
    16: optional string language = "english";
}

typedef list<Tweet> TweetList
typedef set<Tweet> TweetSet

struct TweetSearchResult {
    1: TweetList tweetList;
    2: TweetSet tweetSet;
}

exception TwitterUnavailable {
    1: string message;
}

const i32 MAX_RESULTS = 100;
