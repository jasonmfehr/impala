#include "observe/otel-parsers.h"

#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

using namespace std;
using namespace impala;

TEST(OtelParsersTest, EmptyHeaders) {
	vector<pair<string, string>> parsed;

	parse_otel_additional_headers("", nullptr);

	EXPECT_TRUE(parsed.empty());
}

TEST(OtelParsersTest, SingleHeader) {
	vector<pair<string, string>> parsed;

	parse_otel_additional_headers("foo=bar",
			[&parsed](const string& key, const string& value) {
				parsed.emplace_back(key, value);
			});

	EXPECT_EQ(1, parsed.size());
	EXPECT_EQ("foo", parsed[0].first);
	EXPECT_EQ("bar", parsed[0].second);
}

TEST(OtelParsersTest, SingleHeaderMissingValue) {
	vector<pair<string, string>> parsed;

	parse_otel_additional_headers("foo=",
			[&parsed](const string& key, const string& value) {
				parsed.emplace_back(key, value);
			});

	EXPECT_EQ(1, parsed.size());
	EXPECT_EQ("foo", parsed[0].first);
	EXPECT_EQ("", parsed[0].second);
}

TEST(OtelParsersTest, SingleHeaderMissingKey) {
	vector<pair<string, string>> parsed;

	parse_otel_additional_headers("=bar",
			[&parsed](const string& key, const string& value) {
				parsed.emplace_back(key, value);
			});

	EXPECT_EQ(1, parsed.size());
	EXPECT_EQ("", parsed[0].first);
	EXPECT_EQ("bar", parsed[0].second);
}

TEST(OtelParsersTest, MultipleHeaders) {
	vector<pair<string, string>> parsed;

	parse_otel_additional_headers("foo=bar1:::foo2=bar3:::foo=bar2:::foo3=bar4",
			[&parsed](const string& key, const string& value) {
				parsed.emplace_back(key, value);
			});

	EXPECT_EQ(4, parsed.size());
	EXPECT_EQ("foo", parsed[0].first);
	EXPECT_EQ("bar1", parsed[0].second);
	EXPECT_EQ("foo2", parsed[1].first);
	EXPECT_EQ("bar3", parsed[1].second);
	EXPECT_EQ("foo", parsed[2].first);
	EXPECT_EQ("bar2", parsed[2].second);
	EXPECT_EQ("foo3", parsed[3].first);
	EXPECT_EQ("bar4", parsed[3].second);
}

TEST(OtelParsersTest, TrimsWhitespace) {
	vector<pair<string, string>> parsed;

	parse_otel_additional_headers("  foo  =  bar  :::  baz=qux  ",
			[&parsed](const string& key, const string& value) {
				parsed.emplace_back(key, value);
			});

	EXPECT_EQ(2, parsed.size());
	EXPECT_EQ("foo", parsed[0].first);
	EXPECT_EQ("bar", parsed[0].second);
	EXPECT_EQ("baz", parsed[1].first);
	EXPECT_EQ("qux", parsed[1].second);
}
