#include "server_args.hpp"
#include <gtest/gtest.h>
#include <initializer_list>
#include <string_view>
#include <vector>

namespace kv {

namespace {

auto parse(std::initializer_list<std::string_view> args) {
  const std::vector<std::string_view> arg_vector(args);
  return parse_server_args(arg_vector);
}

TEST(ServerArgsTest, ParsesDataDirAndPort) {
  const auto args = parse({"--data-dir", "/tmp/data", "--port", "8080"});
  ASSERT_TRUE(args.has_value()) << args.error();
  EXPECT_EQ(args->data_dir, "/tmp/data");
  EXPECT_EQ(args->port, 8080);
}

TEST(ServerArgsTest, DefaultsPortWhenOmitted) {
  const auto args = parse({"--data-dir", "/tmp/data"});
  ASSERT_TRUE(args.has_value()) << args.error();
  EXPECT_EQ(args->port, 50051);
}

TEST(ServerArgsTest, AcceptsPortRangeBoundaries) {
  EXPECT_EQ(parse({"--data-dir", "d", "--port", "1"})->port, 1);
  EXPECT_EQ(parse({"--data-dir", "d", "--port", "65535"})->port, 65535);
}

TEST(ServerArgsTest, RejectsMissingDataDir) {
  const auto args = parse({"--port", "8080"});
  ASSERT_FALSE(args.has_value());
  EXPECT_NE(args.error().find("Usage"), std::string::npos);
}

TEST(ServerArgsTest, RejectsEmptyArguments) {
  EXPECT_FALSE(parse({}).has_value());
}

TEST(ServerArgsTest, RejectsInvalidPorts) {
  for (const auto *port :
       {"abc", "80abc", "", "0", "65536", "-1", "+80", " 80", "99999999999"}) {
    const auto args = parse({"--data-dir", "d", "--port", port});
    ASSERT_FALSE(args.has_value()) << "port '" << port << "' was accepted";
    EXPECT_NE(args.error().find("Invalid port"), std::string::npos);
  }
}

TEST(ServerArgsTest, RejectsFlagWithoutValue) {
  const auto args = parse({"--data-dir", "d", "--port"});
  ASSERT_FALSE(args.has_value());
  EXPECT_NE(args.error().find("Missing value for --port"), std::string::npos);
}

TEST(ServerArgsTest, RejectsUnknownArgument) {
  const auto args = parse({"--data-dir", "d", "--verbose"});
  ASSERT_FALSE(args.has_value());
  EXPECT_NE(args.error().find("Unknown argument: --verbose"),
            std::string::npos);
}

} // namespace

} // namespace kv
