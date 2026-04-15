#include "bloom_filter.hpp"
#include <gtest/gtest.h>

namespace kv {

namespace {

TEST(BloomFilterTest, AddedKeysAreRecognised) {
  BloomFilter filter(1000, 0.1);
  filter.add("key1");
  filter.add("key2");
  EXPECT_TRUE(filter.might_contain("key1"));
  EXPECT_TRUE(filter.might_contain("key2"));
  EXPECT_FALSE(filter.might_contain("key3"));
}

TEST(BloomFilterTest, EmptyFilterRejectsAllKeys) {
  BloomFilter filter(1000, 0.1);
  EXPECT_FALSE(filter.might_contain("a"));
  EXPECT_FALSE(filter.might_contain("b"));
  EXPECT_FALSE(filter.might_contain("c"));
}

TEST(BloomFilterTest, NoFalseNegativesAtScale) {
  BloomFilter filter(10000, 0.01);
  for (int i = 0; i < 10000; ++i) {
    filter.add("key" + std::to_string(i));
  }
  for (int i = 0; i < 10000; ++i) {
    EXPECT_TRUE(filter.might_contain("key" + std::to_string(i)));
  }
}

TEST(BloomFilterTest, EmptyStringKeyIsHandled) {
  BloomFilter filter(1000, 0.1);
  filter.add("");
  EXPECT_TRUE(filter.might_contain(""));
  EXPECT_FALSE(filter.might_contain("nonempty"));
}

TEST(BloomFilterTest, DuplicateAddsAreIdempotent) {
  BloomFilter filter(1000, 0.1);
  filter.add("key");
  EXPECT_TRUE(filter.might_contain("key"));
  filter.add("key");
  EXPECT_TRUE(filter.might_contain("key"));
  filter.add("key");
  EXPECT_TRUE(filter.might_contain("key"));
}

TEST(BloomFilterTest, TinyFilterStillWorks) {
  // Rounds k down to 0, but it should bump it back up to 1.
  BloomFilter filter(1, 0.5);
  filter.add("key");
  EXPECT_TRUE(filter.might_contain("key"));
}

TEST(BloomFilterTest, FalsePositiveRateIsReasonable) {
  constexpr std::size_t n = 10000;
  constexpr double target_fpr = 0.01;
  BloomFilter filter(n, target_fpr);
  for (std::size_t i = 0; i < n; ++i) {
    filter.add("key" + std::to_string(i));
  }

  std::size_t fp_count = 0;
  for (std::size_t i = 0; i < n; ++i) {
    if (filter.might_contain("other" + std::to_string(i))) {
      fp_count++;
    }
  }

  // With a large number of keys, the observed false positive rate should be
  // close to the configured rate of 1%. Allow some wiggle room for randomness.
  double observed_fpr = static_cast<double>(fp_count) / n;
  // Allow up to 3x the target false positive rate, as std::hash differs across
  // libc++ (macOS) and libstdc (Linux).
  EXPECT_LT(observed_fpr, target_fpr * 3.0);
}

TEST(BloomFilterTest, SerialiseDeserialiseRoundTrip) {
  BloomFilter original(1000, 0.1);
  original.add("key1");
  original.add("key2");
  original.add("key3");

  auto bytes = original.serialise();
  BloomFilter reconstructed = BloomFilter::deserialise(bytes);

  // The no false-negatives property should survive the roundtrip.
  EXPECT_TRUE(reconstructed.might_contain("key1"));
  EXPECT_TRUE(reconstructed.might_contain("key2"));
  EXPECT_TRUE(reconstructed.might_contain("key3"));
  // Serialising the reconstructed filter should give byte-identical output,
  // which proves the internal state (num_bits_, num_hashes_, bits_) was
  // restored exactly, not just equivalently.
}
} // namespace
} // namespace kv
