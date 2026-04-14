#include "bloom_filter.hpp"
#include <cmath>
#include <cstdint>
#include <functional>
#include <string_view>
#include <utility>

namespace {
std::pair<uint32_t, uint32_t> hash_pair(std::string_view key) {
  // A bloom filter with k needs k bit indices per key. The naive approach is to
  // run k different hash functions, which is expensive.
  // Instead, use double hashing - get two base hashes, then synthesise k
  // indices as h1 + (i * h2) for i = 0, 1, ..., k - 1. This behaves almost as
  // well as k independent hashes for bloom filter FPR - one hash call buys k
  // indices.
  uint64_t h = std::hash<std::string_view>{}(key);
  uint32_t h1 = static_cast<uint32_t>(h);
  uint32_t h2 = static_cast<uint32_t>(h >> 32);
  return {h1, h2};
}
} // namespace

namespace kv {

BloomFilter::BloomFilter(std::size_t expected_entries,
                         double false_positive_rate) {
  double ln2 = std::log(2.0);
  double n = static_cast<double>(expected_entries);
  double p = false_positive_rate;

  num_bits_ =
      static_cast<std::size_t>(std::ceil(-n * std::log(p) / (ln2 * ln2)));
  // Very small filters can round k down to 0, which makes every add() a no-op
  // and every might_contain() return true.
  num_hashes_ = static_cast<std::size_t>(std::ceil((num_bits_ / n) * ln2));
  if (num_hashes_ < 1) {
    num_hashes_ = 1;
  }
  // Bump any remainder up to the next whole byte.
  std::size_t byte_count = (num_bits_ + 7) / 8;
  bits_.resize(byte_count, 0);
}

void BloomFilter::add(std::string_view key) {
  // Implementation for adding a key to the Bloom filter
}

bool BloomFilter::might_contain(std::string_view key) const {
  // Implementation for checking if a key might be in the Bloom filter
}

} // namespace kv
