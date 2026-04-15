#include "bloom_filter.hpp"
#include <cmath>
#include <cstdint>
#include <cstring>
#include <functional>
#include <stdexcept>
#include <string_view>
#include <utility>
#include <vector>

namespace {
std::pair<uint32_t, uint32_t> hash_pair(std::string_view key) {
  // A Bloom filter with k needs k bit indices per key. The naive approach is to
  // run k different hash functions, which is expensive.
  // Instead, use double hashing - get two base hashes, then synthesise k
  // indices as h1 + (i * h2) for i = 0, 1, ..., k - 1. This behaves almost as
  // well as k independent hashes for Bloom filter FPR - one hash call buys k
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
  // Set k bits in the array - the key's 'fingerprint'. might_contain() will
  // check these same bits to test membership later.
  auto [h1, h2] = hash_pair(key);
  for (std::size_t i = 0; i < num_hashes_; ++i) {
    // Synthesise the i-th hash as h1 + i * h2 (double hashing).
    std::size_t combined_hash = h1 + (i * h2);
    std::size_t bit_index = combined_hash % num_bits_;
    std::size_t byte_index = bit_index / 8;
    std::size_t bit_offset = bit_index % 8;
    // OR-assign sets just this bit, leaves the other 7 bits in the byte alone
    // (so other keys that land in the same byte don't get clobbered).
    bits_[byte_index] |= (1 << bit_offset);
  }
}

bool BloomFilter::might_contain(std::string_view key) const {
  // Recompute the key's k-bit fingerprint. Any zero bit means the key was
  // definitely never added (no false negatives). All bits set means the key
  // was probably added (small false positive rate, controlled at construction).
  auto [h1, h2] = hash_pair(key);
  for (std::size_t i = 0; i < num_hashes_; ++i) {
    std::size_t combined_hash = h1 + (i * h2);
    std::size_t bit_index = combined_hash % num_bits_;
    std::size_t byte_index = bit_index / 8;
    std::size_t bit_offset = bit_index % 8;
    if ((bits_[byte_index] & (1 << bit_offset)) == 0) {
      return false;
    }
  }
  return true;
}

std::vector<uint8_t> BloomFilter::serialise() const {
  // Header: num_bits_ (size_t) + num_hashes_ (size_t), then the bit array.
  constexpr std::size_t header_size = sizeof(num_bits_) + sizeof(num_hashes_);
  std::vector<uint8_t> buffer(header_size + bits_.size());
  std::memcpy(buffer.data(), &num_bits_, sizeof(num_bits_));
  std::memcpy(buffer.data() + sizeof(num_bits_), &num_hashes_,
              sizeof(num_hashes_));
  std::memcpy(buffer.data() + header_size, bits_.data(), bits_.size());
  return buffer;
}

BloomFilter::BloomFilter(std::size_t num_bits, std::size_t num_hashes,
                         std::vector<uint8_t> bits)
    : num_bits_(num_bits), num_hashes_(num_hashes), bits_(std::move(bits)) {}

BloomFilter BloomFilter::deserialise(std::span<const uint8_t> data) {
  if (data.size() < 16) {
    throw std::runtime_error(
        "Data is too short to contain Bloom filter header");
  }

  std::size_t num_bits;
  std::size_t num_hashes;
  std::memcpy(&num_bits, data.data() + 0, 8);
  std::memcpy(&num_hashes, data.data() + 8, 8);
  std::vector<uint8_t> bits(data.begin() + 16, data.end());
  return BloomFilter(num_bits, num_hashes, std::move(bits));
}
} // namespace kv
