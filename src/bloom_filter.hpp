#ifndef KV_ENGINE_BLOOM_FILTER_HPP
#define KV_ENGINE_BLOOM_FILTER_HPP

#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>
#include <vector>

namespace kv {

class BloomFilter {
public:
  explicit BloomFilter(std::size_t expected_entries,
                       double false_positive_rate);
  void add(std::string_view key);
  // Probabilistic set membership test. `might_contain(key)` returns false if
  // key was definitely never added (no false negatives) and true if key was
  // probably added (small false positive rate controlled at construction).
  [[nodiscard]] bool might_contain(std::string_view key) const;

  // Serialise the filter's state to a byte array for on-disk storage.
  [[nodiscard]] std::vector<uint8_t> serialise() const;
  // Reconstruct a Bloom filter from its serialised byte array form.
  [[nodiscard]] static BloomFilter deserialise(std::span<const uint8_t> data);

private:
  BloomFilter(std::size_t num_bits, std::size_t num_hashes,
              std::vector<uint8_t> bits);
  std::size_t num_bits_;
  std::size_t num_hashes_;
  std::vector<uint8_t> bits_;
};

} // namespace kv

#endif // KV_ENGINE_BLOOM_FILTER_HPP
