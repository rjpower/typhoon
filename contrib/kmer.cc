#include "typhoon/mapreduce.h"
#include "typhoon/worker.h"
#include "third_party/rollinghashcpp/rabinkarphash.h"
#include "third_party/rollinghashcpp/cyclichash.h"
#include "third_party/rollinghashcpp/generalhash.h"

static const int kReadLen = 4;
static const int kMinCount = 16;

// Thank you C++, for your wonderfully incomplete string functions.
std::string strip(const std::string& in, char strip) {
  std::string out;
  out.reserve(in.size());
  for (char c: in) {
    if (c != strip) {
      out.push_back(c);
    }
  }
  return out;
}

// Count k-MERs from a FASTA genome representation.
//
// To improve performance, this is done in 2 stages: first counts are
// computed using a rolling hash.  Hashes that occur with sufficient
// frequency are then output.
class KMERMapper: public Mapper {
public:
  void mapShard(Source* input) {
    std::unordered_map<int64_t, int64_t> hashes;
    ColGroup rows;
    auto it = std::shared_ptr<Iterator>(input->iterator());
    while (it->next(&rows)) {
      KarpRabinHash<uint64_t> hasher(kReadLen, 63);
      for (auto block: *rows.data[1].data.str) {
        block = strip(block, '\n');
        for (int i = 0; i < kReadLen; ++i) {
          hasher.eat(block[i]);
        }

        for (int i = kReadLen; i < block.size(); ++i) {
          hasher.update(block[i - kReadLen], block[i]);
          hashes[hasher.hashvalue] += 1;
        }
      }
    }

    it.reset(input->iterator());
    while (it->next(&rows)) {
      KarpRabinHash<uint64_t> hasher(kReadLen, 63);
      for (auto block: *rows.data[1].data.str) {
        block = strip(block, '\n');
        gpr_log(GPR_INFO, "Block: %d %s", block.size(), block.substr(0, 1024).c_str());
        for (int i = 0; i < kReadLen; ++i) {
          hasher.eat(block[i]);
        }

        for (int i = kReadLen; i < block.size(); ++i) {
          hasher.update(block[i - kReadLen], block[i]);
          auto res = hashes.find(hasher.hashvalue);
          if (res != hashes.end() && res->second >= kMinCount) {
            this->put<std::string, uint64_t>(
                block.substr(i - kReadLen, kReadLen), 1);
          }
        }
      }
    }
  }
};
REGISTER_MAPPER(KMERMapper);
