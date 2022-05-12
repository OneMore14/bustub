//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.h
//
// Identification: src/include/execution/executors/distinct_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/distinct_plan.h"

namespace bustub {

struct DistinctKey {
  std::vector<Value> columns_;

  bool operator==(const DistinctKey &other) const {
    for (uint32_t i = 0; i < other.columns_.size(); i++) {
      if (columns_[i].CompareEquals(other.columns_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

}  // namespace bustub

namespace std {

template <>
struct hash<bustub::DistinctKey> {
  std::size_t operator()(const bustub::DistinctKey &key) const {
    size_t curr_hash = 0;
    for (const auto &column_key : key.columns_) {
      if (!column_key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&column_key));
      }
    }
    return curr_hash;
  }
};
}  // namespace std

namespace bustub {

class SimpleDistinctSet {
 public:
  void Insert(const DistinctKey &key) { set_.insert(key); }

  void Clear() { set_.clear(); }

  size_t Size() { return set_.size(); }

  class Iterator {
   public:
    explicit Iterator(std::unordered_set<DistinctKey>::const_iterator iter) : iter_{iter} {}

    /** @return The key of the iterator */
    const DistinctKey &Key() { return *iter_; }

    /** @return The iterator before it is incremented */
    Iterator &operator++() {
      ++iter_;
      return *this;
    }

    /** @return `true` if both iterators are identical */
    bool operator==(const Iterator &other) { return this->iter_ == other.iter_; }

    /** @return `true` if both iterators are different */
    bool operator!=(const Iterator &other) { return this->iter_ != other.iter_; }

   private:
    std::unordered_set<DistinctKey>::const_iterator iter_;
  };

  Iterator Begin() { return Iterator{set_.cbegin()}; }

  Iterator End() { return Iterator{set_.cend()}; }

 private:
  std::unordered_set<DistinctKey> set_{};
};

/**
 * DistinctExecutor removes duplicate rows from child ouput.
 */
class DistinctExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new DistinctExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The limit plan to be executed
   * @param child_executor The child executor from which tuples are pulled
   */
  DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the distinct */
  void Init() override;

  /**
   * Yield the next tuple from the distinct.
   * @param[out] tuple The next tuple produced by the distinct
   * @param[out] rid The next tuple RID produced by the distinct
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the distinct */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

 private:
  /** The distinct plan node to be executed */
  const DistinctPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;
  bool end_{false};
  SimpleDistinctSet distinct_set_{};

  SimpleDistinctSet::Iterator iter_;
};
}  // namespace bustub
