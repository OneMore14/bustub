//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

struct HashJoinKey {
  Value key_;

  bool operator==(const HashJoinKey &other) const { return key_.CompareEquals(other.key_) == CmpBool::CmpTrue; }
};

struct HashJoinValue {
  std::vector<Tuple> tuples_;
};
}  // namespace bustub

namespace std {

template <>
struct hash<bustub::HashJoinKey> {
  std::size_t operator()(const bustub::HashJoinKey &key) const { return bustub::HashUtil::HashValue(&key.key_); }
};
}  // namespace std

namespace bustub {

class SimpleJoinHashTable {
 public:
  void Insert(const Value &key, const Tuple &value) {
    HashJoinKey join_key{key};
    if (ht_.count(join_key) == 0) {
      ht_[join_key] = HashJoinValue{};
    }
    ht_[join_key].tuples_.push_back(value);
  }

  std::vector<Tuple> &Get(const Value &key) {
    HashJoinKey join_key{key};
    return ht_[join_key].tuples_;
  }

  void Clear() { ht_.clear(); }

 private:
  /** The hash table is just a map from aggregate keys to aggregate values */
  std::unordered_map<HashJoinKey, HashJoinValue> ht_{};
};

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join
   * @param[out] rid The next tuple RID produced by the join
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the join */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  const AbstractPlanNode *left_plan_;
  const AbstractPlanNode *right_plan_;
  std::unique_ptr<AbstractExecutor> left_executor_;
  std::unique_ptr<AbstractExecutor> right_executor_;
  SimpleJoinHashTable hash_table_;
  Tuple cur_right_tuple_;
  int index_ = -1;
  bool end_{false};
};

}  // namespace bustub
