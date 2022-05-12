//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  left_plan_ = plan->GetLeftPlan();
  right_plan_ = plan->GetRightPlan();
  left_executor_ = std::move(left_child);
  right_executor_ = std::move(right_child);
}

void HashJoinExecutor::Init() {
  hash_table_.Clear();
  left_executor_->Init();
  right_executor_->Init();
  end_ = false;
  RID rid;
  Tuple tuple;
  index_ = -1;
  while (left_executor_->Next(&tuple, &rid)) {
    auto key = plan_->LeftJoinKeyExpression()->Evaluate(&tuple, left_plan_->OutputSchema());
    hash_table_.Insert(key, tuple);
  }
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (end_) {
    return false;
  }

  if (index_ < 0) {
    Tuple right_tuple;
    RID right_rid;
    while (right_executor_->Next(&right_tuple, &right_rid)) {
      auto right_key = plan_->RightJoinKeyExpression()->Evaluate(&right_tuple, right_plan_->OutputSchema());
      auto left_tuples = hash_table_.Get(right_key);
      if (!left_tuples.empty()) {
        index_ = static_cast<int>(left_tuples.size()) - 1;
        cur_right_tuple_ = right_tuple;
        break;
      }
    }
  }

  if (index_ >= 0) {
    auto left_tuple = hash_table_.Get(
        plan_->RightJoinKeyExpression()->Evaluate(&cur_right_tuple_, right_plan_->OutputSchema()))[index_];
    index_--;
    std::vector<Value> values;
    for (const auto &column : plan_->OutputSchema()->GetColumns()) {
      auto column_expr = reinterpret_cast<const ColumnValueExpression *>(column.GetExpr());
      if (column_expr->GetTupleIdx() == 0) {
        values.push_back(left_tuple.GetValue(plan_->GetLeftPlan()->OutputSchema(), column_expr->GetColIdx()));
      } else {
        values.push_back(cur_right_tuple_.GetValue(plan_->GetRightPlan()->OutputSchema(), column_expr->GetColIdx()));
      }
    }
    *tuple = Tuple(values, plan_->OutputSchema());

    return true;
  }

  end_ = true;
  return false;
}

}  // namespace bustub
