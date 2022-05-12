//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "execution/expressions/column_value_expression.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  left_executor_ = std::move(left_executor);
  right_executor_ = std::move(right_executor);
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  end_ = false;
  results_.clear();
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (end_) {
    return false;
  }
  if (!results_.empty()) {
    *tuple = results_.back();
    results_.pop_back();
    return true;
  }
  Tuple left_tuple;
  RID left_rid;
  while (left_executor_->Next(&left_tuple, &left_rid)) {
    Tuple right_tuple;
    RID right_rid;
    right_executor_->Init();
    while (right_executor_->Next(&right_tuple, &right_rid)) {
      if (plan_->Predicate() != nullptr && plan_->Predicate()
                                               ->EvaluateJoin(&left_tuple, plan_->GetLeftPlan()->OutputSchema(),
                                                              &right_tuple, plan_->GetRightPlan()->OutputSchema())
                                               .GetAs<bool>()) {
        std::vector<Value> values;
        for (auto &column : plan_->OutputSchema()->GetColumns()) {
          auto column_expr = reinterpret_cast<const ColumnValueExpression *>(column.GetExpr());
          if (column_expr->GetTupleIdx() == 0) {
            values.push_back(left_tuple.GetValue(plan_->GetLeftPlan()->OutputSchema(), column_expr->GetColIdx()));
          } else {
            values.push_back(right_tuple.GetValue(plan_->GetRightPlan()->OutputSchema(), column_expr->GetColIdx()));
          }
        }

        results_.emplace_back(values, plan_->OutputSchema());
      }
    }
    if (!results_.empty()) {
      *tuple = results_.back();
      results_.pop_back();
      return true;
    }
  }

  end_ = true;
  return false;
}

}  // namespace bustub
