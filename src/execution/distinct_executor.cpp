//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"
#include "execution/expressions/column_value_expression.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), iter_(distinct_set_.Begin()) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
}

void DistinctExecutor::Init() {
  child_executor_->Init();
  end_ = false;
  Tuple tuple;
  RID rid;
  distinct_set_.Clear();
  while (child_executor_->Next(&tuple, &rid)) {
    DistinctKey key;
    for (uint32_t i = 0; i < plan_->OutputSchema()->GetColumnCount(); i++) {
      key.columns_.push_back(tuple.GetValue(plan_->OutputSchema(), i));
    }
    //      for (auto &column : plan_->OutputSchema()->GetColumns()) {
    //        auto column_expr = reinterpret_cast<const ColumnValueExpression *>(column.GetExpr());
    //        std::cout << column_expr->GetColIdx()<<std::endl;
    //        key.columns_.push_back(tuple.GetValue(plan_->GetChildPlan()->OutputSchema(), column_expr->GetColIdx()));
    //      }
    distinct_set_.Insert(key);
  }
  iter_ = distinct_set_.Begin();
}

bool DistinctExecutor::Next(Tuple *tuple, RID *rid) {
  if (end_) {
    return false;
  }
  while (iter_ != distinct_set_.End()) {
    auto values = iter_.Key().columns_;
    Tuple new_tuple(values, child_executor_->GetOutputSchema());
    *tuple = new_tuple;

    ++iter_;
    if (iter_ == distinct_set_.End()) {
      end_ = true;
    }
    return true;
  }

  return false;
}

}  // namespace bustub
