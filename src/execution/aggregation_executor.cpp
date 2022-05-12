//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"
#include "execution/expressions/aggregate_value_expression.h"
#include "execution/expressions/column_value_expression.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx), aht_(plan->GetAggregates(), plan->GetAggregateTypes()), aht_iterator_(aht_.Begin()) {
  plan_ = plan;
  child_ = std::move(child);
}

void AggregationExecutor::Init() {
  child_->Init();
  end_ = false;
  aht_.Clear();
  Tuple tuple;
  RID rid;
  while (child_->Next(&tuple, &rid)) {
    auto key = MakeAggregateKey(&tuple);
    auto value = MakeAggregateValue(&tuple);
    aht_.InsertCombine(key, value);
  }
  aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  if (end_) {
    return false;
  }
  while (aht_iterator_ != aht_.End()) {
    auto &key = aht_iterator_.Key();
    auto &value = aht_iterator_.Val();
    ++aht_iterator_;
    if (plan_->GetHaving() == nullptr ||
        plan_->GetHaving()->EvaluateAggregate(key.group_bys_, value.aggregates_).GetAs<bool>()) {
      std::vector<Value> values;
      for (auto &column : plan_->OutputSchema()->GetColumns()) {
        auto column_expr = reinterpret_cast<const AggregateValueExpression *>(column.GetExpr());
        values.push_back(column_expr->EvaluateAggregate(key.group_bys_, value.aggregates_));
      }
      Tuple new_tuple(values, plan_->OutputSchema());
      *tuple = new_tuple;
      return true;
    }
  }
  end_ = true;
  return false;
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

}  // namespace bustub
