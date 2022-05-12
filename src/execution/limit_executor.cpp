//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
  limit_ = plan_->GetLimit();
}

void LimitExecutor::Init() {
  end_ = false;
  child_executor_->Init();
  limit_ = plan_->GetLimit();
}

bool LimitExecutor::Next(Tuple *tuple, RID *rid) {
  if (end_) {
    return false;
  }
  while (child_executor_->Next(tuple, rid)) {
    limit_--;
    if (limit_ == 0) {
      end_ = true;
    }
    return true;
  }
  return false;
}

}  // namespace bustub
