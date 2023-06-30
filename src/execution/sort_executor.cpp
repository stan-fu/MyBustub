#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      cmp_(plan_->GetOrderBy(), child_executor_) {}

void SortExecutor::Init() {
  child_executor_->Init();
  results_.clear();
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    results_.push_back(tuple);
  }
  std::sort(results_.begin(), results_.end(), cmp_);
  iter_ = results_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == results_.end()) {
    return false;
  }
  *tuple = *iter_++;
  return true;
}

}  // namespace bustub
