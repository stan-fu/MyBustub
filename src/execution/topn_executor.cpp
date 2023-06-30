#include "execution/executors/topn_executor.h"
#include <algorithm>
namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      cmp_(plan_->GetOrderBy(), child_executor_) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  container_.clear();
  cmp_.SetSchema(child_executor_->GetOutputSchema());
  std::priority_queue<Tuple, std::vector<Tuple>, Cmp> queue(cmp_);

  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    if (queue.size() >= plan_->GetN()) {
      if (cmp_(tuple, queue.top())) {
        queue.pop();
        queue.push(tuple);
      }
    } else {
      queue.push(tuple);
    }
  }
  while (!queue.empty()) {
    container_.push_back(queue.top());
    queue.pop();
  }
  index_ = container_.size() - 1;
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (index_ >= plan_->GetN()) {
    return false;
  }
  *tuple = container_[index_--];
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return container_.size(); };

}  // namespace bustub
