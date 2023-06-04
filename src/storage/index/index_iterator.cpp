/**
 * index_iterator.cpp
 */
#include "storage/index/index_iterator.h"
#include <cassert>
#include "include/common/logger.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  // return page_->GetNextPageId() == INVALID_PAGE_ID && index_ == page_->GetSize();
  return index_ == static_cast<size_t>(page_->GetSize());
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  BUSTUB_ASSERT(page_ != nullptr, "iterator invalid");
  return page_->array_[index_];
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  // if ((index_ = page_->GetSize() - 1) && page_->GetNextPageId() != INVALID_PAGE_ID) {
  //   auto guard = bpm_->FetchPageBasic(page_->GetNextPageId());
  //   page = guard.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  //   index_ = 0;
  //   return *this;
  // }
  index_++;
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
