/**
 * index_iterator.cpp
 */
#include "storage/index/index_iterator.h"
#include <cassert>
#include <memory>
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
  // ReadPageGuard guard = bpm_->FetchPageRead(page_id_);
  auto page = guard_.As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  return page->GetNextPageId() == INVALID_PAGE_ID && index_ == page->GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  // ReadPageGuard guard = bpm_->FetchPageRead(page_id_);
  auto page = guard_.As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  return page->array_[index_];
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  // ReadPageGuard guard = bpm_->FetchPageRead(page_id_);
  auto page = guard_.As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  if (index_ == page->GetSize() - 1 && page->GetNextPageId() != INVALID_PAGE_ID) {
    guard_ = bpm_->FetchPageBasic(page->GetNextPageId());
    page_id_ = guard_.PageId();
    index_ = 0;
    return *this;
  }
  index_++;
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
