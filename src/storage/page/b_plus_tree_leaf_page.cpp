//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "include/common/logger.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  BUSTUB_ASSERT(max_size <= (int)LEAF_PAGE_SIZE, "max_size init error");
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
  next_page_id_ = INVALID_PAGE_ID;
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  BUSTUB_ASSERT(index < GetSize(), "index invalid");
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  BUSTUB_ASSERT(index < GetSize(), "index invalid");
  return array_[index].second;
}

/**
 * @brief Helper method to insert key-value pair into leaf page, current page size must not be full
 *
 * @return false if exist duplicated key, else true
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, KeyComparator &comp) -> bool {
  // find appropriate position to insert
  // should not move pair if insert failed, thus can't find and move at the same time
  int k = 0;
  for (; k < GetSize(); k++) {
    if (comp(key, array_[k].first) <= 0) {
      break;
    }
  }
  if (comp(key, array_[k].first) == 0) {
    return false;
  }

  // insert key at array_[k]
  IncreaseSize(1);
  for (int i = GetSize() - 2; i >= k; i--) {
    array_[i + 1] = array_[i];
  }
  array_[k] = {key, value};
  return true;
}

/**
 * @brief Helper method to reallocate array[begin:end] to array_;
 *
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Reallocate(const std::vector<MappingType> &array, int begin, int end) {
  SetSize(end - begin);
  for (int i = 0, j = begin; j < end; i++, j++) {
    array_[i] = array[j];
  }
}

/**
 * @brief Helper method to accquire array_ from leaf page
 *
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::AccquireArray(std::vector<MappingType> &array) const {
  for (int i = 0; i < GetSize(); i++) {
    array.push_back(array_[i]);
  }
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
