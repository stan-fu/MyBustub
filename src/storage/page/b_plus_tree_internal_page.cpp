//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "include/common/logger.h"
#include "storage/page/b_plus_tree_internal_page.h"
namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetMaxSize(max_size);
  SetSize(0);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  BUSTUB_ASSERT(index > 0 && index < GetSize(), "index invalid");
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  BUSTUB_ASSERT(index > 0 && index < GetSize(), "index invalid");
  array_[index].first = key;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, KeyComparator &comp) -> bool {
  IncreaseSize(1);
  if (GetSize() == 1) {
    array_[0] = {key, value};
    return true;
  }
  // find appropriate position to insert
  // Different from internal page, insert operation is from leaf to root, would not exist duplicated key in internal
  // page while leaf-insert success
  for (int i = GetSize() - 2; i > 0; i--) {
    if (comp(key, array_[i].first) > 0) {
      array_[i + 1] = {key, value};
      return true;
    }
    array_[i + 1] = array_[i];
  }
  array_[1] = {key, value};
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Reallocate(const std::vector<MappingType> &array, int begin, int end) {
  SetSize(end - begin);
  for (int i = 0, j = begin; j < end; i++, j++) {
    array_[i] = array[j];
  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::AccquireArray(std::vector<MappingType> &array) {
  for (int i = 0; i < GetSize(); i++) {
    array.push_back(array_[i]);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  for (int i = 0; i < static_cast<int>(INTERNAL_PAGE_SIZE); i++) {
    if (array_[i].second == value) {
      return i;
    }
  }
  BUSTUB_ASSERT(false, "value not exist");
  return 0;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
