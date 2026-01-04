/*------------------------------------------------------------------------------
 - Copyright (c) 2024. Websoft research group, Nanjing University.
 -
 - This program is free software: you can redistribute it and/or modify
 - it under the terms of the GNU General Public License as published by
 - the Free Software Foundation, either version 3 of the License, or
 - (at your option) any later version.
 -
 - This program is distributed in the hope that it will be useful,
 - but WITHOUT ANY WARRANTY; without even the implied warranty of
 - MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 - GNU General Public License for more details.
 -
 - You should have received a copy of the GNU General Public License
 - along with this program.  If not, see <https://www.gnu.org/licenses/>.
 -----------------------------------------------------------------------------*/

//
// Created by ziqi on 2024/7/28.
//

#include "index_bptree.h"
#include "../../../common/error.h"
#include "../buffer/page_guard.h"
#include <algorithm>
#include <cstring>
#include <vector>
#include <string>

#define TEST_BPTREE

namespace njudb {

static void DebugPrintLeaf(const BPTreeLeafPage *leaf_node, const RecordSchema *key_schema)
{
  return;
  // print all keys in the leaf node for debugging
  page_id_t leaf_page_id = leaf_node->GetPageId();
  printf("Leaf %d: ", leaf_page_id);
  for (int i = 0; i < leaf_node->GetSize(); i++) {
    Record current_key(key_schema, nullptr, leaf_node->KeyAt(i), INVALID_RID);
    printf("key[%d]: %s; ", i, current_key.GetValueAt(0)->ToString().c_str());
  }
  printf("\n");
}

static void DebugPrintInternal(const BPTreeInternalPage *internal_node, const RecordSchema *key_schema)
{
  return;
  // print all keys in the internal node for debugging
  page_id_t internal_page_id = internal_node->GetPageId();
  printf("Internal %d: ", internal_page_id);
  for (int i = 0; i < internal_node->GetSize(); i++) {
    Record current_key(key_schema, nullptr, internal_node->KeyAt(i), INVALID_RID);
    printf("key[%d]: %s, value: %d; ", i, current_key.GetValueAt(0)->ToString().c_str(), internal_node->ValueAt(i));
  }
  printf("\n");
}

// BPTreePage implementation
void BPTreePage::Init(idx_id_t index_id, page_id_t page_id, page_id_t parent_id, BPTreeNodeType node_type, int max_size)
{
  index_id_       = index_id;
  page_id_        = page_id;
  parent_page_id_ = parent_id;
  node_type_      = node_type;
  max_size_       = max_size;
  size_           = 0;
}

auto BPTreePage::IsLeaf() const -> bool
{
  return node_type_ == BPTreeNodeType::LEAF;
}

auto BPTreePage::IsRoot() const -> bool
{
  return parent_page_id_ == INVALID_PAGE_ID;
}

auto BPTreePage::GetSize() const -> int
{
  return size_;
}

auto BPTreePage::GetMaxSize() const -> int
{
  return max_size_;
}
void BPTreePage::SetSize(int size) { size_ = size; }

auto BPTreePage::GetPageId() const -> page_id_t
{
  return page_id_;
}

auto BPTreePage::GetParentPageId() const -> page_id_t
{
  return parent_page_id_;
}

void BPTreePage::SetParentPageId(page_id_t parent_page_id) { parent_page_id_ = parent_page_id; }
auto BPTreePage::IsSafe(bool is_insert) const -> bool
{
  if (is_insert) {
    return size_ < max_size_;
  }
  int min_size = IsRoot() ? (IsLeaf() ? 1 : 2) : (max_size_ + 1) / 2;
  return size_ > min_size;
}

// BPTreeLeafPage implementation
void BPTreeLeafPage::Init(idx_id_t index_id, page_id_t page_id, page_id_t parent_id, int key_size, int max_size)
{
  BPTreePage::Init(index_id, page_id, parent_id, BPTreeNodeType::LEAF, max_size);
  key_size_     = key_size;
  next_page_id_ = INVALID_PAGE_ID;
}

auto BPTreeLeafPage::GetNextPageId() const -> page_id_t
{
  return next_page_id_;
}

void BPTreeLeafPage::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

auto BPTreeLeafPage::KeyAt(int index) const -> const char *
{
  return GetKeysArray() + index * key_size_;
}

auto BPTreeLeafPage::ValueAt(int index) const -> RID
{
  return GetValuesArray()[index];
}

void BPTreeLeafPage::SetKeyAt(int index, const char *key) { std::memcpy(GetKeysArray() + index * key_size_, key, key_size_); }

void BPTreeLeafPage::SetValueAt(int index, const RID &value) { GetValuesArray()[index] = value; }

auto BPTreeLeafPage::KeyIndex(const Record &key, const RecordSchema *schema) const -> int
{
  for (int i = 0; i < size_; i++) {
    Record current_key(schema, nullptr, KeyAt(i), INVALID_RID);
    if (Record::Compare(key, current_key) <= 0) {
      return i;
    }
  }
  return size_;
}

auto BPTreeLeafPage::LowerBound(const Record &key, const RecordSchema *schema) const -> int
{
  return KeyIndex(key, schema);
}

auto BPTreeLeafPage::UpperBound(const Record &key, const RecordSchema *schema) const -> int
{
  // Find the first position where key < keys[pos]
  // This is useful for < queries
  for (int i = 0; i < size_; i++) {
    Record current_key(schema, nullptr, KeyAt(i), INVALID_RID);
    if (Record::Compare(key, current_key) < 0) {
      return i;
    }
  }
  return size_;
}

auto BPTreeLeafPage::Lookup(const Record &key, const RecordSchema *schema) const -> std::vector<RID>
{
  std::vector<RID> result;
  int              index = KeyIndex(key, schema);
  while (index < size_) {
    Record current_key(schema, nullptr, KeyAt(index), INVALID_RID);
    if (Record::Compare(key, current_key) == 0) {
      result.push_back(ValueAt(index));
      index++;
    } else {
      break;
    }
  }
  return result;
}

auto BPTreeLeafPage::Insert(const Record &key, const RID &value, const RecordSchema *schema) -> int
{
  int index = KeyIndex(key, schema);
  for (int i = size_; i > index; i--) {
    SetKeyAt(i, KeyAt(i - 1));
    SetValueAt(i, ValueAt(i - 1));
  }
  SetKeyAt(index, key.GetData());
  SetValueAt(index, value);
  size_++;
  return size_;
}

void BPTreeLeafPage::MoveHalfTo(BPTreeLeafPage *recipient)
{
  int move_size = size_ / 2;
  int start_idx = size_ - move_size;
  recipient->CopyNFrom(KeyAt(start_idx), GetValuesArray() + start_idx, move_size);
  size_ -= move_size;
}

void BPTreeLeafPage::CopyNFrom(const char *keys, const RID *values, int size)
{
  for (int i = 0; i < size; i++) {
    SetKeyAt(size_ + i, keys + i * key_size_);
    SetValueAt(size_ + i, values[i]);
  }
  size_ += size;
}

auto BPTreeLeafPage::RemoveRecord(const Record &key, const RecordSchema *schema) -> int
{
  int index = KeyIndex(key, schema);
  if (index < size_) {
    Record current_key(schema, nullptr, KeyAt(index), INVALID_RID);
    if (Record::Compare(key, current_key) == 0) {
      for (int i = index; i < size_ - 1; i++) {
        SetKeyAt(i, KeyAt(i + 1));
        SetValueAt(i, ValueAt(i + 1));
      }
      size_--;
      return size_;
    }
  }
  return -1;
}

void BPTreeLeafPage::MoveAllTo(BPTreeLeafPage *recipient)
{
  recipient->CopyNFrom(KeyAt(0), GetValuesArray(), size_);
  recipient->SetNextPageId(GetNextPageId());
  size_ = 0;
}

// BPTreeInternalPage implementation
void BPTreeInternalPage::Init(idx_id_t index_id, page_id_t page_id, page_id_t parent_id, int key_size, int max_size)
{
  BPTreePage::Init(index_id, page_id, parent_id, BPTreeNodeType::INTERNAL, max_size);
  key_size_ = key_size;
}

auto BPTreeInternalPage::KeyAt(int index) const -> const char *
{
  return GetKeysArray() + index * key_size_;
}

auto BPTreeInternalPage::GetKeySize() const -> int
{
  return key_size_;
}

auto BPTreeInternalPage::ValueAt(int index) const -> page_id_t
{
  return GetChildrenArray()[index];
}

void BPTreeInternalPage::SetKeyAt(int index, const char *key) { std::memcpy(GetKeysArray() + index * key_size_, key, key_size_); }

void BPTreeInternalPage::SetValueAt(int index, page_id_t value) { GetChildrenArray()[index] = value; }

auto BPTreeInternalPage::Lookup(const Record &key, const RecordSchema *schema) const -> page_id_t
{
  for (int i = 1; i < size_; i++) {
    Record current_key(schema, nullptr, KeyAt(i), INVALID_RID);
    if (Record::Compare(key, current_key) < 0) {
      return ValueAt(i - 1);
    }
  }
  return ValueAt(size_ - 1);
}

auto BPTreeInternalPage::LookupForLowerBound(const Record &key, const RecordSchema *schema) const -> page_id_t
{
  // For lower bound, we want to find the leftmost position where key could be inserted
  // This means finding the leftmost child that could contain keys >= key
  for (int i = 1; i < size_; i++) {
    Record current_key(schema, nullptr, KeyAt(i), INVALID_RID);
    if (Record::Compare(key, current_key) <= 0) {
      return ValueAt(i - 1);
    }
  }
  return ValueAt(size_ - 1);
}

auto BPTreeInternalPage::LookupForUpperBound(const Record &key, const RecordSchema *schema) const -> page_id_t
{
  // For upper bound, we want to find the rightmost position where key could be inserted
  // This means finding the rightmost child that could contain keys <= key
  return Lookup(key, schema);
}

void BPTreeInternalPage::PopulateNewRoot(page_id_t old_root_id, const Record &new_key, page_id_t new_page_id)
{
  SetValueAt(0, old_root_id);
  SetKeyAt(1, new_key.GetData());
  SetValueAt(1, new_page_id);
  size_ = 2;
}

auto BPTreeInternalPage::InsertNodeAfter(page_id_t old_value, const Record &new_key, page_id_t new_value) -> int
{
  int index = -1;
  for (int i = 0; i < size_; i++) {
    if (ValueAt(i) == old_value) {
      index = i;
      break;
    }
  }
  if (index == -1) return -1;

  for (int i = size_; i > index + 1; i--) {
    SetKeyAt(i, KeyAt(i - 1));
    SetValueAt(i, ValueAt(i - 1));
  }
  SetKeyAt(index + 1, new_key.GetData());
  SetValueAt(index + 1, new_value);
  size_++;
  return size_;
}

void BPTreeInternalPage::MoveHalfTo(BPTreeInternalPage *recipient, BufferPoolManager *buffer_pool_manager)
{
  int move_size = size_ / 2;
  int start_idx = size_ - move_size;
  recipient->CopyNFrom(KeyAt(start_idx), GetChildrenArray() + start_idx, move_size, buffer_pool_manager);
  size_ -= move_size;
}

void BPTreeInternalPage::CopyNFrom(
    const char *keys, const page_id_t *values, int size, BufferPoolManager *buffer_pool_manager)
{
  for (int i = 0; i < size; i++) {
    SetKeyAt(size_ + i, keys + i * key_size_);
    SetValueAt(size_ + i, values[i]);

    auto child_guard = buffer_pool_manager->FetchPageWrite(index_id_, values[i]);
    auto child_node  = reinterpret_cast<BPTreePage *>(PageContentPtr(child_guard.GetMutableData()));
    child_node->SetParentPageId(page_id_);
  }
  size_ += size;
}

void BPTreeInternalPage::MoveAllTo(
    BPTreeInternalPage *recipient, const Record &middle_key, BufferPoolManager *buffer_pool_manager)
{
  // For internal nodes, we need to merge:
  // 1. The middle key from the parent (this becomes a key in the recipient)
  // 2. All keys and children from the source node
  
  SetKeyAt(0, middle_key.GetData());
  recipient->CopyNFrom(KeyAt(0), GetChildrenArray(), size_, buffer_pool_manager);
  size_ = 0;
}

// BPTreeIndex implementation
BPTreeIndex::BPTreeIndex(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, idx_id_t index_id,
    const RecordSchema *key_schema)
    : Index(disk_manager, buffer_pool_manager, IndexType::BPTREE, index_id, key_schema)
{

  // Initialize index header
  InitializeIndex();
}

void BPTreeIndex::InitializeIndex()
{
  // Get or create header page
  auto header_guard = buffer_pool_manager_->FetchPageWrite(index_id_, FILE_HEADER_PAGE_ID);
  if (!header_guard.IsValid()) {
    NJUDB_THROW(NJUDB_EXCEPTION_EMPTY, "Cannot fetch header page");
  }

  auto header = reinterpret_cast<BPTreeIndexHeader *>(header_guard.GetMutableData());

  // Check if already initialized
  if (header->page_num_ != 0) {
    return;
  }

  // first check if the header and the schema raw data can accomodate int the header file
  if (key_schema_->SerializeSize() + sizeof(BPTreeIndexHeader) > PAGE_SIZE) {
    NJUDB_THROW(NJUDB_INDEX_FAIL, "Key schema too large to fit in B+ tree header");
  }

  // Initialize header
  header->root_page_id_       = INVALID_PAGE_ID;
  header->first_free_page_id_ = INVALID_PAGE_ID;
  header->tree_height_        = 0;
  header->page_num_           = 1;  // Header page counts
  header->key_size_           = key_schema_->GetRecordLength();
  header->value_size_         = sizeof(RID);

  // Note: TEST_BPTREE mode is for testing your B+tree implementation.
  // you can only undef it after you have passed gtests and is ready to use it in executors.
#ifdef TEST_BPTREE
  std::cout << "TEST_BPTREE mode: using fixed max sizes" << std::endl;
  header->leaf_max_size_     = 4;
  header->internal_max_size_ = 4;
#else
  // Calculate max sizes based on page size

  size_t leaf_header_size     = sizeof(BPTreeLeafPage);
  size_t available_leaf_space = PAGE_SIZE - PAGE_HEADER_SIZE - leaf_header_size;
  header->leaf_max_size_      = available_leaf_space / (header->key_size_ + sizeof(RID));

  size_t internal_header_size     = sizeof(BPTreeInternalPage);
  size_t available_internal_space = PAGE_SIZE - PAGE_HEADER_SIZE - internal_header_size;
  header->internal_max_size_      = available_internal_space / (header->key_size_ + sizeof(page_id_t));

  // check if the max size of leaf and internal are valid
  if (static_cast<int>(header->leaf_max_size_) <= 0 || static_cast<int>(header->internal_max_size_) <= 0) {
    NJUDB_THROW(NJUDB_INDEX_FAIL, "Key too large for a B+ tree node to fit into a single page");
  }
#endif
}

auto BPTreeIndex::NewPage() -> page_id_t
{
  auto header_guard = buffer_pool_manager_->FetchPageWrite(index_id_, FILE_HEADER_PAGE_ID);
  auto header       = reinterpret_cast<BPTreeIndexHeader *>(header_guard.GetMutableData());
  page_id_t new_pid;

  if (header->first_free_page_id_ != INVALID_PAGE_ID) {
    new_pid = header->first_free_page_id_;
    auto free_page_guard = buffer_pool_manager_->FetchPageWrite(index_id_, new_pid);
    header->first_free_page_id_ = free_page_guard.GetPage()->GetNextFreePageId();
  } else {
    new_pid = header->page_num_++;
  }
  return new_pid;
}

void BPTreeIndex::DeletePage(page_id_t page_id)
{
  auto header_guard = buffer_pool_manager_->FetchPageWrite(index_id_, FILE_HEADER_PAGE_ID);
  auto header       = reinterpret_cast<BPTreeIndexHeader *>(header_guard.GetMutableData());

  auto page_guard = buffer_pool_manager_->FetchPageWrite(index_id_, page_id);
  page_guard.GetPage()->SetNextFreePageId(header->first_free_page_id_);
  header->first_free_page_id_ = page_id;
}

auto BPTreeIndex::FindLeafPage(const Record &key, bool leftMost) -> page_id_t
{
  auto header_guard = buffer_pool_manager_->FetchPageRead(index_id_, FILE_HEADER_PAGE_ID);
  auto header       = reinterpret_cast<const BPTreeIndexHeader *>(header_guard.GetData());
  page_id_t curr_pid = header->root_page_id_;

  if (curr_pid == INVALID_PAGE_ID) return INVALID_PAGE_ID;

  while (true) {
    auto page_guard = buffer_pool_manager_->FetchPageRead(index_id_, curr_pid);
    auto node       = reinterpret_cast<const BPTreePage *>(PageContentPtr(page_guard.GetData()));
    if (node->IsLeaf()) break;

    auto internal_node = reinterpret_cast<const BPTreeInternalPage *>(node);
    if (leftMost) {
      curr_pid = internal_node->ValueAt(0);
    } else {
      curr_pid = internal_node->Lookup(key, key_schema_);
    }
  }
  return curr_pid;
}

auto BPTreeIndex::FindLeafPageForRange(const Record &key, bool isLowerBound) -> page_id_t
{
  auto header_guard = buffer_pool_manager_->FetchPageRead(index_id_, FILE_HEADER_PAGE_ID);
  auto header       = reinterpret_cast<const BPTreeIndexHeader *>(header_guard.GetData());
  page_id_t curr_pid = header->root_page_id_;

  if (curr_pid == INVALID_PAGE_ID) return INVALID_PAGE_ID;

  while (true) {
    auto page_guard = buffer_pool_manager_->FetchPageRead(index_id_, curr_pid);
    auto node       = reinterpret_cast<const BPTreePage *>(PageContentPtr(page_guard.GetData()));
    if (node->IsLeaf()) break;

    auto internal_node = reinterpret_cast<const BPTreeInternalPage *>(node);
    if (isLowerBound) {
      curr_pid = internal_node->LookupForLowerBound(key, key_schema_);
    } else {
      curr_pid = internal_node->LookupForUpperBound(key, key_schema_);
    }
  }
  return curr_pid;
}

void BPTreeIndex::StartNewTree(const Record &key, const RID &value)
{
  page_id_t new_pid      = NewPage();
  auto      header_guard = buffer_pool_manager_->FetchPageWrite(index_id_, FILE_HEADER_PAGE_ID);
  auto      header       = reinterpret_cast<BPTreeIndexHeader *>(header_guard.GetMutableData());
  header->root_page_id_  = new_pid;
  header->tree_height_   = 1;
  header->num_entries_   = 1;

  auto page_guard = buffer_pool_manager_->FetchPageWrite(index_id_, new_pid);
  auto leaf_node  = reinterpret_cast<BPTreeLeafPage *>(PageContentPtr(page_guard.GetMutableData()));
  leaf_node->Init(index_id_, new_pid, INVALID_PAGE_ID, header->key_size_, header->leaf_max_size_);
  leaf_node->Insert(key, value, key_schema_);
}

auto BPTreeIndex::InsertIntoLeaf(const Record &key, const RID &value) -> bool
{
  page_id_t leaf_pid   = FindLeafPage(key);
  auto      page_guard = buffer_pool_manager_->FetchPageWrite(index_id_, leaf_pid);
  auto      leaf_node  = reinterpret_cast<BPTreeLeafPage *>(PageContentPtr(page_guard.GetMutableData()));

  if (leaf_node->GetSize() < leaf_node->GetMaxSize()) {
    leaf_node->Insert(key, value, key_schema_);
    auto header_guard = buffer_pool_manager_->FetchPageWrite(index_id_, FILE_HEADER_PAGE_ID);
    auto header       = reinterpret_cast<BPTreeIndexHeader *>(header_guard.GetMutableData());
    header->num_entries_++;
    return true;
  }

  page_id_t new_pid        = NewPage();
  auto      new_page_guard = buffer_pool_manager_->FetchPageWrite(index_id_, new_pid);
  auto      new_leaf_node  = reinterpret_cast<BPTreeLeafPage *>(PageContentPtr(new_page_guard.GetMutableData()));
  new_leaf_node->Init(
      index_id_, new_pid, leaf_node->GetParentPageId(), leaf_node->key_size_, leaf_node->GetMaxSize());

  leaf_node->MoveHalfTo(new_leaf_node);

  Record middle_key(key_schema_, nullptr, new_leaf_node->KeyAt(0), INVALID_RID);
  if (Record::Compare(key, middle_key) < 0) {
    leaf_node->Insert(key, value, key_schema_);
  } else {
    new_leaf_node->Insert(key, value, key_schema_);
  }

  new_leaf_node->SetNextPageId(leaf_node->GetNextPageId());
  leaf_node->SetNextPageId(new_pid);

  page_guard.Drop();
  new_page_guard.Drop();

  InsertIntoParent(leaf_pid, middle_key, new_pid);

  auto header_guard = buffer_pool_manager_->FetchPageWrite(index_id_, FILE_HEADER_PAGE_ID);
  auto header       = reinterpret_cast<BPTreeIndexHeader *>(header_guard.GetMutableData());
  header->num_entries_++;
  return true;
}

void BPTreeIndex::InsertIntoParent(page_id_t old_node_id, const Record &key, page_id_t new_node_id)
{
  page_id_t parent_id;
  bool      is_root;
  {
    auto old_node_guard = buffer_pool_manager_->FetchPageRead(index_id_, old_node_id);
    auto old_node       = reinterpret_cast<const BPTreePage *>(PageContentPtr(old_node_guard.GetData()));
    is_root             = old_node->IsRoot();
    parent_id           = old_node->GetParentPageId();
  }

  if (is_root) {
    InsertIntoNewRoot(old_node_id, key, new_node_id);
    return;
  }

  auto parent_guard = buffer_pool_manager_->FetchPageWrite(index_id_, parent_id);
  auto parent_node  = reinterpret_cast<BPTreeInternalPage *>(PageContentPtr(parent_guard.GetMutableData()));

  if (parent_node->GetSize() <= parent_node->GetMaxSize()) {
    parent_node->InsertNodeAfter(old_node_id, key, new_node_id);
    return;
  }

  page_id_t new_parent_pid   = NewPage();
  auto      new_parent_guard = buffer_pool_manager_->FetchPageWrite(index_id_, new_parent_pid);
  auto      new_parent_node  = reinterpret_cast<BPTreeInternalPage *>(PageContentPtr(new_parent_guard.GetMutableData()));
  new_parent_node->Init(
      index_id_, new_parent_pid, parent_node->GetParentPageId(), parent_node->GetKeySize(), parent_node->GetMaxSize());

  parent_node->InsertNodeAfter(old_node_id, key, new_node_id);
  parent_node->MoveHalfTo(new_parent_node, buffer_pool_manager_);

  Record push_key(key_schema_, nullptr, new_parent_node->KeyAt(0), INVALID_RID);
  
  parent_guard.Drop();
  new_parent_guard.Drop();

  InsertIntoParent(parent_id, push_key, new_parent_pid);
}

void BPTreeIndex::InsertIntoNewRoot(page_id_t old_root_id, const Record &key, page_id_t new_page_id)
{
  page_id_t new_root_pid = NewPage();
  auto      header_guard = buffer_pool_manager_->FetchPageWrite(index_id_, FILE_HEADER_PAGE_ID);
  auto      header       = reinterpret_cast<BPTreeIndexHeader *>(header_guard.GetMutableData());
  header->root_page_id_  = new_root_pid;
  header->tree_height_++;

  auto page_guard = buffer_pool_manager_->FetchPageWrite(index_id_, new_root_pid);
  auto root_node  = reinterpret_cast<BPTreeInternalPage *>(PageContentPtr(page_guard.GetMutableData()));
  root_node->Init(index_id_, new_root_pid, INVALID_PAGE_ID, header->key_size_, header->internal_max_size_);
  root_node->PopulateNewRoot(old_root_id, key, new_page_id);

  auto old_node_guard = buffer_pool_manager_->FetchPageWrite(index_id_, old_root_id);
  auto old_node       = reinterpret_cast<BPTreePage *>(PageContentPtr(old_node_guard.GetMutableData()));
  old_node->SetParentPageId(new_root_pid);

  auto new_node_guard = buffer_pool_manager_->FetchPageWrite(index_id_, new_page_id);
  auto new_node       = reinterpret_cast<BPTreePage *>(PageContentPtr(new_node_guard.GetMutableData()));
  new_node->SetParentPageId(new_root_pid);
}

void BPTreeIndex::Insert(const Record &key, const RID &rid)
{
  std::unique_lock<std::shared_mutex> lock(index_latch_);
  auto                                header_guard = buffer_pool_manager_->FetchPageRead(index_id_, FILE_HEADER_PAGE_ID);
  auto header = reinterpret_cast<const BPTreeIndexHeader *>(header_guard.GetData());

  if (header->root_page_id_ == INVALID_PAGE_ID) {
    header_guard.Drop();
    StartNewTree(key, rid);
    return;
  }
  header_guard.Drop();
  InsertIntoLeaf(key, rid);
}

auto BPTreeIndex::Delete(const Record &key) -> bool
{
  std::unique_lock<std::shared_mutex> lock(index_latch_);
  page_id_t                           leaf_pid = FindLeafPage(key);
  if (leaf_pid == INVALID_PAGE_ID) return false;

  auto page_guard = buffer_pool_manager_->FetchPageWrite(index_id_, leaf_pid);
  auto leaf_node  = reinterpret_cast<BPTreeLeafPage *>(PageContentPtr(page_guard.GetMutableData()));

  if (leaf_node->RemoveRecord(key, key_schema_) == -1) return false;

  auto header_guard = buffer_pool_manager_->FetchPageWrite(index_id_, FILE_HEADER_PAGE_ID);
  auto header       = reinterpret_cast<BPTreeIndexHeader *>(header_guard.GetMutableData());
  header->num_entries_--;
  header_guard.Drop();

  bool should_rebalance = !leaf_node->IsSafe(false);
  page_guard.Drop();

  if (should_rebalance) {
    CoalesceOrRedistribute(leaf_pid);
  }
  return true;
}

auto BPTreeIndex::CoalesceOrRedistribute(page_id_t node_id) -> bool
{
  auto node_guard = buffer_pool_manager_->FetchPageWrite(index_id_, node_id);
  auto node       = reinterpret_cast<BPTreePage *>(PageContentPtr(node_guard.GetMutableData()));

  if (node->IsRoot()) {
    return AdjustRoot(node);
  }

  page_id_t parent_id    = node->GetParentPageId();
  auto      parent_guard = buffer_pool_manager_->FetchPageWrite(index_id_, parent_id);
  auto      parent_node  = reinterpret_cast<BPTreeInternalPage *>(PageContentPtr(parent_guard.GetMutableData()));

  int index = -1;
  for (int i = 0; i < parent_node->GetSize(); i++) {
    if (parent_node->ValueAt(i) == node_id) {
      index = i;
      break;
    }
  }

  int neighbor_index = (index == 0) ? 1 : index - 1;
  page_id_t neighbor_pid = parent_node->ValueAt(neighbor_index);
  
  auto neighbor_guard = buffer_pool_manager_->FetchPageWrite(index_id_, neighbor_pid);
  auto neighbor_node = reinterpret_cast<BPTreePage *>(PageContentPtr(neighbor_guard.GetMutableData()));

  if (neighbor_node->GetSize() + node->GetSize() <= node->GetMaxSize()) {
    node_guard.Drop();
    parent_guard.Drop();
    neighbor_guard.Drop();
    
    if (index == 0) {
      return Coalesce(node_id, neighbor_pid, parent_id, 1);
    }
    return Coalesce(neighbor_pid, node_id, parent_id, index);
  }

  Redistribute(neighbor_node, node, index);
  return false;
}

auto BPTreeIndex::Coalesce(page_id_t neighbor_node_id, page_id_t node_id, page_id_t parent_id, int index) -> bool
{
  auto neighbor_guard = buffer_pool_manager_->FetchPageWrite(index_id_, neighbor_node_id);
  auto node_guard     = buffer_pool_manager_->FetchPageWrite(index_id_, node_id);
  auto parent_guard   = buffer_pool_manager_->FetchPageWrite(index_id_, parent_id);

  auto neighbor_node = reinterpret_cast<BPTreePage *>(PageContentPtr(neighbor_guard.GetMutableData()));
  auto node          = reinterpret_cast<BPTreePage *>(PageContentPtr(node_guard.GetMutableData()));
  auto parent_node   = reinterpret_cast<BPTreeInternalPage *>(PageContentPtr(parent_guard.GetMutableData()));

  if (node->IsLeaf()) {
    auto leaf_node     = reinterpret_cast<BPTreeLeafPage *>(node);
    auto neighbor_leaf = reinterpret_cast<BPTreeLeafPage *>(neighbor_node);
    leaf_node->MoveAllTo(neighbor_leaf);
  } else {
    auto internal_node     = reinterpret_cast<BPTreeInternalPage *>(node);
    auto neighbor_internal = reinterpret_cast<BPTreeInternalPage *>(neighbor_node);
    Record middle_key(key_schema_, nullptr, parent_node->KeyAt(index), INVALID_RID);
    internal_node->MoveAllTo(neighbor_internal, middle_key, buffer_pool_manager_);
  }

  for (int i = index; i < parent_node->GetSize() - 1; i++) {
    parent_node->SetKeyAt(i, parent_node->KeyAt(i + 1));
    parent_node->SetValueAt(i, parent_node->ValueAt(i + 1));
  }
  parent_node->SetSize(parent_node->GetSize() - 1);

  DeletePage(node_id);

  bool parent_unsafe = !parent_node->IsSafe(false);
  
  neighbor_guard.Drop();
  node_guard.Drop();
  parent_guard.Drop();

  if (parent_unsafe) {
    return CoalesceOrRedistribute(parent_id);
  }
  return true;
}

void BPTreeIndex::Redistribute(BPTreePage *neighbor_node, BPTreePage *node, int index)
{
  page_id_t parent_id    = node->GetParentPageId();
  auto      parent_guard = buffer_pool_manager_->FetchPageWrite(index_id_, parent_id);
  auto      parent_node  = reinterpret_cast<BPTreeInternalPage *>(PageContentPtr(parent_guard.GetMutableData()));

  if (node->IsLeaf()) {
    auto leaf_node     = reinterpret_cast<BPTreeLeafPage *>(node);
    auto neighbor_leaf = reinterpret_cast<BPTreeLeafPage *>(neighbor_node);

    if (index == 0) {
      leaf_node->SetKeyAt(leaf_node->GetSize(), neighbor_leaf->KeyAt(0));
      leaf_node->SetValueAt(leaf_node->GetSize(), neighbor_leaf->ValueAt(0));
      leaf_node->SetSize(leaf_node->GetSize() + 1);

      for (int i = 0; i < neighbor_leaf->GetSize() - 1; i++) {
        neighbor_leaf->SetKeyAt(i, neighbor_leaf->KeyAt(i + 1));
        neighbor_leaf->SetValueAt(i, neighbor_leaf->ValueAt(i + 1));
      }
      neighbor_leaf->SetSize(neighbor_leaf->GetSize() - 1);
      parent_node->SetKeyAt(1, neighbor_leaf->KeyAt(0));
    } else {
      for (int i = leaf_node->GetSize(); i > 0; i--) {
        leaf_node->SetKeyAt(i, leaf_node->KeyAt(i - 1));
        leaf_node->SetValueAt(i, leaf_node->ValueAt(i - 1));
      }
      leaf_node->SetKeyAt(0, neighbor_leaf->KeyAt(neighbor_leaf->GetSize() - 1));
      leaf_node->SetValueAt(0, neighbor_leaf->ValueAt(neighbor_leaf->GetSize() - 1));
      leaf_node->SetSize(leaf_node->GetSize() + 1);
      neighbor_leaf->SetSize(neighbor_leaf->GetSize() - 1);
      parent_node->SetKeyAt(index, leaf_node->KeyAt(0));
    }
  } else {
    auto internal_node     = reinterpret_cast<BPTreeInternalPage *>(node);
    auto neighbor_internal = reinterpret_cast<BPTreeInternalPage *>(neighbor_node);

    if (index == 0) {
      internal_node->SetKeyAt(internal_node->GetSize(), parent_node->KeyAt(1));
      internal_node->SetValueAt(internal_node->GetSize(), neighbor_internal->ValueAt(0));
      internal_node->SetSize(internal_node->GetSize() + 1);

      auto child_guard = buffer_pool_manager_->FetchPageWrite(index_id_, neighbor_internal->ValueAt(0));
      auto child_node  = reinterpret_cast<BPTreePage *>(PageContentPtr(child_guard.GetMutableData()));
      child_node->SetParentPageId(internal_node->GetPageId());

      parent_node->SetKeyAt(1, neighbor_internal->KeyAt(1));
      for (int i = 0; i < neighbor_internal->GetSize() - 1; i++) {
        neighbor_internal->SetKeyAt(i, neighbor_internal->KeyAt(i + 1));
        neighbor_internal->SetValueAt(i, neighbor_internal->ValueAt(i + 1));
      }
      neighbor_internal->SetSize(neighbor_internal->GetSize() - 1);
    } else {
      for (int i = internal_node->GetSize(); i > 0; i--) {
        internal_node->SetKeyAt(i, internal_node->KeyAt(i - 1));
        internal_node->SetValueAt(i, internal_node->ValueAt(i - 1));
      }
      internal_node->SetValueAt(0, neighbor_internal->ValueAt(neighbor_internal->GetSize() - 1));
      internal_node->SetKeyAt(1, parent_node->KeyAt(index));

      auto child_guard = buffer_pool_manager_->FetchPageWrite(index_id_, neighbor_internal->ValueAt(neighbor_internal->GetSize() - 1));
      auto child_node  = reinterpret_cast<BPTreePage *>(PageContentPtr(child_guard.GetMutableData()));
      child_node->SetParentPageId(internal_node->GetPageId());

      parent_node->SetKeyAt(index, neighbor_internal->KeyAt(neighbor_internal->GetSize() - 1));
      internal_node->SetSize(internal_node->GetSize() + 1);
      neighbor_internal->SetSize(neighbor_internal->GetSize() - 1);
    }
  }
}

auto BPTreeIndex::AdjustRoot(BPTreePage *old_root_node) -> bool
{
  if (old_root_node->IsLeaf()) {
    if (old_root_node->GetSize() == 0) {
      auto header_guard = buffer_pool_manager_->FetchPageWrite(index_id_, FILE_HEADER_PAGE_ID);
      auto header       = reinterpret_cast<BPTreeIndexHeader *>(header_guard.GetMutableData());
      header->root_page_id_ = INVALID_PAGE_ID;
      header->tree_height_  = 0;
      DeletePage(old_root_node->GetPageId());
      return true;
    }
    return false;
  }

  if (old_root_node->GetSize() == 1) {
    auto internal_node = reinterpret_cast<BPTreeInternalPage *>(old_root_node);
    page_id_t new_root_id = internal_node->ValueAt(0);

    auto header_guard = buffer_pool_manager_->FetchPageWrite(index_id_, FILE_HEADER_PAGE_ID);
    auto header       = reinterpret_cast<BPTreeIndexHeader *>(header_guard.GetMutableData());
    header->root_page_id_ = new_root_id;
    header->tree_height_--;

    auto new_root_guard = buffer_pool_manager_->FetchPageWrite(index_id_, new_root_id);
    auto new_root_node  = reinterpret_cast<BPTreePage *>(PageContentPtr(new_root_guard.GetMutableData()));
    new_root_node->SetParentPageId(INVALID_PAGE_ID);

    DeletePage(old_root_node->GetPageId());
    return true;
  }
  return false;
}

auto BPTreeIndex::Search(const Record &key) -> std::vector<RID>
{
  std::shared_lock<std::shared_mutex> lock(index_latch_);
  page_id_t                           leaf_pid = FindLeafPage(key);
  if (leaf_pid == INVALID_PAGE_ID) return {};

  auto page_guard = buffer_pool_manager_->FetchPageRead(index_id_, leaf_pid);
  auto leaf_node  = reinterpret_cast<const BPTreeLeafPage *>(PageContentPtr(page_guard.GetData()));
  return leaf_node->Lookup(key, key_schema_);
}

auto BPTreeIndex::SearchRange(const Record &low_key, const Record &high_key) -> std::vector<RID>
{
  std::shared_lock<std::shared_mutex> lock(index_latch_);
  page_id_t                           leaf_pid = FindLeafPageForRange(low_key, true);
  if (leaf_pid == INVALID_PAGE_ID) return {};

  std::vector<RID> result;
  while (leaf_pid != INVALID_PAGE_ID) {
    auto page_guard = buffer_pool_manager_->FetchPageRead(index_id_, leaf_pid);
    auto leaf_node  = reinterpret_cast<const BPTreeLeafPage *>(PageContentPtr(page_guard.GetData()));

    int start_idx = leaf_node->LowerBound(low_key, key_schema_);
    for (int i = start_idx; i < leaf_node->GetSize(); i++) {
      Record curr_key(key_schema_, nullptr, leaf_node->KeyAt(i), INVALID_RID);
      if (Record::Compare(curr_key, high_key) <= 0) {
        result.push_back(leaf_node->ValueAt(i));
      } else {
        return result;
      }
    }
    leaf_pid = leaf_node->GetNextPageId();
  }
  return result;
}

// Iterator implementation
BPTreeIndex::BPTreeIterator::BPTreeIterator(BPTreeIndex *tree, page_id_t leaf_page_id, int index)
    : tree_(tree), leaf_page_id_(leaf_page_id), index_(index)
{}

auto BPTreeIndex::BPTreeIterator::IsValid() -> bool
{
  if (leaf_page_id_ == INVALID_PAGE_ID) return false;
  auto page_guard = tree_->buffer_pool_manager_->FetchPageRead(tree_->index_id_, leaf_page_id_);
  auto leaf_node  = reinterpret_cast<const BPTreeLeafPage *>(PageContentPtr(page_guard.GetData()));
  return index_ < leaf_node->GetSize();
}

void BPTreeIndex::BPTreeIterator::Next()
{
  auto page_guard = tree_->buffer_pool_manager_->FetchPageRead(tree_->index_id_, leaf_page_id_);
  auto leaf_node  = reinterpret_cast<const BPTreeLeafPage *>(PageContentPtr(page_guard.GetData()));
  index_++;
  if (index_ >= leaf_node->GetSize()) {
    leaf_page_id_ = leaf_node->GetNextPageId();
    index_        = 0;
  }
}

auto BPTreeIndex::BPTreeIterator::GetKey() -> Record
{
  auto page_guard = tree_->buffer_pool_manager_->FetchPageRead(tree_->index_id_, leaf_page_id_);
  auto leaf_node  = reinterpret_cast<const BPTreeLeafPage *>(PageContentPtr(page_guard.GetData()));
  return Record(tree_->key_schema_, nullptr, leaf_node->KeyAt(index_), INVALID_RID);
}

auto BPTreeIndex::BPTreeIterator::GetRID() -> RID
{
  auto page_guard = tree_->buffer_pool_manager_->FetchPageRead(tree_->index_id_, leaf_page_id_);
  auto leaf_node  = reinterpret_cast<const BPTreeLeafPage *>(PageContentPtr(page_guard.GetData()));
  return leaf_node->ValueAt(index_);
}

auto BPTreeIndex::Begin() -> std::unique_ptr<IIterator>
{
  std::shared_lock<std::shared_mutex> lock(index_latch_);
  page_id_t                           leaf_pid = FindLeafPage(Record(key_schema_), true);
  return std::make_unique<BPTreeIterator>(this, leaf_pid, 0);
}

auto BPTreeIndex::Begin(const Record &key) -> std::unique_ptr<IIterator>
{
  std::shared_lock<std::shared_mutex> lock(index_latch_);
  page_id_t                           leaf_pid = FindLeafPage(key);
  if (leaf_pid == INVALID_PAGE_ID) return End();

  auto page_guard = buffer_pool_manager_->FetchPageRead(index_id_, leaf_pid);
  auto leaf_node  = reinterpret_cast<const BPTreeLeafPage *>(PageContentPtr(page_guard.GetData()));
  int  index      = leaf_node->LowerBound(key, key_schema_);

  if (index >= leaf_node->GetSize()) {
    page_id_t next_pid = leaf_node->GetNextPageId();
    return std::make_unique<BPTreeIterator>(this, next_pid, 0);
  }

  return std::make_unique<BPTreeIterator>(this, leaf_pid, index);
}

auto BPTreeIndex::End() -> std::unique_ptr<IIterator>
{
  return std::make_unique<BPTreeIterator>(this, INVALID_PAGE_ID, 0);
}

void BPTreeIndex::Clear()
{
  std::unique_lock<std::shared_mutex> lock(index_latch_);
  auto                                header_guard = buffer_pool_manager_->FetchPageWrite(index_id_, FILE_HEADER_PAGE_ID);
  auto header = reinterpret_cast<BPTreeIndexHeader *>(header_guard.GetMutableData());

  if (header->root_page_id_ != INVALID_PAGE_ID) {
    ClearPage(header->root_page_id_);
  }
  header->root_page_id_       = INVALID_PAGE_ID;
  header->tree_height_        = 0;
  header->num_entries_        = 0;
  header->page_num_           = 1;
  header->first_free_page_id_ = INVALID_PAGE_ID;
}

void BPTreeIndex::ClearPage(page_id_t page_id)
{
  auto page_guard = buffer_pool_manager_->FetchPageRead(index_id_, page_id);
  auto node       = reinterpret_cast<const BPTreePage *>(PageContentPtr(page_guard.GetData()));

  if (!node->IsLeaf()) {
    auto internal_node = reinterpret_cast<const BPTreeInternalPage *>(node);
    for (int i = 0; i < internal_node->GetSize(); i++) {
      ClearPage(internal_node->ValueAt(i));
    }
  }
  DeletePage(page_id);
}

auto BPTreeIndex::IsEmpty() -> bool
{
  std::shared_lock<std::shared_mutex> lock(index_latch_);
  auto                                header_guard = buffer_pool_manager_->FetchPageRead(index_id_, FILE_HEADER_PAGE_ID);
  auto header = reinterpret_cast<const BPTreeIndexHeader *>(header_guard.GetData());
  return header->root_page_id_ == INVALID_PAGE_ID;
}

auto BPTreeIndex::Size() -> size_t
{
  std::shared_lock<std::shared_mutex> lock(index_latch_);
  auto                                header_guard = buffer_pool_manager_->FetchPageRead(index_id_, FILE_HEADER_PAGE_ID);
  auto header = reinterpret_cast<const BPTreeIndexHeader *>(header_guard.GetData());
  return header->num_entries_;
}

auto BPTreeIndex::GetHeight() -> int
{
  std::shared_lock<std::shared_mutex> lock(index_latch_);
  auto                                header_guard = buffer_pool_manager_->FetchPageRead(index_id_, FILE_HEADER_PAGE_ID);
  auto header = reinterpret_cast<const BPTreeIndexHeader *>(header_guard.GetData());
  return header->tree_height_;
}

}  // namespace njudb
