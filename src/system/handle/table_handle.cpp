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
// Created by ziqi on 2024/7/19.
//

#include "table_handle.h"
namespace njudb {

TableHandle::TableHandle(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, table_id_t table_id,
    TableHeader &hdr, RecordSchemaUptr &schema, StorageModel storage_model)
    : tab_hdr_(hdr),
      table_id_(table_id),
      disk_manager_(disk_manager),
      buffer_pool_manager_(buffer_pool_manager),
      schema_(std::move(schema)),
      storage_model_(storage_model)
{
  // set table id for table handle;
  schema_->SetTableId(table_id_);
  if (storage_model_ == PAX_MODEL) {
    // calculate offsets of fields
    size_t offset = 0;
    for (size_t i = 0; i < schema_->GetFieldCount(); i++) {
        field_offset_.push_back(offset);
        offset += schema_->GetFieldAt(i).field_.field_size_ * tab_hdr_.rec_per_page_;
    }
  }
}

auto TableHandle::GetRecord(const RID &rid) -> RecordUptr
{
  auto nullmap = std::make_unique<char[]>(tab_hdr_.nullmap_size_);
  auto data    = std::make_unique<char[]>(tab_hdr_.rec_size_);
  
  PageHandleUptr page_handle = FetchPageHandle(rid.PageID());
  
  if (!BitMap::GetBit(page_handle->GetBitmap(), rid.SlotID())) {
    buffer_pool_manager_->UnpinPage(table_id_, rid.PageID(), false);
    NJUDB_THROW(NJUDB_RECORD_MISS, "Record not found");
  }
  
  page_handle->ReadSlot(rid.SlotID(), nullmap.get(), data.get());
  
  buffer_pool_manager_->UnpinPage(table_id_, rid.PageID(), false);
  
  return std::make_unique<Record>(schema_.get(), nullmap.get(), data.get(), rid);
}

auto TableHandle::GetChunk(page_id_t pid, const RecordSchema *chunk_schema) -> ChunkUptr { 
  PageHandleUptr page_handle = FetchPageHandle(pid);
  auto chunk = page_handle->ReadChunk(chunk_schema);
  buffer_pool_manager_->UnpinPage(table_id_, pid, false);
  return chunk;
}

auto TableHandle::InsertRecord(const Record &record) -> RID { 
  PageHandleUptr page_handle = CreatePageHandle();
  
  size_t slot_id = BitMap::FindFirst(page_handle->GetBitmap(), tab_hdr_.rec_per_page_, 0, false);
  
  page_handle->WriteSlot(slot_id, record.GetNullMap(), record.GetData(), false);
  
  BitMap::SetBit(page_handle->GetBitmap(), slot_id, true);
  page_handle->GetPage()->SetRecordNum(page_handle->GetPage()->GetRecordNum() + 1);
  
  if (page_handle->GetPage()->GetRecordNum() == tab_hdr_.rec_per_page_) {
    tab_hdr_.first_free_page_ = page_handle->GetPage()->GetNextFreePageId();
    page_handle->GetPage()->SetNextFreePageId(INVALID_PAGE_ID);
  }
  
  RID rid(page_handle->GetPage()->GetPageId(), static_cast<slot_id_t>(slot_id));
  buffer_pool_manager_->UnpinPage(table_id_, page_handle->GetPage()->GetPageId(), true);
  
  return rid;
}

void TableHandle::InsertRecord(const RID &rid, const Record &record)
{
  if (rid.PageID() == INVALID_PAGE_ID) {
    NJUDB_THROW(NJUDB_PAGE_MISS, fmt::format("Page: {}", rid.PageID()));
  }
  
  PageHandleUptr page_handle = FetchPageHandle(rid.PageID());
  
  if (BitMap::GetBit(page_handle->GetBitmap(), rid.SlotID())) {
     buffer_pool_manager_->UnpinPage(table_id_, rid.PageID(), false);
     NJUDB_THROW(NJUDB_RECORD_EXISTS, "Record exists");
  }
  
  page_handle->WriteSlot(rid.SlotID(), record.GetNullMap(), record.GetData(), false);
  
  BitMap::SetBit(page_handle->GetBitmap(), rid.SlotID(), true);
  page_handle->GetPage()->SetRecordNum(page_handle->GetPage()->GetRecordNum() + 1);
  
  if (page_handle->GetPage()->GetRecordNum() == tab_hdr_.rec_per_page_ && tab_hdr_.first_free_page_ == rid.PageID()) {
      tab_hdr_.first_free_page_ = page_handle->GetPage()->GetNextFreePageId();
      page_handle->GetPage()->SetNextFreePageId(INVALID_PAGE_ID);
  }
  
  buffer_pool_manager_->UnpinPage(table_id_, rid.PageID(), true);
}

void TableHandle::DeleteRecord(const RID &rid) { 
  PageHandleUptr page_handle = FetchPageHandle(rid.PageID());
  
  if (!BitMap::GetBit(page_handle->GetBitmap(), rid.SlotID())) {
    buffer_pool_manager_->UnpinPage(table_id_, rid.PageID(), false);
    NJUDB_THROW(NJUDB_RECORD_MISS, "Record missing");
  }
  
  BitMap::SetBit(page_handle->GetBitmap(), rid.SlotID(), false);
  page_handle->GetPage()->SetRecordNum(page_handle->GetPage()->GetRecordNum() - 1);
  
  if (page_handle->GetPage()->GetRecordNum() == tab_hdr_.rec_per_page_ - 1) {
      page_handle->GetPage()->SetNextFreePageId(tab_hdr_.first_free_page_);
      tab_hdr_.first_free_page_ = rid.PageID();
  }
  
  buffer_pool_manager_->UnpinPage(table_id_, rid.PageID(), true);
}

void TableHandle::UpdateRecord(const RID &rid, const Record &record) { 
  PageHandleUptr page_handle = FetchPageHandle(rid.PageID());
  
  if (!BitMap::GetBit(page_handle->GetBitmap(), rid.SlotID())) {
    buffer_pool_manager_->UnpinPage(table_id_, rid.PageID(), false);
    NJUDB_THROW(NJUDB_RECORD_MISS, "Record missing");
  }
  
  page_handle->WriteSlot(rid.SlotID(), record.GetNullMap(), record.GetData(), true);
  
  buffer_pool_manager_->UnpinPage(table_id_, rid.PageID(), true);
}

auto TableHandle::FetchPageHandle(page_id_t page_id) -> PageHandleUptr
{
  auto page = buffer_pool_manager_->FetchPage(table_id_, page_id);
  return WrapPageHandle(page);
}

auto TableHandle::CreatePageHandle() -> PageHandleUptr
{
  if (tab_hdr_.first_free_page_ == INVALID_PAGE_ID) {
    return CreateNewPageHandle();
  }
  auto page = buffer_pool_manager_->FetchPage(table_id_, tab_hdr_.first_free_page_);
  return WrapPageHandle(page);
}

auto TableHandle::CreateNewPageHandle() -> PageHandleUptr
{
  auto page_id = static_cast<page_id_t>(tab_hdr_.page_num_);
  tab_hdr_.page_num_++;
  auto page   = buffer_pool_manager_->FetchPage(table_id_, page_id);
  auto pg_hdl = WrapPageHandle(page);
  page->SetNextFreePageId(tab_hdr_.first_free_page_);
  tab_hdr_.first_free_page_ = page_id;
  return pg_hdl;
}

auto TableHandle::WrapPageHandle(Page *page) -> PageHandleUptr
{
  switch (storage_model_) {
    case StorageModel::NARY_MODEL: return std::make_unique<NAryPageHandle>(&tab_hdr_, page);
    case StorageModel::PAX_MODEL: return std::make_unique<PAXPageHandle>(&tab_hdr_, page, schema_.get(), field_offset_);
    default: NJUDB_FATAL("Unknown storage model");
  }
}

auto TableHandle::GetTableId() const -> table_id_t { return table_id_; }

auto TableHandle::GetTableHeader() const -> const TableHeader & { return tab_hdr_; }

auto TableHandle::GetSchema() const -> const RecordSchema & { return *schema_; }

auto TableHandle::GetTableName() const -> std::string
{
  auto file_name = disk_manager_->GetFileName(table_id_);
  return OBJNAME_FROM_FILENAME(file_name);
}

auto TableHandle::GetStorageModel() const -> StorageModel { return storage_model_; }

auto TableHandle::GetFirstRID() -> RID
{
  auto page_id = FILE_HEADER_PAGE_ID + 1;
  while (page_id < static_cast<page_id_t>(tab_hdr_.page_num_)) {
    auto pg_hdl = FetchPageHandle(page_id);
    auto id     = BitMap::FindFirst(pg_hdl->GetBitmap(), tab_hdr_.rec_per_page_, 0, true);
    if (id != tab_hdr_.rec_per_page_) {
      buffer_pool_manager_->UnpinPage(table_id_, page_id, false);
      return {page_id, static_cast<slot_id_t>(id)};
    }
    buffer_pool_manager_->UnpinPage(table_id_, page_id, false);
    page_id++;
  }
  return INVALID_RID;
}

auto TableHandle::GetNextRID(const RID &rid) -> RID
{
  auto page_id = rid.PageID();
  auto slot_id = rid.SlotID();
  while (page_id < static_cast<page_id_t>(tab_hdr_.page_num_)) {
    auto pg_hdl = FetchPageHandle(page_id);
    slot_id = static_cast<slot_id_t>(BitMap::FindFirst(pg_hdl->GetBitmap(), tab_hdr_.rec_per_page_, slot_id + 1, true));
    if (slot_id == static_cast<slot_id_t>(tab_hdr_.rec_per_page_)) {
      buffer_pool_manager_->UnpinPage(table_id_, page_id, false);
      page_id++;
      slot_id = -1;
    } else {
      buffer_pool_manager_->UnpinPage(table_id_, page_id, false);
      return {page_id, static_cast<slot_id_t>(slot_id)};
    }
  }
  return INVALID_RID;
}

auto TableHandle::HasField(const std::string &field_name) const -> bool
{
  return schema_->HasField(table_id_, field_name);
}

}  // namespace njudb
