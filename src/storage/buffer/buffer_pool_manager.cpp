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
// Created by ziqi on 2024/7/17.
//
#include "buffer_pool_manager.h"
#include "page_guard.h"
#include "replacer/lru_replacer.h"
#include "replacer/lru_k_replacer.h"

#include "../../../common/error.h"

namespace njudb {

BufferPoolManager::BufferPoolManager(DiskManager *disk_manager, njudb::LogManager *log_manager, size_t replacer_lru_k)
    : disk_manager_(disk_manager), log_manager_(log_manager)
{
  if (REPLACER == "LRUReplacer") {
    replacer_ = std::make_unique<LRUReplacer>();
  } else if (REPLACER == "LRUKReplacer") {
    replacer_ = std::make_unique<LRUKReplacer>(replacer_lru_k);
  } else {
    NJUDB_FATAL("Unknown replacer: " + REPLACER);
  }
  // init free_list_
  for (frame_id_t i = 0; i < static_cast<int>(BUFFER_POOL_SIZE); i++) {
    free_list_.push_back(i);
  }
}

auto BufferPoolManager::FetchPage(file_id_t fid, page_id_t pid) -> Page * {
  std::scoped_lock lock(latch_);
  auto iter = page_frame_lookup_.find({fid, pid});
  
  if (iter != page_frame_lookup_.end()) {
    frame_id_t frame_id = iter->second;
    Frame *frame = &frames_[frame_id];
    
    frame->Pin();
    replacer_->Pin(frame_id);
    return frame->GetPage();
  } else {
    try {
      frame_id_t frame_id = GetAvailableFrame();
      UpdateFrame(frame_id, fid, pid);
      return frames_[frame_id].GetPage();
    } catch (const NJUDBException_ &e) {
      return nullptr;
    }
  }
}

auto BufferPoolManager::UnpinPage(file_id_t fid, page_id_t pid, bool is_dirty) -> bool {
  std::scoped_lock lock(latch_);
  
  auto iter = page_frame_lookup_.find({fid, pid});
  if (iter == page_frame_lookup_.end()) {
    return false;
  }
  
  frame_id_t frame_id = iter->second;
  Frame *frame = &frames_[frame_id];
  
  if (frame->GetPinCount() <= 0) {
    return false;
  }
  
  if (is_dirty) {
    frame->SetDirty(true);
  }
  
  frame->Unpin();
  if (frame->GetPinCount() == 0) {
    replacer_->Unpin(frame_id);
  }
  
  return true;
}

auto BufferPoolManager::DeletePage(file_id_t fid, page_id_t pid) -> bool {
  std::scoped_lock lock(latch_);
  
  auto iter = page_frame_lookup_.find({fid, pid});
  if (iter == page_frame_lookup_.end()) {
    return true;
  }
  
  frame_id_t frame_id = iter->second;
  Frame *frame = &frames_[frame_id];
  
  if (frame->GetPinCount() > 0) {
    return false;
  }
  
  if (frame->IsDirty()) {
      disk_manager_->WritePage(fid, pid, frame->GetPage()->GetData());
  }
  
  page_frame_lookup_.erase(iter);
  
  replacer_->Pin(frame_id);

  frame->Reset();
  free_list_.push_back(frame_id);
  
  return true;
}

auto BufferPoolManager::DeleteAllPages(file_id_t fid) -> bool {
  std::scoped_lock lock(latch_);
  
  for (auto it = page_frame_lookup_.begin(); it != page_frame_lookup_.end(); ) {
    if (it->first.fid == fid) {
      frame_id_t frame_id = it->second;
      Frame *frame = &frames_[frame_id];
      
      if (frame->GetPinCount() > 0) {
          return false;
      }
      
      if (frame->IsDirty()) {
          disk_manager_->WritePage(fid, it->first.pid, frame->GetPage()->GetData());
      }
      
      replacer_->Pin(frame_id);
      
      frame->Reset();
      free_list_.push_back(frame_id);
      
      it = page_frame_lookup_.erase(it);
    } else {
      ++it;
    }
  }
  return true;
}

auto BufferPoolManager::FlushPage(file_id_t fid, page_id_t pid) -> bool {
  std::scoped_lock lock(latch_);
  auto iter = page_frame_lookup_.find({fid, pid});
  if (iter == page_frame_lookup_.end()) {
    return false;
  }
  
  frame_id_t frame_id = iter->second;
  Frame *frame = &frames_[frame_id];
  
  if (frame->IsDirty()) {
    disk_manager_->WritePage(fid, pid, frame->GetPage()->GetData());
    frame->SetDirty(false);
  }
  
  return true;
}

auto BufferPoolManager::FlushAllPages(file_id_t fid) -> bool {
  std::scoped_lock lock(latch_);
  for (const auto& pair : page_frame_lookup_) {
    if (pair.first.fid == fid) {
      frame_id_t frame_id = pair.second;
      Frame *frame = &frames_[frame_id];
      if (frame->IsDirty()) {
        disk_manager_->WritePage(fid, pair.first.pid, frame->GetPage()->GetData());
        frame->SetDirty(false);
      }
    }
  }
  return true;
}

auto BufferPoolManager::GetAvailableFrame() -> frame_id_t {
  if (!free_list_.empty()) {
    frame_id_t frame_id = free_list_.front();
    free_list_.pop_front();
    return frame_id;
  }
  
  frame_id_t victim_id;
  if (replacer_->Victim(&victim_id)) {
    Frame *victim_frame = &frames_[victim_id];
    
    if (victim_frame->IsDirty()) {
      disk_manager_->WritePage(victim_frame->GetPage()->GetFileId(), 
                               victim_frame->GetPage()->GetPageId(), 
                               victim_frame->GetPage()->GetData());
      victim_frame->SetDirty(false);
    }
    
    page_frame_lookup_.erase({victim_frame->GetPage()->GetFileId(), victim_frame->GetPage()->GetPageId()});
    victim_frame->Reset();
    
    return victim_id;
  }
  
  throw NJUDBException_(NJUDB_NO_FREE_FRAME, "BufferPoolManager", "GetAvailableFrame", "No free frame available in buffer pool");
}

void BufferPoolManager::UpdateFrame(frame_id_t frame_id, file_id_t fid, page_id_t pid) {
  Frame *frame = &frames_[frame_id];
  
  if (frame->IsDirty()) {
     disk_manager_->WritePage(frame->GetPage()->GetFileId(), frame->GetPage()->GetPageId(), frame->GetPage()->GetData());
  }

  frame->Reset();
  frame->GetPage()->SetFilePageId(fid, pid);
  
  disk_manager_->ReadPage(fid, pid, frame->GetPage()->GetData());
  
  frame->Pin();
  replacer_->Pin(frame_id);
  
  page_frame_lookup_[{fid, pid}] = frame_id;
}

auto BufferPoolManager::GetFrame(file_id_t fid, page_id_t pid) -> Frame *
{
  const auto it = page_frame_lookup_.find({fid, pid});
  return it == page_frame_lookup_.end() ? nullptr : &frames_[it->second];
}

auto BufferPoolManager::FetchPageRead(file_id_t fid, page_id_t pid) -> ReadPageGuard
{
  Page *page = FetchPage(fid, pid);
  return {this, page, fid, pid};
}

auto BufferPoolManager::FetchPageWrite(file_id_t fid, page_id_t pid) -> WritePageGuard
{
  Page *page = FetchPage(fid, pid);
  return {this, page, fid, pid};
}

}  // namespace njudb
