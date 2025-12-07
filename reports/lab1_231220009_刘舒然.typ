// ==========================================
// 1. Template Setup (模版设置 - 不要随意修改)
// ==========================================

#let project(
  title: "",
  student_info: (),
  body
) = {
  // 设置文档元数据
  set document(author: student_info.name, title: title)
  
  // 设置页面尺寸与页边距
  set page(
    paper: "a4",
    margin: (x: 2.5cm, y: 2.5cm),
    numbering: "1 / 1",
  )

  // 设置字体 (中英文混排)
  // 英文使用 Times New Roman，中文优先使用 Source Han Serif SC (思源宋体) 或 SimSun (宋体)
  set text(
    font: ("Times New Roman", "SimSun"),
    size: 12pt,
    lang: "zh"
  )

  // 设置标题样式
  set heading(numbering: "1.1")
  show heading: it => {
    set text(font: ("Arial"), weight: "bold")
    v(0.5em)
    it
    v(0.3em)
  }

  // 设置代码块样式
  show raw.where(block: true): block.with(
    fill: luma(240),
    inset: 10pt,
    radius: 4pt,
    width: 100%,
  )
  // 代码字体
  show raw: set text(font: ("Consolas", "Courier New"), size:12pt)

  // 标题展示
  align(center)[
    #text(20pt, weight: "bold")[#title]
    #v(1em)
  ]

  // === 顶部学生信息表格 (根据实验要求定制) ===
  align(center)[
    #table(
      columns: (1.5fr, 1fr, 3fr, 2fr),
      inset: 8pt,
      align: center + horizon,
      stroke: 1pt + black,
      [*学号*], [*姓名*], [*邮箱*], [*完成题目*],
      [#student_info.id], [#student_info.name], [#student_info.email], [#student_info.tasks]
    )
  ]
  
  v(2em)

  // 正文段落设置：两端对齐，首行缩进
  set par(justify: true, first-line-indent: 2em, leading: 1em)
  // 修复首行缩进对标题的影响
  show heading: it => { set par(first-line-indent: 0em); it }
  
  body
}

// 辅助函数：用于显示测试结果的绿色/红色块
#let test_result(name, passed: true) = {
  let color = if passed { rgb("#e6fffa") } else { rgb("#fff5f5") }
  let stroke_color = if passed { rgb("#319795") } else { rgb("#e53e3e") }
  let text_color = if passed { rgb("#2c7a7b") } else { rgb("#c53030") }
  let icon = if passed { "✅" } else { "❌" }
  
  block(
    fill: color,
    stroke: (left: 4pt + stroke_color),
    inset: 10pt,
    width: 100%,
    radius: 2pt
  )[
    #text(weight: "bold", fill: text_color)[#icon #name 单元测试]
  ]
}

// ==========================================
// 2. Main Document (文档正文 - 在此填空)
// ==========================================

// --- 用户配置区 ---
#show: project.with(
  title: "实验 1: 存储管理实验报告",
  student_info: (
    id: "231220009",                 // 修改为你的学号
    name: "刘舒然",                   // 修改为你的姓名
    email: "231220009@smail.nju.edu.cn", // 修改为你的邮箱
    tasks: "1 / 2 / 3 / f1 / f2"    // 修改你完成的题目
  )
)

// --- 正文开始 ---

#outline(
  title: "目录",
  indent: auto,
  depth: 3
)
#pagebreak()

= 构建和准备

#h(2em)我的实验环境为 `Arch Linux` 系统，内核版本为 `Linux arch 6.17.9-arch1-1 #1 SMP PREEMPT_DYNAMIC Mon, 24 Nov 2025 15:21:09 +0000 x86_64 GNU/Linux`，使用的 GCC 版本为 `gcc (GCC) 15.2.1 20251112`。然而，在首次 `make` 的过程中遇到了问题：
#image("/assets/image.png")
#h(2em)这需要在 `test/storage/replacer_test.cpp` 中添加 `#include <algorithm>` 头文件以解决。

之后，我仔细阅读了源代码，理解了完成实验一主要需要了解的两个数据结构：`DiskManager` 和 `Frame`。其中，`DiskManager` 负责数据库系统与底层文件系统之间的交互，而 `Frame` 则表示代表了缓冲池中的一个物理内存块，是 `BufferPoolManager` 管理的基本单位。

= t1. LRU

== 实现思路

- `Pin`: 如果页面已在链表中，将其标记为 `is_evictable = false`，并移动到链表尾部。如果不在链表中，则新建节点加入尾部
- `Unpin`: 将页面的状态更新为     `is_evictable = true`。如果页面意外不在链表中，则作为新页面加入 MRU。
- `Victim`: 找到第一个 `is_evictable == true` 的页面。将其从链表和哈希表中彻底移除。

== 测试结果
#test_result("ReplacerTest.LRU", passed: true)
#image("/assets/image-1.png")

#line(length: 100%, stroke: 0.5pt + gray)

= t2. Buffer Pool Manager

== 实现思路

- FetchPage:
  - 命中: 如果页面已在 Buffer Pool 中，直接返回对应的 Frame。必须增加引用计数，并调用 `replacer_->Pin()` 通知替换算法该页面正在被使用
  - 未命中:
    1. 调用 `GetAvailableFrame()` 获取一个可用帧
    2. 调用 `UpdateFrame()` 将目标磁盘页加载到该帧中
    3. 返回该页

- GetAvailableFrame: 优先获取空闲帧。若无空闲帧，调用 `replacer_->Victim()` 选择牺牲帧。如果牺牲帧是脏的，必须先调用 `disk_manager_->WritePage()` 将其写回磁盘。最后还要完成清理工作

- UpdateFrame:
  1. *先*调用 frame->Reset() 清除旧状态
  2. 设置新的 fid 和 pid。
  3. 从磁盘读取数据到内存。
  4. 更新映射，Pin 该帧。

- UnpinPage:
  - 减少 Frame 的引用计数。
  - 当参数 is_dirty 为 true 时，将 Frame 标记为脏
  - 当引用计数降为 0 时，调用 `replacer_->Unpin()`，通知替换算法该帧现在可以被淘汰

- FlushPage/DeletePage:
    - FlushPage: 强制将脏页写回磁盘，并清除脏标记
    - DeletePage: 如果页面当前被 Pin 住，则不能删除。删除时需确保脏数据已写回，然后将帧放回 `free_list_`，并从 `page_frame_lookup_` 移除

== 测试结果
#test_result("BufferPoolManagerTest.SimpleTest", passed: true)
#test_result("BufferPoolManagerTest.MultiThread", passed: true)
#image("/assets/image-2.png")

= t3. Table Handle

== 实现思路

- GetRecord:
  1. 通过 rid 中的 PageID 调用 FetchPageHandle 获取页面句柄
  2. 利用页面 Bitmap 检查该 SlotID 是否有效。若无效，抛出 NJUDB_RECORD_MISS
  3. 若有效，调用 PageHandle::ReadSlot 将数据读取到临时的 data 和 nullmap 缓冲区中
  4. 最后，调用 UnpinPage 释放页面，构建并返回 Record 对象

- InsertRecord:
  1. 调用 CreatePageHandle 获取一个有空闲槽位的页面句柄
  2. 利用 BitMap::FindFirst 在该页中找到第一个空闲的 slot_id
  3. 调用 PageHandle::WriteSlot 将记录数据写入该槽位
  4. 更新 Bitmap 和 RecordNum
  5. 若插入后页面变满，需要更新文件头的`first_free_page_` 指针，将其指向该页原本记录的下一空闲页，并将该页移出空闲链表
  6. 调用 UnpinPage 释放页面

- InsertRecord:
  1. 指定位置插入。先获取页面并检查 Bitmap，若目标位置已有数据则抛出异常
  2. 写入数据，更新 Bitmap 和计数
  3. 同样检查页面是否因此变满，若是，则维护空闲链表

- DeleteRecord:
  1. 获取页面，检查 Bitmap 确认记录存在
  2. 更新 Bitmap 和减少记录计数
  3. 若删除前页面是满的，说明该页现在有空位了，将其头插法加入到文件的空闲链表中
  4. 调用 UnpinPage

- UpdateRecord:
  1. 获取页面，确认记录存在
  2. 调用 PageHandle::WriteSlot 覆盖旧数据（
  3. 调用 UnpinPage

== 测试结果
#test_result("TableHandle.Simple", passed: true)
#test_result("TableHandle.MultiThread", passed: true)
#image("/assets/image-3.png")

= f1. LRU K Replacer

== 实现思路

- Pin: 记录一次访问，获取当前全局时间戳    `cur_ts_`，调用 AddHistory 更新节点的访问历史，将页面标记为不可淘汰

- Victim: 遍历所有 is_evictable 的节点
  - 维护 max_dist 和 earliest_ts
  - 若当前节点 $D_k = +infinity$:
    - 如果之前的 max_dist 不是 $+infinity$，说明找到了更优先的淘汰类别，直接更新 $"max_dist" = +infinity$ 并记录当前节点
    - 如果之前的 max_dist 已经是 $+infinity$，则比较两个节点的最早访问时间，保留时间更早的那个
  - 若当前节点 $D_k < +infinity$: 只有当之前的 max_dist 也不是 $+infinity$ 时才进行比较。若当前 $D_k > "max_dist"$，则更新候选

== 测试结果
#test_result("ReplacerTest.LRUK", passed: true)
#image("/assets/image-4.png")

