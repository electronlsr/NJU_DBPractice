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
  title: "实验 4: 索引实验报告",
  student_info: (
    id: "231220009",                 // 修改为你的学号
    name: "刘舒然",                   // 修改为你的姓名
    email: "231220009@smail.nju.edu.cn", // 修改为你的邮箱
    tasks: "t1 / t2 / f1"    // 修改你完成的题目
  )
)

// --- 正文开始 ---

#outline(
  title: "目录",
  indent: auto,
  depth: 3
)
#pagebreak()

= t1. B+ 树索引

== 实现思路

本次实验实现了基于磁盘的 B+ 树索引结构。核心组件包括 `BPTreePage`（页面基类）、`BPTreeLeafPage`（叶子节点）、`BPTreeInternalPage`（内部节点）以及 `BPTreeIndex`（索引管理器）。

1. *页面结构设计*：
   - 所有的 B+ 树节点都继承自 `BPTreePage`，包含通用的元数据（如 `page_id`、`parent_id`、`size` 等）。
   - `BPTreeLeafPage` 存储键值对 `(Key, RID)`，并维护一个 `next_page_id` 指针以支持高效的顺序扫描。
   - `BPTreeInternalPage` 存储 `(Key, PageId)` 对，其中 Key 是子节点的分隔键，PageId 指向子节点。注意内部节点的第 0 个 Key 是无效的，第 0 个 Value 指向第一个子节点。

2. *查找操作 (Find/Search)*：
   - 从根节点开始，如果是内部节点，使用二分查找（或线性查找）找到第一个大于等于目标 Key 的位置，进而递归到对应的子节点。
   - 重复该过程直到到达叶子节点。
   - 在叶子节点中再次查找目标 Key，返回对应的 RID。

3. *插入操作 (Insert)*：
   - 首先找到目标叶子节点。
   - 如果叶子节点未满，直接插入并保持有序。
   - 如果叶子节点已满，则触发 *分裂 (Split)*：
     - 创建一个新的叶子节点。
     - 将原节点的一半数据移动到新节点（`MoveHalfTo`）。
     - 将中间 Key 提升到父节点（`InsertIntoParent`）。
   - 如果父节点也满了，递归向上分裂，直到根节点。根节点分裂会增加树的高度。
   - *优化*：在递归调用 `InsertIntoParent` 之前，尽早释放子节点的 PageGuard，以防止在 BufferPool 较小时发生 `NJUDB_NO_FREE_FRAME` 错误。

4. *删除操作 (Delete)*：
   - 找到包含目标 Key 的叶子节点并删除。
   - 如果删除后节点过小（低于最小填充因子），则触发 *合并或重分配 (CoalesceOrRedistribute)*：
     - 首先尝试从兄弟节点 *借 (Redistribute)* 一个节点。如果兄弟节点有富余，则旋转一个 Key 过来。
     - 如果兄弟节点也无法借出，则与兄弟节点 *合并 (Coalesce)*。
     - 合并会导致父节点删除一个 Key 和指针，可能递归触发父节点的合并。
   - 同样，在递归调用前释放不必要的 PageGuard 锁，避免死锁或资源耗尽。

5. *迭代器 (Iterator)*：
   - 实现了 `BPTreeIterator`，支持 `Begin()` 和 `Begin(key)`。
   - 迭代器维护当前叶子节点的 `page_id` 和槽位 `index`。
   - 当遍历完当前页面的所有元素后，通过 `next_page_id` 跳转到下一个叶子节点，实现全表或范围扫描。

== 遇到的困难及解决

在实现过程中，最大的困难是 *Buffer Pool 资源耗尽 (NJUDB_NO_FREE_FRAME)*。
在 `Insert` 和 `Delete` 操作引发递归分裂或合并时，如果一直持有子节点和父节点的 PageGuard 不释放，当树的高度增加或并发操作较多时，很快就会耗尽缓冲池的所有 Frame。
*解决方案*：在准备进行递归调用（如 `InsertIntoParent` 或 `CoalesceOrRedistribute`）之前，显式调用 `Drop()` 释放当前层级持有的 PageGuard。仅传递必要的 `page_id` 和 `Key` 给递归函数，让递归函数自行重新获取锁。这大大减少了同时锁定的页面数量。

== 测试结果

B+ 树的所有单元测试均通过。

#test_result("BPTreeTest", passed: true)
// #image("/assets/image-6.png") 
