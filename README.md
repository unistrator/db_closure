# 通用多叉树 + 闭包表 SQLite 持久化

一个用纯 Python 实现的**通用多叉树**库，加上一份配套的 **SQLite 持久化方案**（邻接表 + 闭包表 + 索引表三表设计，闭包表由触发器全自动维护）。两个模块都是单文件、零第三方依赖，可以独立使用，也能无缝拼起来：

- [`tree.py`](tree.py) —— 内存多叉树 `Tree` / `TreeNode`
- [`db.py`](db.py) —— SQLite 持久化 `ClosureTreeDB`（与 `tree.py` 双向桥接）
- [`main.py`](main.py) —— 端到端最小示例：建内存树 → 写 SQLite → 读回 → 校验

> 如果只想要一个内存里能用的多叉树，看 [`tree.py`](tree.py) 一个文件就够了。
> 如果还想把树落盘 / 跨进程共享 / 用 SQL 查祖先后代，再加上 [`db.py`](db.py)。

---

## 目录

- [项目结构](#项目结构)
- [环境要求](#环境要求)
- [快速开始](#快速开始)
  - [只用内存树](#只用内存树)
  - [配合 SQLite 持久化](#配合-sqlite-持久化)
- [核心概念](#核心概念)
- [tree.py API 概览](#treepy-api-概览)
  - [构造与基本信息](#构造与基本信息)
  - [增删改查](#增删改查)
  - [遍历](#遍历)
  - [子树操作](#子树操作)
  - [由 dict 一步建树](#由-dict-一步建树)
  - [序列化与持久化](#序列化与持久化)
  - [可视化与 Pythonic 接口](#可视化与-pythonic-接口)
- [db.py API 概览](#dbpy-api-概览)
  - [实例化与生命周期](#实例化与生命周期)
  - [写操作](#写操作)
  - [读操作](#读操作)
  - [tree_index 索引表](#tree_index-索引表)
  - [与 tree.py 双向桥接](#与-treepy-双向桥接)
  - [离线对账](#离线对账)
- [数据库表结构](#数据库表结构)
- [运行内置演示](#运行内置演示)
- [设计要点](#设计要点)
- [深入文档](#深入文档)

---

## 项目结构

```text
tree/
├── tree.py                  # 内存多叉树（TreeNode / Tree）
├── db.py                    # SQLite 闭包表 + 邻接表（ClosureTreeDB）
├── main.py                  # 端到端最小示例
├── docs/
│   ├── 树结构.md                                 # tree.py 设计说明
│   ├── 数据库存储树形结构方案.md                  # 四种方案总览
│   ├── 方案01-邻接表.md / 方案02-路径枚举.md
│   ├── 方案03-嵌套集.md / 方案04-闭包表.md
│   ├── SQLite实施方案-*.md                       # 各方案的 SQLite 实施细节
│   ├── 四方案性能对比.md
│   └── db使用说明.md                             # db.py 的详细使用文档
├── save_tree/               # 运行 demo 时 Tree.save() 默认保存的 JSON 目录
└── README.md
```

---

## 环境要求

- Python **3.9+**（用到 `list[...]` / `dict[...]` 等 PEP 585 写法，源文件已 `from __future__ import annotations`）
- **零第三方依赖**：仅用到标准库 `json` / `sqlite3` / `collections` / `pathlib` / `typing` / `warnings`

直接把需要的源文件拷进项目即可使用：只要内存树就拿 `tree.py`；要持久化就把 `tree.py` + `db.py` 一起带上。

---

## 快速开始

### 只用内存树

```python
from tree import Tree

tree = Tree("1", root_name="根节点")
tree.add("2", parent_value="1", name="左分支")
tree.add("3", parent_value="1", name="右分支")
tree.add("4", parent_value="1")
tree.add("5", parent_value="2")
tree.add("6", parent_value="2")
tree.add("7", parent_value="3")
tree.add("8", parent_value="5")

print(len(tree))                                   # 节点总数
print(tree.height())                               # 树的高度
print("5" in tree)                                 # 成员判断
print([n.value for n in tree.bfs()])               # BFS
print([n.value for n in tree.dfs_preorder()])      # DFS 先序
print([n.value for n in tree.dfs_postorder()])     # DFS 后序
print([n.value for n in tree.traverse_by_order()]) # 按 order ↑ / level ↓

path = tree.save()                                 # 保存为 save_tree/<根节点 name>.json
loaded = Tree.load("根节点")                        # 按根节点 name 加载
```

### 配合 SQLite 持久化

```python
from tree import Tree
from db import ClosureTreeDB

tree = Tree("1", root_name="根节点")
tree.add("2", parent_value="1", name="左分支")
tree.add("3", parent_value="1", name="右分支")
tree.add("5", parent_value="2")
tree.add("8", parent_value="5")

with ClosureTreeDB("tree.db") as db:
    db.save_tree(tree, tree_id=1)                 # 写入；闭包表由触发器自动维护
    assert db.verify(tree_id=1)                   # 离线对账：闭包表与邻接表一致

    root = db.get_root(tree_id=1)
    print([n["name"] for n in db.get_subtree(root["id"])])   # 整棵子树
    print([n["name"] for n in db.get_ancestors(root["id"])]) # 祖先链

    found_tid = db.find_tree_id("根节点")          # 按根名反查 tree_id
    rebuilt = db.load_tree(tree_id=found_tid)     # 还原为内存 Tree
```

直接运行端到端示例（会在当前目录生成 `tree.db`）：

```bash
python main.py
```

---

## 核心概念

| 概念 | 说明 |
| --- | --- |
| `value` | 节点的**值**；`add` / `find` / `remove` 等默认以它为键 |
| `name` | 节点的**名字**；默认等于 `value`，用于可视化和 `Tree.save` 的文件名、`db.py` 的 `tree_index.root_name` |
| `level` | 节点的**层级**：根节点为 `0`，每向下一层 +1；由内置方法自动维护 |
| `order` | 节点的**顺序编号**：叶子按 DFS 先序全局从 `0` 起编号；非叶取所有子节点 `order` 的最大值。叶子若 `value` 是 dict 且含 `"order"` 键，则直接同步该值 |
| `tree_id` | DB 侧的**树编号**：一棵 `tree_id` 对应一棵树（"一棵 tree_id 一个根"是核心不变量） |
| 空树 | `Tree()` 构造出 `self.root is None` 的对象，仍是合法 `Tree` 实例 |

> `level` / `order` 在 `Tree` 的内置方法里会自动刷新，正常情况下**不需要手动维护**。只有在外部绕开封装直接改动 `parent` / `children` 时，才需要 `tree.refresh_levels()` / `tree.refresh_orders()`。
>
> DB 侧 `node_order` 与 `TreeNode.order` **同语义**，由 `_recalc_order(tree_id)` 在所有结构变更后整棵刷新；闭包表 `tree_closure` 由触发器全自动维护，业务代码不用碰。

---

## tree.py API 概览

所有 API 均挂在 `Tree` 上，节点对象本身是 `TreeNode`。

### 构造与基本信息

```python
Tree()                          # 空树
Tree("root")                    # 直接给定根节点的 value
Tree("root", root_name="根")    # 同时指定 name

tree.is_empty()
tree.size()                     # 等价于 len(tree)
tree.height()                   # 空树为 -1，单节点为 0
tree.depth(node)                # 等价于 node.level
tree.refresh_levels()
tree.refresh_orders()
```

### 增删改查

```python
tree.add(value, parent_value=None, *, name=None)

tree.remove(value)                                   # 删首个匹配（连同其子树）
tree.remove_all(value=None, *, predicate=None)       # 批量删，返回实际删除的子树数

tree.update_value(value, new_value)
tree.update_value_all(value=None, new_value=None, *, predicate=None)
tree.update_name(value, new_name)
tree.update_name_all(value=None, new_name=None, *, predicate=None)

tree.find(value=None, *, predicate=None)             # BFS，返回首个匹配
tree.find_all(value=None, *, predicate=None)         # 返回所有匹配（BFS 顺序）
```

所有 `*_all` 和 `find*` 都支持两种查询方式：

- **按值**：直接传 `value`
- **按谓词**：传 `predicate=lambda n: ...`，可以写任意复杂条件（例如 `n.is_leaf()`）

> `add` / `remove` / `update_*` 等方法会通过 `_MISSING` 哨兵区分"没传参"和"显式传 None"，因此 `parent_value=None` / `value=None` 都是合法的查询键，可在 `value=None` 的节点下继续 `add` 子节点。

### 遍历

```python
tree.bfs(start=None)                   # 广度优先
tree.dfs_preorder(start=None)          # 深度优先 · 先序（迭代实现）
tree.dfs_postorder(start=None)         # 深度优先 · 后序（迭代实现）
tree.traverse_by_order(start=None)     # 按 (order ↑, level ↓) 遍历
```

### 子树操作

```python
tree.deep_copy_subtree(value=None, *, predicate=None)      # 返回独立的新 Tree
tree.shallow_copy_subtree(value=None, *, predicate=None)   # 新 Tree 与原树共享节点

tree.replace_with_subtree(
    value=None,
    subtree=<Tree | TreeNode>,
    *,
    predicate=None,
    deep_copy=True,
)

tree.insert_parent(value, target_value=None, *, name=None, predicate=None)
```

- `deep_copy_subtree`：结构完全独立，修改新树不影响原树。
- `shallow_copy_subtree`：新 `Tree` 只是一层壳，**内部节点与原树共享**；适合把某棵子树当独立树调用遍历 / 保存等方法而不想付拷贝成本。注意此时新树各节点的 `level` / `order` 保留的是**原树视角**的值。
- `replace_with_subtree`：把匹配到的第一个节点（连同其原有子树）整体替换为给定子树；`deep_copy=False` 时会把源节点从原位置整体摘下避免脏状态。
- `insert_parent`：在指定节点之上"套一层壳"，把新节点插入到目标节点与其原父节点之间；不传 `target_value` 时默认在根之上提升一层。

### 由 dict 一步建树

```python
Tree.from_subtrees(root_value, subtrees, *, root_name=None, deep_copy=True)
Tree.nodes_from_dict({"a": 1, "b": 2})                  # → 一组独立的叶子 TreeNode
Tree.tree_from_dict("R", {"a": 1, "b": 2}, root_name="根")  # → 一棵以 R 为根、a/b 为孩子的新树
```

- `from_subtrees`：把若干散落的 `Tree` / `TreeNode` 戴上一个共同的根组成新树；`deep_copy=False` 时会把源节点直接摘走、源 `Tree` 置空。
- `nodes_from_dict`：把扁平 dict 转成一组叶子节点（`key → name`、`value → value`），不递归展开。
- `tree_from_dict`：上面两步的便捷组合，一行直接得到一棵新树。

### 序列化与持久化

```python
tree.to_dict(node=None)                                # 返回嵌套 dict
Tree.from_dict(data)                                   # 反序列化

tree.save(directory="save_tree", *, overwrite=True)    # 写入 <directory>/<根节点 name>.json
Tree.load(path, *, directory="save_tree")              # 按路径 / 文件名 / 根节点名加载
```

- `save` 以根节点的 `name` 作为文件名；`name` 中不允许出现在文件名里的字符会被替换为 `_`，清理后为空时回退为 `"unknown_tree"`。
- `overwrite=False` 时遇到同名文件会抛 `FileExistsError`，避免误覆盖。
- `load` 的 `path` 支持三种写法：完整路径 / 仅文件名 / 仅根节点名（自动补 `.json` 并在 `directory` 下查找）。
- 导出的 JSON 里包含的 `level` / `order` 字段**仅供展示**，`from_dict` 会忽略它们并由结构重新推导，所以手工编辑 JSON 无需担心写错这两个值。

### 可视化与 Pythonic 接口

```python
str(tree)           # 等价于 tree.to_str()，以缩进的 JSON 文本形式展示
repr(tree)          # 形如 Tree(size=..., height=...)
len(tree)           # 节点总数
for v in tree: ...  # 按 DFS 先序迭代出每个节点的 value
"5" in tree         # 按 value 成员判断
```

---

## db.py API 概览

`db.py` 只暴露一个核心类 `ClosureTreeDB`，它在 SQLite 之上同时维护**邻接表**（真值）和**闭包表**（祖先-后代派生索引），并通过触发器把闭包表与人类可读的"根名 → tree_id"索引表全部自动同步。

### 实例化与生命周期

```python
from db import ClosureTreeDB

with ClosureTreeDB("tree.db") as db:                 # 文件库
    ...

with ClosureTreeDB(":memory:") as db:                # 进程内内存库（默认）
    ...
```

- 构造时会自动 `CREATE TABLE IF NOT EXISTS` + 注册触发器，可重复打开同一文件。
- 启用了 `PRAGMA foreign_keys = ON`，外键 `ON DELETE CASCADE` 会把 `tree_closure` / 子节点一并清理。
- 与 `sqlite3.Connection` 一致：**默认不允许跨线程使用**。

### 写操作

```python
db.add_root(tree_id, name, payload=None) -> int       # 新建一棵树的根
db.add_node(parent_id, name, payload=None) -> int     # 在父节点下挂新孩子，tree_id 自动继承
db.remove_subtree(node_id) -> int                     # 删除节点 + 所有后代
db.remove_node(node_id) -> bool                       # 仅删除节点本身，孩子上提到祖父
db.move_subtree(node_id, new_parent_id) -> None       # 移动子树；new_parent_id=None 表示提升为根
```

强制不变量（违反时抛 `ValueError`）：

- **一棵 `tree_id` 只能有一个根**。
- **根名（`tree_index.root_name`）全库唯一**。
- 不允许跨树移动子树（源 `tree_id` 必须等于目标父节点的 `tree_id`）。
- 不允许移动到自己或自己的后代下（防环）。
- `payload` 必须是可 JSON 序列化的对象（不可序列化时抛 `TypeError`，避免静默丢类型）。

### 读操作

```python
db.get_node(node_id)                                  # 单节点
db.get_subtree(node_id, *, include_self=True)         # 整棵子树（depth ↑、同层按 sibling_order）
db.get_ancestors(node_id)                             # 祖先链（从根到父）
db.get_children(node_id)                              # 直接孩子（按 sibling_order）
db.get_depth(node_id)                                 # 节点深度
db.get_subtree_height(node_id)                        # 子树最大深度
db.get_root(tree_id)                                  # 取某棵树的根
db.is_ancestor(ancestor_id, descendant_id)            # O(1) 主键命中（闭包表的标志性优势）
```

返回的节点都是 `dict`，已经把 `payload` 反序列化好；闭包表查询全部走索引，"祖先 / 后代 / 是否祖先"等问题都是常数时间的索引命中，不用递归 CTE。

### tree_index 索引表

```python
db.list_trees()                                       # 列出全库所有树
db.find_tree_id(root_name) -> Optional[int]           # 按根名反查 tree_id
db.rename_tree(tree_id, new_root_name) -> bool        # 改根名；tree_index 由触发器自动同步
```

`tree_index` 是给业务侧用的"人类可读索引"，让上层不用再维护"哪棵 tree_id 是哪棵树"的字典；新增根、删根、改根名、把节点提升为根 / 降级为非根，全部由触发器同步。

### 与 tree.py 双向桥接

```python
db.save_tree(tree, tree_id, *, overwrite=False) -> dict[int, int]
db.load_tree(tree_id=None, *, tree_name=None) -> Tree
```

- `save_tree`：把内存 `Tree` 写入数据库，按 `children` 列表下标写 `sibling_order`，闭包表由 INSERT 触发器自动维护。同 `tree_id` 已有数据时 `overwrite=False` 抛错（避免静默追加出双根），`overwrite=True` 会先清掉同 `tree_id` 的旧数据再写。返回 `{id(内存节点): 数据库 id}` 映射。
- `load_tree`：从数据库重建内存 `Tree`。可同时按 `tree_id` 或 `tree_name` 定位（同时传入时 `tree_name` 优先）；找不到统一返回空树。重建只用邻接表，节点的 `name` / `value` / `order` 全部从 DB 取，`level` 由结构 BFS 重新推导。
- 子节点顺序在 `save_tree` ↔ `load_tree` 间**无损往返**。

### 离线对账

```python
db.verify(tree_id) -> bool
```

用递归 CTE 重算"应该有的闭包"，与 `tree_closure` 做对比；返回 `True` 表示触发器维护正确。生产环境可定期跑一次作为完整性检查。

---

## 数据库表结构

```text
tree_node                               -- 邻接表：真值来源
├── id            INTEGER PK AUTOINC
├── tree_id       INTEGER NOT NULL
├── parent_id     INTEGER NULL  → tree_node(id) ON DELETE CASCADE
├── name          TEXT    NOT NULL
├── payload       TEXT             -- JSON 文本（任意可序列化对象）
├── sibling_order INTEGER NOT NULL -- 同父亲下的相对顺序
├── node_order    INTEGER NOT NULL -- 与 TreeNode.order 同语义，由 _recalc_order 整棵刷新
└── created_at    TEXT    NOT NULL DEFAULT (datetime('now'))

tree_closure                            -- 闭包表：祖先 ↔ 后代派生索引
├── ancestor    INTEGER → tree_node(id) ON DELETE CASCADE
├── descendant  INTEGER → tree_node(id) ON DELETE CASCADE
├── depth       INTEGER >= 0
└── PRIMARY KEY (ancestor, descendant)

tree_index                              -- 人类可读索引：tree_id ↔ 根名
├── tree_id    INTEGER PRIMARY KEY
├── root_name  TEXT    NOT NULL UNIQUE
└── updated_at TEXT    NOT NULL DEFAULT (datetime('now'))
```

由 6 个触发器自动维护：

1. **插入节点** → 写入自反闭包行 + 把父亲所有祖先连到新节点。
2. **更新 `parent_id`**（移动子树）→ 拆"子树外祖先 → 子树内任意节点"的旧关系，写"新祖先链 × 子树内所有节点"的新关系。
3. **插入根节点** → `tree_index` UPSERT 一行。
4. **更新根节点 `name`** → 同步 `tree_index.root_name`。
5. **删除根节点** → `tree_index` 改指向剩余编号最小的根，全删则清掉索引行。
6. **`parent_id` 变化导致"是否为根"切换** → 同步 `tree_index`。

---

## 运行内置演示

每个文件都自带一个 `__main__` 演示，可以直接执行。

```bash
# 内存树：构造 / 遍历 / 保存加载 / 批量改值 / 深浅拷贝 / 子树替换 / from_subtrees ……
python tree.py

# SQLite 闭包表：建库 → 写节点 → 读 → 移动 → tree.py 互转 → verify 对账
python db.py

# 端到端最小示例：内存建树 → 写入 SQLite → 读回 → 一致性断言
python main.py
```

`tree.py` / `main.py` 运行后会在当前目录下生成 `save_tree/` 文件夹，把示范用到的几棵树保存成 JSON；`db.py` 默认在 `:memory:` 库里跑，不会落盘；`main.py` 会在当前目录生成 `tree.db`，可用任意 SQLite 客户端（如 DB Browser for SQLite）打开查看 `tree_node` / `tree_closure` / `tree_index` 三张表。

---

## 设计要点

**`tree.py`：**

- **两个类分工清晰**：`TreeNode` 只关心"我是谁 / 父母是谁 / 有哪些孩子"；`Tree` 只持有 `root`，作为对外门面。
- **结构 = 对象 + 引用**：父子关系通过 `parent` 指针 + `children` 列表双向维护，没有任何外部索引表。
- **双向引用的不变量**：任何改结构的操作都必须同时维护 `parent` 和 `children`；所有内置方法都会帮你处理好，并自动刷新 `level` / `order`。
- **深树友好**：`height`、DFS 前 / 后序、`to_dict`、`from_dict`、`_refresh_levels`、`_refresh_orders`、`_deep_copy_node` 等全部改成**迭代实现**（显式栈 / 队列），即使树非常深也不会触发 Python 递归栈上限。
- **空树是合法的一等公民**：`Tree()` 与 `Tree.from_dict(None)` 都返回 `root is None` 的空 `Tree`，边界判断只看 `self.root is None` 即可。

**`db.py`：**

- **邻接表是真值，闭包表是派生索引**：业务代码只动 `tree_node`，`tree_closure` 全靠触发器自动维护；想做完整性检查随时 `verify()`。
- **`tree_index` 让上层不用记 `tree_id`**：按业务名字（根名）反查 `tree_id` 即可。
- **强不变量优先于灵活性**：`add_root` / `move_subtree` / `save_tree` 都会主动检查"一棵 tree_id 一个根"和"根名全库唯一"，把脏状态挡在门外。
- **`node_order` 复用内存树算法**：`_recalc_order` 直接用 `tree.py` 的 `_refresh_orders` 算，避免在 SQL 里重写"叶子 DFS 全局编号 + 非叶取 max" 这套规则，保证两边语义完全一致。
- **跨树操作显式拒绝**：跨 `tree_id` 移动会让父子 `tree_id` 不一致，目前不支持自动同步，直接抛 `ValueError`。

---

## 深入文档

- 内存树设计 ▸ [`docs/树结构.md`](docs/%E6%A0%91%E7%BB%93%E6%9E%84.md)
- SQLite 持久化使用说明 ▸ [`docs/db使用说明.md`](docs/db%E4%BD%BF%E7%94%A8%E8%AF%B4%E6%98%8E.md)
- 数据库存树四种方案对比 ▸ [`docs/数据库存储树形结构方案.md`](docs/%E6%95%B0%E6%8D%AE%E5%BA%93%E5%AD%98%E5%82%A8%E6%A0%91%E5%BD%A2%E7%BB%93%E6%9E%84%E6%96%B9%E6%A1%88.md)
  - [`方案01-邻接表.md`](docs/%E6%96%B9%E6%A1%8801-%E9%82%BB%E6%8E%A5%E8%A1%A8.md) · [`方案02-路径枚举.md`](docs/%E6%96%B9%E6%A1%8802-%E8%B7%AF%E5%BE%84%E6%9E%9A%E4%B8%BE.md) · [`方案03-嵌套集.md`](docs/%E6%96%B9%E6%A1%8803-%E5%B5%8C%E5%A5%97%E9%9B%86.md) · [`方案04-闭包表.md`](docs/%E6%96%B9%E6%A1%8804-%E9%97%AD%E5%8C%85%E8%A1%A8.md)
  - 各方案 SQLite 实施 ▸ [`SQLite实施方案-邻接表.md`](docs/SQLite%E5%AE%9E%E6%96%BD%E6%96%B9%E6%A1%88-%E9%82%BB%E6%8E%A5%E8%A1%A8.md) · [`-路径枚举.md`](docs/SQLite%E5%AE%9E%E6%96%BD%E6%96%B9%E6%A1%88-%E8%B7%AF%E5%BE%84%E6%9E%9A%E4%B8%BE.md) · [`-嵌套集.md`](docs/SQLite%E5%AE%9E%E6%96%BD%E6%96%B9%E6%A1%88-%E5%B5%8C%E5%A5%97%E9%9B%86.md) · [`-触发器自动维护的闭包表.md`](docs/SQLite%E5%AE%9E%E6%96%BD%E6%96%B9%E6%A1%88-%E8%A7%A6%E5%8F%91%E5%99%A8%E8%87%AA%E5%8A%A8%E7%BB%B4%E6%8A%A4%E7%9A%84%E9%97%AD%E5%8C%85%E8%A1%A8.md)
- [`docs/四方案性能对比.md`](docs/%E5%9B%9B%E6%96%B9%E6%A1%88%E6%80%A7%E8%83%BD%E5%AF%B9%E6%AF%94.md)
