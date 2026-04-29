"""闭包表方案的 SQLite 实现。

对应 ``docs/方案04-闭包表.md`` 的设计：

- ``tree_node``（邻接表）是真值来源，业务代码只动它；
- ``tree_closure``（祖先-后代派生索引）由触发器全自动维护：

  * ``AFTER INSERT ON tree_node``        → 写入自反 + 父亲祖先链
  * ``AFTER UPDATE OF parent_id``        → 移动子树时拆旧连接、重连新连接
  * ``ON DELETE CASCADE``                → 删节点时连带清理闭包行

- 与 :mod:`tree` 的内存模型双向打通：``save_tree`` / ``load_tree``；
- 提供 :meth:`ClosureTreeDB.verify` 离线对账（递归 CTE 重算闭包做对比）。

设计要点和读 / 写 SQL 的语义说明，参考方案文档第 3、4、6 节。
"""

from __future__ import annotations

import json
import sqlite3
import warnings
from collections import deque
from pathlib import Path
from typing import Any, Optional

from tree import Tree, TreeNode


# ============ DDL ============

_SCHEMA_SQL = """
-- ⚠ 本 schema 不向后兼容历史版本：tree_node 新增 sibling_order 列、
-- tree_index.root_name 新增 UNIQUE 约束。如有旧 .db 文件请重建。
CREATE TABLE IF NOT EXISTS tree_node (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    tree_id       INTEGER NOT NULL,
    parent_id     INTEGER NULL,
    name          TEXT    NOT NULL,
    payload       TEXT,                                       -- JSON 文本
    sibling_order INTEGER NOT NULL DEFAULT 0,                 -- 同父亲下的相对顺序
    node_order    INTEGER NOT NULL DEFAULT 0,                 -- 派生：与 tree.py 的
                                                              -- TreeNode.order 同语义；
                                                              -- 由 _recalc_order(tree_id)
                                                              -- 在所有结构变更后整棵刷新
    created_at    TEXT    NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (parent_id) REFERENCES tree_node(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_tree_parent
    ON tree_node(tree_id, parent_id);
CREATE INDEX IF NOT EXISTS idx_tree_sibling
    ON tree_node(parent_id, sibling_order);

CREATE TABLE IF NOT EXISTS tree_closure (
    ancestor   INTEGER NOT NULL,
    descendant INTEGER NOT NULL,
    depth      INTEGER NOT NULL CHECK (depth >= 0),
    PRIMARY KEY (ancestor, descendant),
    FOREIGN KEY (ancestor)   REFERENCES tree_node(id) ON DELETE CASCADE,
    FOREIGN KEY (descendant) REFERENCES tree_node(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_closure_descendant
    ON tree_closure(descendant);
CREATE INDEX IF NOT EXISTS idx_closure_depth
    ON tree_closure(descendant, depth);

-- tree_index：tree_id ↔ 根节点名 的关联表（人类可读索引）。
-- 由下方触发器全自动维护：新增根、根改名、删根都会同步。
-- root_name UNIQUE：强制"按根名能唯一定位一棵树"；写入端会先做友好检查
-- 再插入，所以这里的 UNIQUE 主要做兜底。
CREATE TABLE IF NOT EXISTS tree_index (
    tree_id    INTEGER PRIMARY KEY,
    root_name  TEXT    NOT NULL UNIQUE,
    updated_at TEXT    NOT NULL DEFAULT (datetime('now'))
);
"""


# ============ 触发器 ============

_TRIGGERS_SQL = """
-- 1) 插入节点：写入自反记录 + 把父亲所有祖先连到新节点
CREATE TRIGGER IF NOT EXISTS trg_tree_node_after_insert
AFTER INSERT ON tree_node
FOR EACH ROW
BEGIN
    INSERT INTO tree_closure(ancestor, descendant, depth)
        VALUES (NEW.id, NEW.id, 0);

    INSERT INTO tree_closure(ancestor, descendant, depth)
        SELECT c.ancestor, NEW.id, c.depth + 1
        FROM   tree_closure c
        WHERE  c.descendant = NEW.parent_id;
END;

-- 2) 移动子树：UPDATE parent_id 时整段拆旧 + 重连
--    实质就是文档 4.5 节的 SQL，只是把 :n / :m 换成 NEW.id / NEW.parent_id。
CREATE TRIGGER IF NOT EXISTS trg_tree_node_after_move
AFTER UPDATE OF parent_id ON tree_node
FOR EACH ROW
WHEN OLD.parent_id IS NOT NEW.parent_id
BEGIN
    -- 2.1 删"子树外祖先 → 子树内任意节点"的旧关系
    DELETE FROM tree_closure
    WHERE descendant IN (
        SELECT descendant FROM tree_closure WHERE ancestor = NEW.id
    )
    AND ancestor NOT IN (
        SELECT descendant FROM tree_closure WHERE ancestor = NEW.id
    );

    -- 2.2 写"新祖先链 × 子树内所有节点"的新关系
    --     这里语义就是笛卡尔积，写成显式 CROSS JOIN 比"JOIN 不带 ON"更清晰。
    INSERT INTO tree_closure(ancestor, descendant, depth)
        SELECT s.ancestor,
               d.descendant,
               s.depth + d.depth + 1
        FROM       tree_closure s
        CROSS JOIN tree_closure d
        WHERE  s.descendant = NEW.parent_id
          AND  d.ancestor   = NEW.id;
END;

-- 3) tree_index 维护：新增根节点 → 写入 / 覆盖索引行。
--    parent_id IS NULL 的节点即根节点；用 UPSERT 让 save_tree 这种
--    "先 DELETE 再 INSERT" 的覆盖式写入也能正确刷新 root_name。
CREATE TRIGGER IF NOT EXISTS trg_tree_index_after_root_insert
AFTER INSERT ON tree_node
FOR EACH ROW
WHEN NEW.parent_id IS NULL
BEGIN
    INSERT INTO tree_index(tree_id, root_name)
        VALUES (NEW.tree_id, NEW.name)
    ON CONFLICT(tree_id) DO UPDATE
        SET root_name  = excluded.root_name,
            updated_at = datetime('now');
END;

-- 4) tree_index 维护：根节点改名 → 同步索引行。
CREATE TRIGGER IF NOT EXISTS trg_tree_index_after_root_rename
AFTER UPDATE OF name ON tree_node
FOR EACH ROW
WHEN NEW.parent_id IS NULL
BEGIN
    UPDATE tree_index
       SET root_name  = NEW.name,
           updated_at = datetime('now')
     WHERE tree_id = NEW.tree_id;
END;

-- 5) tree_index 维护：根节点被删 → 索引行回退到"剩余编号最小的根"，
--    若该 tree_id 下已无任何根则删除索引行。
--    非根节点删除会被 WHEN 拦掉，所以 ON DELETE CASCADE 把子孙带下来时
--    不会触发这条逻辑，性能上等于 O(根数量) 而非 O(节点总数)。
CREATE TRIGGER IF NOT EXISTS trg_tree_index_after_root_delete
AFTER DELETE ON tree_node
FOR EACH ROW
WHEN OLD.parent_id IS NULL
BEGIN
    -- 还有别的根：tree_index 改指向编号最小的剩余根。
    UPDATE tree_index
       SET root_name  = (
               SELECT name FROM tree_node
               WHERE tree_id = OLD.tree_id AND parent_id IS NULL
               ORDER BY id LIMIT 1
           ),
           updated_at = datetime('now')
     WHERE tree_id = OLD.tree_id
       AND EXISTS (
           SELECT 1 FROM tree_node
           WHERE tree_id = OLD.tree_id AND parent_id IS NULL
       );
    -- 没有任何根：清掉索引行。
    DELETE FROM tree_index
     WHERE tree_id = OLD.tree_id
       AND NOT EXISTS (
           SELECT 1 FROM tree_node
           WHERE tree_id = OLD.tree_id AND parent_id IS NULL
       );
END;

-- 6) tree_index 维护：UPDATE parent_id 改变了节点"是否为根"时同步索引。
--    覆盖两种情形：
--      a) 非根 → 根（提升）：UPSERT 索引行指向编号最小的根
--      b) 根 → 非根（降级）：若还有其他根 UPSERT 指向编号最小的，否则删行
--    ⚠ 配合代码层"一棵 tree_id 一个根"的不变量使用：写入端会拒绝
--    重复根，所以"提升时已有别的根"这种脏状态实际进不来。
CREATE TRIGGER IF NOT EXISTS trg_tree_index_after_parent_change
AFTER UPDATE OF parent_id ON tree_node
FOR EACH ROW
WHEN (OLD.parent_id IS NULL AND NEW.parent_id IS NOT NULL)
  OR (OLD.parent_id IS NOT NULL AND NEW.parent_id IS NULL)
BEGIN
    INSERT INTO tree_index(tree_id, root_name)
        SELECT NEW.tree_id, name
        FROM   tree_node
        WHERE  tree_id = NEW.tree_id AND parent_id IS NULL
        ORDER BY id LIMIT 1
    ON CONFLICT(tree_id) DO UPDATE
        SET root_name  = excluded.root_name,
            updated_at = datetime('now');
    DELETE FROM tree_index
     WHERE tree_id = NEW.tree_id
       AND NOT EXISTS (
           SELECT 1 FROM tree_node
           WHERE tree_id = NEW.tree_id AND parent_id IS NULL
       );
END;
"""


# ============ 主类 ============


class ClosureTreeDB:
    """SQLite 上的闭包表 + 邻接表。

    用法::

        with ClosureTreeDB("tree.db") as db:
            root = db.add_root(tree_id=1, name="根")
            a = db.add_node(parent_id=root, name="A")
            b = db.add_node(parent_id=a, name="B")
            print(db.get_subtree(root))                  # 整棵子树
            print(db.is_ancestor(root, b))               # True
            db.move_subtree(b, new_parent_id=root)       # b 上提到根下
            assert db.verify(tree_id=1)                  # 触发器维护正确

    线程安全说明：``sqlite3.Connection`` 默认不允许跨线程使用；本类同样。
    """

    def __init__(self, db_path: str | Path = ":memory:") -> None:
        self.db_path = ":memory:" if str(db_path) == ":memory:" else str(Path(db_path))
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self._init_schema()

    def _init_schema(self) -> None:
        with self.conn:
            self.conn.executescript(_SCHEMA_SQL)
            self.conn.executescript(_TRIGGERS_SQL)

    # ---------- 上下文管理 ----------

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "ClosureTreeDB":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    # ============ 写操作 ============

    def add_root(self, tree_id: int, name: Any, payload: Any = None) -> int:
        """新增一棵树的根节点，返回数据库自增 id。

        强制不变量：

        - **一棵 tree_id 只能有一个根**——重复 ``tree_id`` 抛 :class:`ValueError`；
        - **根节点名（``tree_index.root_name``）全库唯一**——重名抛
          :class:`ValueError`，避免后续按名反查时多匹配。

        想替换已存在的树请先 :meth:`remove_subtree` 旧根，或改用
        :meth:`save_tree` ``overwrite=True``。
        """
        with self.conn:
            existing_root = self.conn.execute(
                "SELECT id FROM tree_node "
                "WHERE tree_id = ? AND parent_id IS NULL LIMIT 1",
                (tree_id,),
            ).fetchone()
            if existing_root is not None:
                raise ValueError(
                    f"tree_id={tree_id} 已存在根节点 (id={existing_root['id']})；"
                    "请先 remove_subtree 旧根，或用 save_tree(..., overwrite=True)"
                )
            self._ensure_root_name_available(str(name))
            cur = self.conn.execute(
                "INSERT INTO tree_node"
                "(tree_id, parent_id, name, payload, sibling_order, node_order) "
                "VALUES (?, NULL, ?, ?, 0, 0)",
                (tree_id, str(name), self._dump(payload)),
            )
        return int(cur.lastrowid)

    def _ensure_root_name_available(
        self, name: str, *, ignore_tree_id: Optional[int] = None
    ) -> None:
        """若另一棵树已经叫 ``name``，抛 :class:`ValueError`。

        ``ignore_tree_id`` 用于"改名为自己当前的名字"或 overwrite 同 tree_id 的
        场景，跳过自己这一行。
        """
        if ignore_tree_id is None:
            row = self.conn.execute(
                "SELECT tree_id FROM tree_index WHERE root_name = ? LIMIT 1",
                (name,),
            ).fetchone()
        else:
            row = self.conn.execute(
                "SELECT tree_id FROM tree_index "
                "WHERE root_name = ? AND tree_id != ? LIMIT 1",
                (name, ignore_tree_id),
            ).fetchone()
        if row is not None:
            raise ValueError(
                f"根节点名 {name!r} 已被 tree_id={row['tree_id']} 占用；"
                "根名全库唯一——请改名或先删掉占用方"
            )

    def add_node(self, parent_id: int, name: Any, payload: Any = None) -> int:
        """在 ``parent_id`` 下挂一个新节点，返回新 id；``tree_id`` 自动继承父节点。

        新节点的 ``sibling_order`` 自动填为"父亲下当前最大兄弟序 + 1"，
        保证 :meth:`save_tree` ↔ :meth:`load_tree` 的子节点顺序无损往返。

        ``parent_id`` 不能为 ``None``——建根请用 :meth:`add_root`，参数语义
        不同（一个继承父亲的 ``tree_id``，另一个开新树）。
        """
        if parent_id is None:
            raise ValueError("add_node 不能用于建根；请使用 add_root(tree_id, ...)")
        with self.conn:
            row = self.conn.execute(
                "SELECT tree_id FROM tree_node WHERE id = ?", (parent_id,)
            ).fetchone()
            if row is None:
                raise ValueError(f"父节点不存在: {parent_id}")
            cur = self.conn.execute(
                "INSERT INTO tree_node"
                "(tree_id, parent_id, name, payload, sibling_order) "
                "VALUES (?, ?, ?, ?, "
                "        (SELECT COALESCE(MAX(sibling_order), -1) + 1 "
                "         FROM tree_node WHERE parent_id = ?))",
                (row["tree_id"], parent_id, str(name), self._dump(payload),
                 parent_id),
            )
            self._recalc_order(int(row["tree_id"]))
        return int(cur.lastrowid)

    def remove_subtree(self, node_id: int) -> int:
        """删除 ``node_id`` 及其所有后代，返回实际删除的节点数。

        利用闭包表一次列出所有后代，再让外键 ``ON DELETE CASCADE`` 自动清理闭包行；
        最后整棵刷新一次 ``node_order``（被删的子树消失会让叶子全局编号重排）。
        """
        descendants = [
            r["descendant"]
            for r in self.conn.execute(
                "SELECT descendant FROM tree_closure WHERE ancestor = ?", (node_id,)
            )
        ]
        if not descendants:
            return 0
        # node_id 一定在 descendants 里（含自反），先记下它的 tree_id 用于重算。
        tree_id = self.conn.execute(
            "SELECT tree_id FROM tree_node WHERE id = ?", (node_id,)
        ).fetchone()["tree_id"]
        placeholders = ",".join("?" * len(descendants))
        with self.conn:
            self.conn.execute(
                f"DELETE FROM tree_node WHERE id IN ({placeholders})", descendants
            )
            self._recalc_order(int(tree_id))
        return len(descendants)

    def remove_node(self, node_id: int) -> bool:
        """仅删除节点本身，把它的孩子上提到祖父位置（孩子的孩子不动）。

        实现：先把 ``parent_id = node_id`` 的所有孩子改挂到 ``node_id.parent_id``
        ——这一步会触发"移动子树"触发器，自动重算闭包；再删 ``node_id`` 本身，
        外键 CASCADE 把 ``node_id`` 残留的闭包行清掉。

        约束：**删根 + 多孩子时拒绝**——会让一棵 ``tree_id`` 出现多根，
        破坏"一棵 tree_id 一个根"的不变量。这种情况请改用：

        - :meth:`remove_subtree` 整棵子树删掉，或
        - 先把多余孩子合并到唯一保留的孩子下，再 :meth:`remove_node`，或
        - 先 :meth:`insert_parent` 给孩子套个新根再删。

        删根 + 0 或 1 个孩子的情形会被正确处理（``tree_index`` 由触发器同步）。
        """
        with self.conn:
            node = self.conn.execute(
                "SELECT parent_id, tree_id FROM tree_node WHERE id = ?",
                (node_id,),
            ).fetchone()
            if node is None:
                return False
            new_parent = node["parent_id"]  # None 表示自己就是根

            if new_parent is None:
                n_children = self.conn.execute(
                    "SELECT COUNT(*) AS n FROM tree_node WHERE parent_id = ?",
                    (node_id,),
                ).fetchone()["n"]
                if n_children > 1:
                    raise ValueError(
                        f"无法删除有 {n_children} 个孩子的根节点 {node_id}："
                        "会留下多棵孤立子树（违反一棵 tree_id 一个根的不变量）；"
                        "请改用 remove_subtree(node_id) 或先合并孩子"
                    )
                # 删根 + 0/1 孩子：单孩子升根时把 sibling_order 重置为 0。
                self.conn.execute(
                    "UPDATE tree_node "
                    "SET parent_id = NULL, sibling_order = 0 "
                    "WHERE parent_id = ?",
                    (node_id,),
                )
            else:
                # 上提孩子到祖父 new_parent：
                # 把它们的 sibling_order 整体加上 new_parent 当前末尾偏移，
                # 既保持子节点之间的相对顺序，又接到新兄弟末尾。
                offset = int(self.conn.execute(
                    "SELECT COALESCE(MAX(sibling_order), -1) + 1 AS so "
                    "FROM tree_node WHERE parent_id = ?",
                    (new_parent,),
                ).fetchone()["so"])
                self.conn.execute(
                    "UPDATE tree_node "
                    "SET parent_id = ?, sibling_order = sibling_order + ? "
                    "WHERE parent_id = ?",
                    (new_parent, offset, node_id),
                )
            self.conn.execute("DELETE FROM tree_node WHERE id = ?", (node_id,))
            self._recalc_order(int(node["tree_id"]))
        return True

    def move_subtree(self, node_id: int, new_parent_id: Optional[int]) -> None:
        """把以 ``node_id`` 为根的子树挂到 ``new_parent_id`` 下。

        ``new_parent_id=None`` 表示把节点提升为根。

        约束（任一不满足都会抛 :class:`ValueError`）：

        - 节点必须存在；
        - 不能挂到自己下面；
        - 目标父节点不能在自己的子树内（防环，闭包表 O(1) 命中）；
        - **不允许跨树移动**：源子树的 ``tree_id`` 必须等于目标父节点的
          ``tree_id``；跨树会让"父子 tree_id 不一致"，目前不支持自动同步；
        - **提升为根时**：目标 ``tree_id`` 不能已有别的根（强制"一棵
          tree_id 一个根"的不变量）。

        闭包表与 ``tree_index`` 的更新由触发器自动完成。
        """
        with self.conn:
            src = self.conn.execute(
                "SELECT tree_id, parent_id FROM tree_node WHERE id = ?",
                (node_id,),
            ).fetchone()
            if src is None:
                raise ValueError(f"节点不存在: {node_id}")
            src_tree_id = src["tree_id"]

            if new_parent_id is not None:
                if new_parent_id == node_id:
                    raise ValueError("不能把节点挂到自己下面")
                if self.is_ancestor(node_id, new_parent_id):
                    raise ValueError(
                        f"移动会形成环：目标父节点 {new_parent_id} 在 "
                        f"{node_id} 的子树内"
                    )
                target = self.conn.execute(
                    "SELECT tree_id FROM tree_node WHERE id = ?",
                    (new_parent_id,),
                ).fetchone()
                if target is None:
                    raise ValueError(f"目标父节点不存在: {new_parent_id}")
                if target["tree_id"] != src_tree_id:
                    raise ValueError(
                        f"暂不支持跨树移动：源 tree_id={src_tree_id} ≠ "
                        f"目标 tree_id={target['tree_id']}"
                    )
            else:
                # 提升为根：原本不是根 → 检查是否会形成"双根"
                if src["parent_id"] is not None:
                    other_root = self.conn.execute(
                        "SELECT id FROM tree_node "
                        "WHERE tree_id = ? AND parent_id IS NULL "
                        "  AND id != ? LIMIT 1",
                        (src_tree_id, node_id),
                    ).fetchone()
                    if other_root is not None:
                        raise ValueError(
                            f"tree_id={src_tree_id} 已有根 "
                            f"(id={other_root['id']})；一棵 tree_id 只能有"
                            f"一个根，无法把 {node_id} 提升为新根"
                        )

            # 计算移动后的 sibling_order：
            # - 提升为根 → 0（根没有兄弟）
            # - 挂到 new_parent_id 下 → 当前末尾追加
            if new_parent_id is None:
                new_so = 0
            else:
                new_so = int(self.conn.execute(
                    "SELECT COALESCE(MAX(sibling_order), -1) + 1 AS so "
                    "FROM tree_node WHERE parent_id = ?",
                    (new_parent_id,),
                ).fetchone()["so"])
            self.conn.execute(
                "UPDATE tree_node SET parent_id = ?, sibling_order = ? "
                "WHERE id = ?",
                (new_parent_id, new_so, node_id),
            )
            self._recalc_order(src_tree_id)

    # ============ 读操作 ============

    _NODE_COLS = (
        "id, tree_id, parent_id, name, payload, "
        "sibling_order, node_order, created_at"
    )

    def get_node(self, node_id: int) -> Optional[dict]:
        row = self.conn.execute(
            f"SELECT {self._NODE_COLS} FROM tree_node WHERE id = ?",
            (node_id,),
        ).fetchone()
        return self._row_to_dict(row) if row else None

    def get_subtree(
        self, node_id: int, *, include_self: bool = True
    ) -> list[dict]:
        """取整棵子树（按 ``depth`` 递增、同层按 ``parent_id, sibling_order`` 排序）。

        排序键的设计目的：BFS 顺序 + 同父亲下按显式兄弟序，方便调用方按层
        遍历或重建嵌套结构时与 :meth:`load_tree` 行为一致。
        """
        sql = (
            f"SELECT n.id, n.tree_id, n.parent_id, n.name, n.payload, "
            f"       n.sibling_order, n.node_order, n.created_at, c.depth "
            f"FROM tree_node n "
            f"JOIN tree_closure c ON c.descendant = n.id "
            f"WHERE c.ancestor = ?"
        )
        if not include_self:
            sql += " AND c.depth > 0"
        sql += " ORDER BY c.depth, n.parent_id, n.sibling_order, n.id"
        return [self._row_to_dict(r) for r in self.conn.execute(sql, (node_id,))]

    def get_ancestors(self, node_id: int) -> list[dict]:
        """取祖先链，顺序为"从根到 ``node_id`` 的父亲"。"""
        sql = (
            "SELECT n.id, n.tree_id, n.parent_id, n.name, n.payload, "
            "       n.sibling_order, n.node_order, n.created_at, c.depth "
            "FROM tree_node n "
            "JOIN tree_closure c ON c.ancestor = n.id "
            "WHERE c.descendant = ? AND c.depth > 0 "
            "ORDER BY c.depth DESC"
        )
        return [self._row_to_dict(r) for r in self.conn.execute(sql, (node_id,))]

    def get_children(self, node_id: int) -> list[dict]:
        """直接孩子（一层），按 ``sibling_order`` 升序。"""
        sql = (
            f"SELECT {self._NODE_COLS} FROM tree_node WHERE parent_id = ? "
            f"ORDER BY sibling_order, id"
        )
        return [self._row_to_dict(r) for r in self.conn.execute(sql, (node_id,))]

    def get_depth(self, node_id: int) -> int:
        """节点深度（根为 0）；节点不存在时抛 :class:`ValueError`。"""
        self._require_node(node_id)
        row = self.conn.execute(
            "SELECT MAX(depth) AS d FROM tree_closure WHERE descendant = ?",
            (node_id,),
        ).fetchone()
        return int(row["d"]) if row["d"] is not None else 0

    def get_subtree_height(self, node_id: int) -> int:
        """以 ``node_id`` 为根的子树最大深度（单节点为 0）；
        节点不存在时抛 :class:`ValueError`。"""
        self._require_node(node_id)
        row = self.conn.execute(
            "SELECT MAX(depth) AS d FROM tree_closure WHERE ancestor = ?",
            (node_id,),
        ).fetchone()
        return int(row["d"]) if row["d"] is not None else 0

    def _require_node(self, node_id: int) -> None:
        """节点不存在时抛 :class:`ValueError`。"""
        row = self.conn.execute(
            "SELECT 1 FROM tree_node WHERE id = ? LIMIT 1", (node_id,)
        ).fetchone()
        if row is None:
            raise ValueError(f"节点不存在: {node_id}")

    def _recalc_order(self, tree_id: int) -> None:
        """重算 ``tree_id`` 这棵树所有节点的 ``node_order``，写回 DB。

        策略：从 DB 拉一份 (id, parent_id, payload) 列表，搭一棵临时
        :class:`tree.TreeNode` 树（``value`` 用反序列化后的 payload，使
        tree.py 中"叶子 value 是 dict 且含 order 字段则同步"的规则生效），
        调 ``_refresh_orders`` 让 tree.py 算法给出每个节点的 ``order``，
        再 ``executemany`` 一次性 UPDATE 回去。

        - 直接复用 tree.py 的算法，避免在 SQL 里重写"叶子 DFS 先序全局编号、
          非叶子取 max(children.order)、dict.value['order'] 同步"这套规则。
        - 调用者必须已经在 ``with self.conn`` 事务内（本方法只发 UPDATE，
          不再开事务），保证与触发它的结构变更原子提交。
        - 树为空时是 no-op。
        """
        rows = self.conn.execute(
            "SELECT id, parent_id, payload FROM tree_node WHERE tree_id = ? "
            "ORDER BY parent_id, sibling_order, id",
            (tree_id,),
        ).fetchall()
        if not rows:
            return
        # 用真实 payload 作为 value：让叶子 value 含 "order" 时能命中 tree.py
        # 的同步规则；db_id 保留在 nodes 字典的键里，回写时按键取 (order, id) 对。
        nodes: dict[int, TreeNode] = {
            r["id"]: TreeNode(value=self._load(r["payload"])) for r in rows
        }
        root: Optional[TreeNode] = None
        for r in rows:
            n = nodes[r["id"]]
            if r["parent_id"] is None:
                root = n
            else:
                parent = nodes[r["parent_id"]]
                n.parent = parent
                parent.children.append(n)
        if root is None:
            return
        root._refresh_orders()
        self.conn.executemany(
            "UPDATE tree_node SET node_order = ? WHERE id = ?",
            [(n.order, db_id) for db_id, n in nodes.items()],
        )

    def is_ancestor(self, ancestor_id: int, descendant_id: int) -> bool:
        """判断 ``ancestor_id`` 是否是 ``descendant_id`` 的祖先（含等于自身）。

        闭包表特有的 O(1) 主键命中。
        """
        row = self.conn.execute(
            "SELECT 1 FROM tree_closure "
            "WHERE ancestor = ? AND descendant = ? LIMIT 1",
            (ancestor_id, descendant_id),
        ).fetchone()
        return row is not None

    def get_root(self, tree_id: int) -> Optional[dict]:
        """取某棵树的根（``parent_id IS NULL``）。"""
        row = self.conn.execute(
            f"SELECT {self._NODE_COLS} FROM tree_node "
            f"WHERE tree_id = ? AND parent_id IS NULL "
            f"ORDER BY id LIMIT 1",
            (tree_id,),
        ).fetchone()
        return self._row_to_dict(row) if row else None

    # ============ tree_index 索引表（"哪棵 tree_id 是哪棵树"） ============

    def list_trees(self) -> list[dict]:
        """列出库里所有树及其根节点名（按 ``tree_id`` 升序）。

        返回每行形如 ``{"tree_id": ..., "root_name": ..., "updated_at": ...}``。
        ``updated_at`` 反映索引行最近一次被触发器刷新的时间。
        """
        return [
            dict(r)
            for r in self.conn.execute(
                "SELECT tree_id, root_name, updated_at "
                "FROM tree_index ORDER BY tree_id"
            )
        ]

    def find_tree_id(self, root_name: str) -> Optional[int]:
        """按根节点名反查 ``tree_id``；找不到返回 ``None``。

        ``tree_index.root_name`` 有 ``UNIQUE`` 约束 + 写入端做了重名检查，
        所以这里至多匹配一行；``ORDER BY tree_id LIMIT 1`` 仅作防御。
        """
        row = self.conn.execute(
            "SELECT tree_id FROM tree_index "
            "WHERE root_name = ? ORDER BY tree_id LIMIT 1",
            (str(root_name),),
        ).fetchone()
        return int(row["tree_id"]) if row else None

    def rename_tree(self, tree_id: int, new_root_name: str) -> bool:
        """重命名某棵树的根节点；``tree_index`` 由触发器自动同步。

        - 找不到该 ``tree_id`` 的根节点时返回 ``False``，不做修改。
        - 新名被别的 ``tree_id`` 占用时抛 :class:`ValueError`（根名全库唯一）。
        - 仅修改根节点的 ``name``；``payload`` / 子树结构不动。
        """
        with self.conn:
            row = self.conn.execute(
                "SELECT id FROM tree_node "
                "WHERE tree_id = ? AND parent_id IS NULL "
                "ORDER BY id LIMIT 1",
                (tree_id,),
            ).fetchone()
            if row is None:
                return False
            self._ensure_root_name_available(
                str(new_root_name), ignore_tree_id=tree_id
            )
            self.conn.execute(
                "UPDATE tree_node SET name = ? WHERE id = ?",
                (str(new_root_name), int(row["id"])),
            )
        return True

    # ============ 与 tree.py 双向桥接 ============

    def save_tree(
        self,
        tree: Tree,
        tree_id: int,
        *,
        overwrite: bool = False,
    ) -> dict[int, int]:
        """把 :class:`tree.Tree` 内存树写入数据库。

        - 用 BFS 写 ``tree_node``；每个孩子按 ``children`` 列表里的下标写入
          ``sibling_order``，使得 :meth:`load_tree` 后子节点顺序与原内存树一致。
        - 闭包表由 INSERT 触发器自动维护，无需手动写。
        - 同 ``tree_id`` 已有数据时：

          * ``overwrite=False``（默认）→ 抛 :class:`ValueError`，避免静默追加
            出"双根"等脏状态；
          * ``overwrite=True`` → 在同一事务内先 ``DELETE FROM tree_node WHERE
            tree_id=?`` 再写新树。``ON DELETE CASCADE`` 会一并把闭包行清掉，
            ``tree_index`` 由触发器同步。

        - 根节点名（``tree.root.name``）若被别的 ``tree_id`` 占用，抛
          :class:`ValueError`（根名全库唯一）；``overwrite=True`` 重写自身
          ``tree_id`` 不算冲突。
        - 返回 ``{id(内存节点): 数据库 id}`` 映射，方便后续按内存对象继续操作；
          空树返回空 dict。
        """
        if tree.root is None:
            return {}
        # 先保证内存树自身的 order 正确（用户可能绕开 Tree.add 直接动 children），
        # 这样下面写入的 node.order 就是可信的，DB 不必再做一次 _recalc_order。
        tree.refresh_orders()
        id_map: dict[int, int] = {}
        # 队列元素 = (节点, 父节点的 DB id, 在父亲 children 中的下标)
        queue: deque[tuple[TreeNode, Optional[int], int]] = deque(
            [(tree.root, None, 0)]
        )
        with self.conn:
            existing = self.conn.execute(
                "SELECT 1 FROM tree_node WHERE tree_id = ? LIMIT 1",
                (tree_id,),
            ).fetchone()
            if existing is not None:
                if not overwrite:
                    raise ValueError(
                        f"tree_id={tree_id} 已存在数据；如需替换请传 "
                        "overwrite=True"
                    )
                self.conn.execute(
                    "DELETE FROM tree_node WHERE tree_id = ?", (tree_id,)
                )
            self._ensure_root_name_available(
                str(tree.root.name), ignore_tree_id=tree_id
            )
            while queue:
                node, parent_db_id, sibling_idx = queue.popleft()
                cur = self.conn.execute(
                    "INSERT INTO tree_node"
                    "(tree_id, parent_id, name, payload, "
                    " sibling_order, node_order) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (tree_id, parent_db_id, str(node.name),
                     self._dump(node.value), sibling_idx, int(node.order)),
                )
                new_id = int(cur.lastrowid)
                id_map[id(node)] = new_id
                for i, child in enumerate(node.children):
                    queue.append((child, new_id, i))
        return id_map

    def load_tree(
        self,
        tree_id: Optional[int] = None,
        *,
        tree_name: Optional[str] = None,
    ) -> Tree:
        """从数据库重建 :class:`tree.Tree` 内存树。

        定位树的方式（优先级从高到低）：

        1. 若提供 ``tree_name``：先在 ``tree_index`` 里按根节点名反查
           ``tree_id``，再用反查到的 ``tree_id`` 加载。即使同时传了
           ``tree_id``，``tree_name`` 也会**优先生效**。
        2. 否则按 ``tree_id`` 直接加载。
        3. 两者都未提供 → 抛 :class:`ValueError`。

        若按 ``tree_name`` 反查不到对应 ``tree_id``，或对应 ``tree_id`` 下没有
        任何节点，统一返回一棵空树（与"传入不存在的 ``tree_id``"的现有行为
        保持一致）。

        注意：**重建只用 ``tree_node``**（邻接表）即可。闭包表只在数据库侧给
        "查祖先 / 查子树"提速，不参与对象图的重建。
        """
        if tree_name is not None:
            resolved = self.find_tree_id(tree_name)
            if resolved is None:
                return Tree()
            tree_id = resolved
        elif tree_id is None:
            raise ValueError("必须提供 tree_id 或 tree_name 之一")

        # ORDER BY parent_id, sibling_order 保证：先把根读出来挂好，再逐个
        # parent 把它的孩子按 sibling_order 升序 append 到 children 列表，
        # 重建后的 children 顺序与 save_tree 写入时的顺序完全一致。
        rows = list(
            self.conn.execute(
                "SELECT id, parent_id, name, payload, "
                "       sibling_order, node_order "
                "FROM tree_node WHERE tree_id = ? "
                "ORDER BY parent_id, sibling_order, id",
                (tree_id,),
            )
        )
        if not rows:
            return Tree()

        root_ids = [r["id"] for r in rows if r["parent_id"] is None]
        if len(root_ids) > 1:
            raise RuntimeError(
                f"tree_id={tree_id} 下检测到 {len(root_ids)} 个根 "
                f"(id={root_ids})；数据已不一致——通常意味着写入端绕过了"
                "add_root / move_subtree 的不变量检查"
            )

        nodes: dict[int, TreeNode] = {}
        for r in rows:
            n = TreeNode(value=self._load(r["payload"]), name=r["name"])
            n.order = int(r["node_order"])  # DB 已存好，不再重算
            nodes[r["id"]] = n
        root: Optional[TreeNode] = None
        for r in rows:
            n = nodes[r["id"]]
            if r["parent_id"] is None:
                root = n
            else:
                parent = nodes[r["parent_id"]]
                n.parent = parent
                parent.children.append(n)

        if root is None:
            return Tree()
        tree = Tree()
        tree.root = root
        # level 是纯结构派生（深度），靠 parent 链一遍 BFS 得到，比 order 便宜，
        # 直接重算更省事；order 已从 DB 灌好，不重算以保留 DB 真值。
        tree.refresh_levels()
        return tree

    # ============ 离线对账 ============

    def verify(self, tree_id: int) -> bool:
        """用递归 CTE 重算"应该有的闭包"，与 ``tree_closure`` 对比。

        返回 ``True`` 表示闭包表与邻接表一致，``False`` 表示出现漂移
        （生产环境里应当报警 + 触发修复）。
        """
        sql = """
        WITH RECURSIVE expected(ancestor, descendant, depth) AS (
            SELECT id, id, 0
            FROM   tree_node
            WHERE  tree_id = :tid
            UNION ALL
            SELECT e.ancestor, n.id, e.depth + 1
            FROM   expected e
            JOIN   tree_node n
              ON   n.parent_id = e.descendant
             AND   n.tree_id   = :tid
        )
        SELECT
            (SELECT COUNT(*) FROM (
                SELECT ancestor, descendant, depth FROM expected
                EXCEPT
                SELECT c.ancestor, c.descendant, c.depth
                FROM   tree_closure c
                JOIN   tree_node n ON n.id = c.descendant
                WHERE  n.tree_id = :tid
            )) AS missing,
            (SELECT COUNT(*) FROM (
                SELECT c.ancestor, c.descendant, c.depth
                FROM   tree_closure c
                JOIN   tree_node n ON n.id = c.descendant
                WHERE  n.tree_id = :tid
                EXCEPT
                SELECT ancestor, descendant, depth FROM expected
            )) AS extra
        """
        row = self.conn.execute(sql, {"tid": tree_id}).fetchone()
        return int(row["missing"]) == 0 and int(row["extra"]) == 0

    # ============ 内部辅助 ============

    @staticmethod
    def _dump(value: Any, *, lenient: bool = False) -> Optional[str]:
        """把 ``value`` 序列化为 JSON 文本，``None`` 透传为 NULL。

        ``lenient=False``（默认）时不可序列化对象会抛 :class:`TypeError`，
        让用户看见"我塞进去的不是基本类型"这件事；``lenient=True`` 时
        fallback 到 ``str(value)`` 并发 :class:`UserWarning`，避免数据
        类型在 save → load 后被静默改成字符串。
        """
        if value is None:
            return None
        try:
            return json.dumps(value, ensure_ascii=False)
        except (TypeError, ValueError) as e:
            if not lenient:
                raise TypeError(
                    f"payload 无法 JSON 序列化（{type(value).__name__}）："
                    f"{e}；请改用基本类型或显式调用 _dump(..., lenient=True)"
                ) from e
            warnings.warn(
                f"payload 已被字符串化（{type(value).__name__}）——"
                "load 出来会变 str，类型不再无损往返",
                UserWarning,
                stacklevel=2,
            )
            return json.dumps(str(value), ensure_ascii=False)

    @staticmethod
    def _load(text: Optional[str]) -> Any:
        if text is None:
            return None
        try:
            return json.loads(text)
        except (TypeError, ValueError):
            return text

    @staticmethod
    def _row_to_dict(row: sqlite3.Row) -> dict:
        """把一行 ``sqlite3.Row`` 翻成 dict，并把 ``payload`` 反序列化。

        调用方必须保证 ``row`` 非 ``None``（"找不到节点"应在调用方处理，
        而不是让本方法返回空 dict 把"不存在"和"存在但全字段为 NULL"混在一起）。
        """
        d = dict(row)
        if "payload" in d:
            d["payload"] = ClosureTreeDB._load(d["payload"])
        return d


# ============ 自检 / 演示 ============

def _print_tree(db: "ClosureTreeDB", root_id: int) -> None:
    """按真实父子关系缩进打印（避免 depth 相同时把不同子树挤成一团）。"""
    rows = db.get_subtree(root_id)
    children_of: dict[int, list[dict]] = {}
    for r in rows:
        children_of.setdefault(r["parent_id"], []).append(r)
    # 从根开始 DFS 打印
    stack: list[tuple[dict, int]] = [(rows[0], 0)]
    while stack:
        node, indent = stack.pop()
        print(f"  {'  ' * indent}- {node['name']} "
              f"(id={node['id']}, depth={node['depth']})")
        for child in reversed(children_of.get(node["id"], [])):
            stack.append((child, indent + 1))


def _demo() -> None:
    """端到端演示：建库 → 写节点 → 读 → 移动 → tree.py 互转 → 校验。"""
    print("=" * 60)
    print("闭包表 + SQLite 演示")
    print("=" * 60)

    with ClosureTreeDB(":memory:") as db:
        # 1) 直接用数据库 API 建树：
        #        根
        #       / | \
        #      A  B  C
        #     /|  |
        #    D E  F
        root = db.add_root(tree_id=1, name="根", payload={"k": "v"})
        a = db.add_node(root, "A")
        b = db.add_node(root, "B")
        c = db.add_node(root, "C")
        d = db.add_node(a, "D")
        e = db.add_node(a, "E")
        f = db.add_node(b, "F")

        print(f"\n[1] 建树后各节点 id：root={root}, A={a}, B={b}, C={c}, "
              f"D={d}, E={e}, F={f}")

        print("\n[2] 取根的整棵子树：")
        _print_tree(db, root)

        print("\n[3] D 的祖先链（从根到父）：",
              [n["name"] for n in db.get_ancestors(d)])

        print("\n[4] B 的直接孩子：",
              [n["name"] for n in db.get_children(b)])

        print(f"\n[5] is_ancestor(root, F) = {db.is_ancestor(root, f)} (期望 True)")
        print(f"     is_ancestor(A, F)    = {db.is_ancestor(a, f)} (期望 False)")
        print(f"     get_depth(F)         = {db.get_depth(f)} (期望 2)")
        print(f"     get_subtree_height(root) = {db.get_subtree_height(root)} "
              f"(期望 2)")

        print("\n[5'] 各节点 node_order（与 tree.py 的 TreeNode.order 同语义）：")
        for r in db.get_subtree(root):
            print(f"  - {r['name']:>4}  id={r['id']}  depth={r['depth']}  "
                  f"node_order={r['node_order']}")

        # 6) 移动子树：把 A 整棵子树挂到 C 下
        print("\n[6] 移动 A 子树到 C 下 ...")
        db.move_subtree(a, new_parent_id=c)
        _print_tree(db, root)
        assert db.is_ancestor(c, d), "移动后 C 应当是 D 的祖先"
        assert not db.is_ancestor(root, d) or db.is_ancestor(c, d)
        assert db.verify(tree_id=1), "移动后闭包表应与邻接表一致"
        print("  → verify(1) 通过")

        # 7) 防环
        print("\n[7] 尝试把 root 挂到 D 下（应抛错）...")
        try:
            db.move_subtree(root, new_parent_id=d)
        except ValueError as ex:
            print(f"  → 拒绝：{ex}")

        # 8) 删除节点（仅自己，孩子上提）
        print("\n[8] remove_node(C)：删 C 但 C 的孩子（A 子树）应上提到根 ...")
        db.remove_node(c)
        _print_tree(db, root)
        assert db.verify(tree_id=1)
        print("  → verify(1) 通过")

        # 9) 与 tree.py 互转：load 出来再 save 回去到一棵新 tree_id
        #    根名全库唯一，所以先把副本根名改为 "根_副本" 再写入。
        print("\n[9] load_tree(1) 还原内存树，再 save_tree 到 tree_id=2 ...")
        mem_tree = db.load_tree(tree_id=1)
        print("  内存树 BFS 顺序的 name：",
              [n.name for n in mem_tree.bfs()])
        mem_tree.root.name = "根_副本"
        db.save_tree(mem_tree, tree_id=2)
        assert db.verify(tree_id=2), "新写入的 tree_id=2 闭包应一致"
        print("  → verify(2) 通过")

        # 10) 删整棵子树（演示 ON DELETE CASCADE 把闭包行级联删掉）
        print("\n[10] remove_subtree(A 在 tree_id=2 中的对应节点) ...")
        # 重新查 tree_id=2 里 name='A' 的节点 id
        a2_id = db.conn.execute(
            "SELECT id FROM tree_node WHERE tree_id = 2 AND name = 'A'"
        ).fetchone()["id"]
        n_removed = db.remove_subtree(a2_id)
        print(f"  → 实际删除 {n_removed} 个节点")
        assert db.verify(tree_id=2)
        print("  → verify(2) 通过")

        # 11) tree_index 演示：列出所有树、按名字反查、改名后自动同步
        print("\n[11] tree_index：当前库里所有树（由触发器自动维护）：")
        for r in db.list_trees():
            print(f"  - tree_id={r['tree_id']}, root_name={r['root_name']!r}, "
                  f"updated_at={r['updated_at']}")

        print(f"\n     find_tree_id('根') = {db.find_tree_id('根')} (期望 1)")
        print(f"     find_tree_id('根_副本') = {db.find_tree_id('根_副本')} (期望 2)")

        print("\n[12] 给 tree_id=1 的根改名为 '组织'，tree_index 应自动同步 ...")
        db.rename_tree(tree_id=1, new_root_name="组织")
        for r in db.list_trees():
            print(f"  - tree_id={r['tree_id']}, root_name={r['root_name']!r}")
        assert db.find_tree_id("组织") == 1
        assert db.find_tree_id("根_副本") == 2
        print("  → rename + 反查通过")

        # 13) 删根：tree_index 也会被触发器自动清掉
        print("\n[13] 删除 tree_id=2 的根（remove_subtree），tree_index 应清掉 ...")
        root2 = db.get_root(tree_id=2)
        assert root2 is not None
        db.remove_subtree(root2["id"])
        print("  当前 tree_index：")
        for r in db.list_trees():
            print(f"  - tree_id={r['tree_id']}, root_name={r['root_name']!r}")
        assert db.find_tree_id("根_副本") is None
        print("  → 索引行已自动清理")

    print("\n所有断言通过。")


if __name__ == "__main__":
    _demo()
