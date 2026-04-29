"""在内存中构造一棵树，然后写入 SQLite 数据库。

依赖：
- :mod:`tree`：内存多叉树。
- :mod:`db`：闭包表 + 邻接表的 SQLite 实现。

运行::

    python main.py

执行后会在当前目录生成 ``tree.db``（SQLite 文件），可用任意 SQLite 客户端
（如 DB Browser for SQLite）打开查看 ``tree_node`` / ``tree_closure`` /
``tree_index`` 三张表。
"""

from __future__ import annotations

from pathlib import Path

from db import ClosureTreeDB
from tree import Tree
from typing import Any


def create_tree_from_dict(data: dict, root_name: str, root_value: Any) -> Tree:
    return Tree.tree_from_dict(root_value, data, root_name=root_name)

def build_sample_tree() -> Tree:
    """构造一棵示例树::

            根节点
           /  |  \\
          2   3   4
         / \\  |
        5   6 7
        |
        8
    """
    tree = Tree("1", root_name="根节点")
    tree.add("2", parent_value="1", name="左分支")
    tree.add("3", parent_value="1", name="右分支")
    tree.add("4", parent_value="1")
    tree.add("5", parent_value="2")
    tree.add("6", parent_value="2")
    tree.add("7", parent_value="3")
    tree.add("8", parent_value="5")
    return tree


def main() -> None:
    db_path = Path("tree.db")

    tree = create_tree_from_dict({
        "ta": 1,
        "tb": 2,
        "tc": 3,
    }, 
    "测试22",
    "测试22"
    )
    print("内存中构造的树：")
    print(tree)
    print(f"节点总数: {len(tree)}，高度: {tree.height()}")

    with ClosureTreeDB(db_path) as db:
        tree_id = 2
        # 只清掉本次要写的 tree_id，保留库中其它树；
        # 闭包表的相关行靠外键 ON DELETE CASCADE 自动级联清理。
        with db.conn:
            db.conn.execute(
                "DELETE FROM tree_node WHERE tree_id = ?", (tree_id,)
            )
        id_map = db.save_tree(tree, tree_id=tree_id)
        print(f"\n已写入 {db_path}（tree_id={tree_id}），"
              f"共 {len(id_map)} 个节点。")

        # 触发器维护的闭包表与邻接表必须一致。
        assert db.verify(tree_id=tree_id), "闭包表与邻接表不一致"
        print("verify(tree_id=1) 通过：闭包表与邻接表一致。")

        root = db.get_root(tree_id)
        assert root is not None
        print(f"\n数据库中的根节点：id={root['id']}, name={root['name']}")
        print("根的整棵子树（数据库视角，按真实父子关系缩进）：")
        rows = db.get_subtree(root["id"])
        children_of: dict[int, list[dict]] = {}
        for r in rows:
            children_of.setdefault(r["parent_id"], []).append(r)
        # 用显式栈做 DFS，保证按真实 parent_id 而不是 depth 缩进。
        stack: list[tuple[dict, int]] = [(rows[0], 0)]
        while stack:
            node, indent = stack.pop()
            print(f"  {'  ' * indent}- {node['name']} "
                  f"(id={node['id']}, depth={node['depth']}, "
                  f"payload={node['payload']!r})")
            for child in reversed(children_of.get(node["id"], [])):
                stack.append((child, indent + 1))

        # 反向回读：从数据库还原内存树，验证内容一致。
        loaded = db.load_tree(tree_id,)
        loaded.save()
        print("\n从数据库重建的内存树：")
        print(loaded)
        assert [n.value for n in loaded.bfs()] == [n.value for n in tree.bfs()]
        assert [n.name for n in loaded.bfs()] == [n.name for n in tree.bfs()]
        print("回读结构与原树一致。")

        # tree_index：触发器自动维护的「tree_id ↔ 根节点名」索引。
        # 业务侧只要记得"根节点叫什么"就能反查 tree_id，不用记数字。
        print("\n库中所有树（tree_index 视图）：")
        for r in db.list_trees():
            print(f"  - tree_id={r['tree_id']}, "
                  f"root_name={r['root_name']!r}, "
                  f"updated_at={r['updated_at']}")

        found_tid = db.find_tree_id(str(tree.root.name))
        print(f"\n按根节点名 {tree.root.name!r} 反查 tree_id = {found_tid}")
        assert found_tid == tree_id

if __name__ == "__main__":
    main()
