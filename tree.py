"""通用多叉树实现。

支持节点的添加、删除、查找、遍历（DFS/BFS）、高度/深度计算以及可视化打印。
"""

from __future__ import annotations

import json
from collections import deque
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Optional

_MISSING: Any = object()  # 专用哨兵；用来区分"没传参"和"显式传了 None"


class TreeNode:
    """树的节点。

    ``level`` 表示节点在所属树中的层级：根节点为 ``0``，每向下一层 +1。
    该属性在节点创建时基于 ``parent`` 自动计算；在树的内置修改方法
    （``remove`` / ``remove_all`` / ``replace_with_subtree`` 等）被调用后也会
    自动保持正确。若外部直接改写 ``parent`` / ``children`` 指针，可调用
    :meth:`_refresh_levels` 或 :meth:`Tree.refresh_levels` 手动修正。

    ``order`` 表示节点的排列顺序：

    - **叶子节点**按 DFS 先序（从左到右、从上到下）在整棵（子）树范围内从 ``0``
      开始**全局**编号，保证所有叶子的 ``order`` 互不相同。
    - **特例**：若叶子节点的 ``value`` 是 :class:`dict` 且包含 ``"order"`` 键，
      则该叶子的 ``order`` 直接同步为 ``value["order"]``，不再使用上述默认编号
      （此时多个叶子的 ``order`` 是否唯一由调用方负责，框架不强制；同时该字段
      应为可比较的数值，否则非叶节点取 ``max`` 时会抛错）。
    - **非叶子节点**的 ``order`` 等于其所有子节点 ``order`` 的最大值——在没有
      上述 dict 覆盖的情况下，相当于该子树内"最晚出现的叶子"的编号。
    - 独立的孤根（无父无子）``order`` 为 ``0``。

    节点创建时先初始化为 ``0``，随后由 :meth:`Tree` 的添加 / 删除 / 替换等方法
    自动刷新；也可调用 :meth:`Tree.refresh_orders` 或 :meth:`_refresh_orders`
    手动重算。
    """

    __slots__ = ("value", "name", "parent", "children", "level", "order")

    def __init__(
        self,
        value: Any,
        name: Optional[Any] = None,
        parent: Optional["TreeNode"] = None,
    ) -> None:
        self.value = value
        self.name: Any = name if name is not None else value
        self.parent: Optional[TreeNode] = parent
        self.children: list[TreeNode] = []
        self.level: int = 0 if parent is None else parent.level + 1
        self.order: int = 0

    def add_child(self, value: Any, name: Optional[Any] = None) -> "TreeNode":
        """在当前节点下挂一个新子节点并返回该子节点。

        .. warning::
            本方法是**底层**操作：新子节点的 ``level`` 在构造时会正确计算，但
            **不会**刷新祖先链上所有节点的 ``order``。如果你在一棵已挂好的
            :class:`Tree` 里改结构，请优先用 :meth:`Tree.add`（或在手动操作后
            调用 :meth:`Tree.refresh_orders`）来保证 ``order`` 字段全树一致。
        """
        child = TreeNode(value, name=name, parent=self)
        self.children.append(child)
        return child

    def is_leaf(self) -> bool:
        return not self.children

    def is_root(self) -> bool:
        return self.parent is None

    def _refresh_levels(self) -> None:
        """基于当前 ``parent`` 重算自身及所有后代的 ``level``（O(N)）。

        用 BFS 迭代实现，深度再大也不会爆 Python 递归栈。
        """
        self.level = 0 if self.parent is None else self.parent.level + 1
        queue: deque[TreeNode] = deque([self])
        while queue:
            node = queue.popleft()
            next_level = node.level + 1
            for child in node.children:
                child.level = next_level
                queue.append(child)

    def _refresh_orders(self) -> None:
        """基于当前结构重算自身及所有后代的 ``order``（O(N)）。

        叶子节点按 DFS 先序在整棵（子）树内从 ``0`` 开始**全局**编号，互不相同；
        若叶子的 ``value`` 是 :class:`dict` 且含 ``"order"`` 键，则用该值覆盖默认
        编号（counter 仍照常自增，等价于"dict 叶子直接钉死自己的 order，普通叶子
        继续按 DFS 序排"）。非叶节点的 ``order`` 等于其所有子节点 ``order`` 的
        最大值。

        用显式栈做迭代后序遍历，避免深树时触发 Python 递归上限。
        """
        counter = 0
        stack: list[tuple[TreeNode, bool]] = [(self, False)]
        while stack:
            node, visited = stack.pop()
            if visited:
                if not node.children:
                    val = node.value
                    if isinstance(val, dict) and "order" in val:
                        node.order = val["order"]
                    else:
                        node.order = counter
                    counter += 1
                else:
                    node.order = max(c.order for c in node.children)
            else:
                stack.append((node, True))
                # 逆序压栈，保证出栈时按原顺序从左到右处理子节点。
                for child in reversed(node.children):
                    stack.append((child, False))

    def __repr__(self) -> str:
        if self.name != self.value:
            return (
                f"TreeNode(value={self.value!r}, name={self.name!r}, "
                f"level={self.level}, order={self.order})"
            )
        return f"TreeNode({self.value!r}, level={self.level}, order={self.order})"


class Tree:
    """通用多叉树。"""

    def __init__(
        self,
        root_value: Any = _MISSING,
        root_name: Optional[Any] = None,
    ) -> None:
        """构造一棵树。

        不传 ``root_value`` 时创建空树；显式传入任何值（包括 ``None``、``0``、
        ``""`` 等 falsy 值）都会立即生成根节点。
        """
        if root_value is _MISSING:
            self.root: Optional[TreeNode] = None
        else:
            self.root = TreeNode(root_value, name=root_name)

    # ---------- 基本信息 ----------

    def is_empty(self) -> bool:
        return self.root is None

    def size(self) -> int:
        """节点总数。"""
        return sum(1 for _ in self._iter_nodes())

    def height(self, node: Optional[TreeNode] = None) -> int:
        """以 node 为根的子树高度（空树为 -1，单节点为 0）。

        迭代版：用栈维护 (节点, 当前深度) 对，扫一遍取最大深度，避免递归。
        """
        node = node if node is not None else self.root
        if node is None:
            return -1
        max_depth = 0
        stack: list[tuple[TreeNode, int]] = [(node, 0)]
        while stack:
            cur, d = stack.pop()
            if d > max_depth:
                max_depth = d
            for child in cur.children:
                stack.append((child, d + 1))
        return max_depth

    def depth(self, node: TreeNode) -> int:
        """节点在树中的深度（根节点为 0）。等价于 ``node.level``。"""
        return node.level

    def refresh_levels(self) -> None:
        """重新计算整棵树每个节点的 ``level``。

        节点创建与树的内置修改方法都会自动维护 ``level``，正常情况无需调用本方法。
        仅在外部绕过这些方法直接改写 ``parent`` / ``children`` 指针时才可能需要手动修正。

        .. note::
            如果 ``self.root`` 实际上还挂在另一棵树里（典型场景是
            :meth:`shallow_copy_subtree` 出来的"假树"），本方法会**沿 parent 链
            爬到真正的根**再刷新；这样既能保证整棵原树的 ``level`` 一致，也避免
            把一段子树视作独立树导致原树状态被破坏。
        """
        if self.root is None:
            return
        top = self.root
        while top.parent is not None:
            top = top.parent
        top._refresh_levels()

    def refresh_orders(self) -> None:
        """重新计算整棵树每个节点的 ``order``。

        树的内置修改方法会自动维护 ``order``，正常情况无需调用。仅在外部直接改动
        ``children`` 列表绕过这些方法时，才可能需要手动刷新。

        .. note::
            和 :meth:`refresh_levels` 同样的理由：本方法会沿 parent 链爬到真正的
            根再刷新，避免在 :meth:`shallow_copy_subtree` 出来的"假树"上调用时，
            把"叶子全局编号"的不变量在原树里搞乱。
        """
        if self.root is None:
            return
        top = self.root
        while top.parent is not None:
            top = top.parent
        top._refresh_orders()

    # ---------- 增删改查 ----------

    def add(
        self,
        value: Any,
        parent_value: Any = _MISSING,
        *,
        name: Optional[Any] = None,
    ) -> TreeNode:
        """在指定父节点下新增子节点；不传 ``parent_value`` 时作为根节点。

        与 :class:`Tree` 的构造函数同款的 ``_MISSING`` 哨兵机制：``parent_value``
        缺省时视为"挂为根"；显式传入任何值（包括 ``None``、``0``、``""`` 等
        falsy 值）都会被当作真正的父节点查询键，从而支持在 ``value=None`` 的
        节点下继续 ``add`` 子节点。

        ``name`` 未传入时默认等于 ``value``。
        """
        if parent_value is _MISSING:
            if self.root is not None:
                raise ValueError("根节点已存在，请指定 parent_value")
            self.root = TreeNode(value, name=name)
            return self.root

        parent = self.find(parent_value)
        if parent is None:
            raise ValueError(f"未找到父节点: {parent_value!r}")
        node = parent.add_child(value, name=name)
        self.refresh_orders()
        return node

    def remove(self, value: Any) -> bool:
        """删除第一个值等于 value 的节点（连同其子树）。"""
        target = self.find(value)
        if target is None:
            return False
        if target is self.root:
            # 根节点被移除后整棵树为空；target 本身作为"独立子树的根"出去时，
            # 它的 level / order 早已是根视角（level=0，order 基于其自身子树），
            # 所以这里**无需**再额外刷新。
            self.root = None
            return True
        parent = target.parent
        assert parent is not None
        parent.children.remove(target)
        target.parent = None
        target._refresh_levels()
        target._refresh_orders()
        self.refresh_orders()
        return True

    def remove_all(
        self,
        value: Any = None,
        *,
        predicate: Optional[Callable[[TreeNode], bool]] = None,
    ) -> int:
        """批量删除所有匹配节点（连同其子树），返回**实际删除的子树数量**。

        若一个节点的祖先也在匹配集合中，则它会随祖先一并删除，不会重复计数。
        根节点匹配时整棵树被清空，计为 1。
        """
        targets = self.find_all(value, predicate=predicate)
        if not targets:
            return 0

        target_ids = {id(n) for n in targets}

        def _ancestor_in_targets(node: TreeNode) -> bool:
            p = node.parent
            while p is not None:
                if id(p) in target_ids:
                    return True
                p = p.parent
            return False

        effective = [n for n in targets if not _ancestor_in_targets(n)]

        count = 0
        for node in effective:
            if node is self.root:
                self.root = None
                count += 1
                continue
            parent = node.parent
            if parent is None:
                continue  # 已被先前操作分离
            parent.children.remove(node)
            node.parent = None
            node._refresh_levels()
            node._refresh_orders()
            count += 1
        self.refresh_orders()
        return count

    def deep_copy_subtree(
        self,
        value: Any = None,
        *,
        predicate: Optional[Callable[[TreeNode], bool]] = None,
    ) -> Optional["Tree"]:
        """以匹配节点为根，**深拷贝**其子树作为一棵独立的新 :class:`Tree` 返回。

        - 按值或自定义谓词定位第一个匹配节点（BFS）。
        - 新树所有节点都是新对象，``parent`` 指针正确重建，与原树结构完全独立——
          对新树的增删改**不会**影响原树。
        - ``value`` / ``name`` 属性按引用拷贝；若它们是可变对象且希望完全隔离，
          可在外层再调用 ``copy.deepcopy``。
        - 未找到匹配时返回 ``None``。
        """
        target = self.find(value, predicate=predicate)
        if target is None:
            return None
        new_tree = Tree()
        new_tree.root = self._deep_copy_node(target, parent=None)
        new_tree.refresh_orders()
        return new_tree

    def shallow_copy_subtree(
        self,
        value: Any = None,
        *,
        predicate: Optional[Callable[[TreeNode], bool]] = None,
    ) -> Optional["Tree"]:
        """以匹配节点为根，**浅拷贝**其子树作为一棵新 :class:`Tree` 返回。

        - 按值或自定义谓词定位第一个匹配节点（BFS）。
        - 新 ``Tree`` 仅是包装壳，``root`` 直接指向原子树的根节点，内部所有节点与
          原树**共享**。对新树节点的修改（改 value、改 name、增删子节点等）会同步
          反映到原树。
        - 适合需要把某棵子树当"独立树"调用方法（遍历、查找、``save``、``to_dict``
          等）但又不想付拷贝成本的场景。
        - 未找到匹配时返回 ``None``。

        .. note::
            由于节点是**共享**的，新树里各节点的 ``level`` / ``order`` 保留的是
            **原树视角**的值。例如原树某个 level=2 的节点作为新树的根，新树的
            ``root.level`` 依然是 ``2`` 而不是 ``0``。如果你需要以"独立树"视角
            重新编号，请改用 :meth:`deep_copy_subtree`。
        """
        target = self.find(value, predicate=predicate)
        if target is None:
            return None
        new_tree = Tree()
        new_tree.root = target
        return new_tree

    def replace_with_subtree(
        self,
        value: Any = None,
        subtree: Optional[Tree | TreeNode] = None,
        *,
        predicate: Optional[Callable[[TreeNode], bool]] = None,
        deep_copy: bool = True,
    ) -> bool:
        """用指定**子树**替换匹配到的第一个节点（连同其原有子树）。

        - 按值或自定义谓词定位第一个匹配节点（BFS）。
        - ``subtree`` 可以是 :class:`Tree`（取其 ``root``）或 :class:`TreeNode`；
          为 ``None`` 或空树时视为无效输入，直接返回 ``False``。
        - ``deep_copy=True``（默认）时，对传入子树做深拷贝后再挂载，替换后与源
          对象完全独立。
        - ``deep_copy=False`` 时直接挂载原节点（与源共享内部结构），同时会把源
          节点从原位置**整体摘下**以避免脏状态：若源节点有父节点，会从其
          ``children`` 列表中移除；若 ``subtree`` 是 :class:`Tree` 且其 ``root``
          正是该节点，则该 ``Tree`` 的 ``root`` 会被置为 ``None``。
        - 若匹配到的是根节点，则整棵树的根会被替换为新子树。
        - 当 ``deep_copy=False`` 且 ``subtree`` 的根恰好就是要被替换的 ``target``
          时，视为成功的 no-op，直接返回 ``True``。
        - 成功替换返回 ``True``；未找到匹配节点或 ``subtree`` 无效时返回 ``False``。
        """
        if subtree is None:
            return False
        if isinstance(subtree, Tree):
            src_root = subtree.root
            src_tree: Optional[Tree] = subtree
        elif isinstance(subtree, TreeNode):
            src_root = subtree
            src_tree = None
        else:
            raise TypeError("subtree 必须是 Tree 或 TreeNode 对象")
        if src_root is None:
            return False

        target = self.find(value, predicate=predicate)
        if target is None:
            return False

        if deep_copy:
            new_root = self._deep_copy_node(src_root, parent=None)
        else:
            if src_root is target:
                return True
            src_parent = src_root.parent
            if src_parent is not None:
                try:
                    src_parent.children.remove(src_root)
                except ValueError:
                    pass
                src_root.parent = None
                # 摘走 src_root 之后，源节点所在树的 order 必须重算：沿 parent
                # 向上爬到顶，从那里做一次 refresh；否则源树会残留错误的 order
                # 值（例如原本是 max 的分支被整个抽走后，父节点的 order 不再正确）。
                src_top = src_parent
                while src_top.parent is not None:
                    src_top = src_top.parent
                src_top._refresh_orders()
            if src_tree is not None and src_tree.root is src_root:
                # 整棵源 Tree 的根被抽走，Tree 置空。
                src_tree.root = None
            new_root = src_root

        if target is self.root:
            new_root.parent = None
            self.root = new_root
            new_root._refresh_levels()
            target._refresh_levels()
            self.refresh_orders()
            target._refresh_orders()
            return True

        parent = target.parent
        assert parent is not None
        idx = parent.children.index(target)
        new_root.parent = parent
        parent.children[idx] = new_root
        target.parent = None
        new_root._refresh_levels()
        target._refresh_levels()
        self.refresh_orders()
        target._refresh_orders()
        return True

    @staticmethod
    def _deep_copy_node(
        node: TreeNode, parent: Optional[TreeNode]
    ) -> TreeNode:
        """深拷贝以 ``node`` 为根的子树，挂到 ``parent`` 下（迭代实现）。"""
        root_copy = TreeNode(node.value, name=node.name, parent=parent)
        stack: list[tuple[TreeNode, TreeNode]] = [(node, root_copy)]
        while stack:
            orig, dup = stack.pop()
            for orig_child in orig.children:
                dup_child = TreeNode(
                    orig_child.value, name=orig_child.name, parent=dup
                )
                dup.children.append(dup_child)
                stack.append((orig_child, dup_child))
        return root_copy

    @classmethod
    def from_subtrees(
        cls,
        root_value: Any,
        subtrees: Iterable["Tree | TreeNode"],
        *,
        root_name: Optional[Any] = None,
        deep_copy: bool = True,
    ) -> "Tree":
        """新建一棵以 ``root_value`` 为根的新树，并把传入的多个散落子节点 / 子树
        挂到这个新根下，作为一级孩子。

        典型场景：手头有若干棵互相独立的小树（或几个游离的 :class:`TreeNode`），
        需要给它们"戴一个共同的帽子"统一管理。

        参数：

        - ``root_value`` / ``root_name``：新根节点的 ``value`` / ``name``，与
          :class:`Tree` 构造函数语义一致；``root_name`` 缺省时退化为 ``root_value``。
        - ``subtrees``：可迭代对象，元素可以是 :class:`Tree` 也可以是
          :class:`TreeNode`；对元素的处理规则：

          * ``None`` 和空 :class:`Tree`（``root is None``）会被**静默跳过**，
            方便上游用列表推导式构造时不必额外过滤。
          * 既不是 :class:`Tree` 也不是 :class:`TreeNode` 的对象抛 :class:`TypeError`。
          * 同一个 :class:`TreeNode` 对象（按 ``id`` 判断）在列表中出现多次会抛
            :class:`ValueError`——避免"同一节点同时挂在多个父节点下"的脏状态。

        - ``deep_copy``：

          * ``True``（默认）：对每个源子树做深拷贝再挂载，新树与所有源对象**完全
            独立**；改新树不会影响源数据，反之亦然。
          * ``False``：直接挂载源节点本身（与源共享内部结构），同时把源节点从原
            位置**整体摘下**——若源节点有父节点，会从其 ``children`` 列表中移除；
            若源节点是某棵 :class:`Tree` 的 ``root``，那棵 ``Tree`` 的 ``root``
            会被置为 ``None``。受影响的源树会自动刷新一次 ``order``，避免残留
            过期数值。

        返回构造好的新 :class:`Tree`；新树的 ``level`` / ``order`` 已就绪。

        .. note::
            ``deep_copy=False`` 时，如果传入的两个节点存在祖孙关系（比如先传父
            节点 A，再传 A 的某个后代 B），由于 A 整体被摘到新根下，B 在被处理
            时其实已经在新树里、parent 指向 A 内部。此时 B 会再次被"摘"到新根
            下，相当于改变了原来的拓扑——这是符合"摘"语义的自然结果，但通常并
            不是用户的本意，建议自行避免这种用法。
        """
        new_tree = cls(root_value, root_name=root_name)
        new_root = new_tree.root
        assert new_root is not None  # 我们刚刚显式建好的根

        # 第一遍：归一化输入、做合法性 / 重复性校验，记录每个源节点对应的 Tree
        # 容器（如有），方便 deep_copy=False 时正确把源 Tree 的 root 置空。
        sources: list[tuple[TreeNode, Optional[Tree]]] = []
        seen: set[int] = set()
        for item in subtrees:
            if item is None:
                continue
            if isinstance(item, Tree):
                src_root = item.root
                src_tree: Optional[Tree] = item
            elif isinstance(item, TreeNode):
                src_root = item
                src_tree = None
            else:
                raise TypeError(
                    "subtrees 元素必须是 Tree 或 TreeNode 对象（None / 空树会被跳过）"
                )
            if src_root is None:
                continue
            if id(src_root) in seen:
                raise ValueError(
                    "subtrees 中存在重复节点，同一对象不能多次挂入新根之下"
                )
            seen.add(id(src_root))
            sources.append((src_root, src_tree))

        # 第二遍：实际挂载。deep_copy=False 时记录被影响的源树根，用于一次性
        # 刷新 order，避免对同一棵源树重复刷新。
        detached_tops: list[TreeNode] = []
        for src_root, src_tree in sources:
            if deep_copy:
                child = cls._deep_copy_node(src_root, parent=new_root)
                new_root.children.append(child)
                continue

            src_parent = src_root.parent
            if src_parent is not None:
                try:
                    src_parent.children.remove(src_root)
                except ValueError:
                    pass
                # 记录源树最顶层根，稍后统一刷新 order。
                src_top = src_parent
                while src_top.parent is not None:
                    src_top = src_top.parent
                detached_tops.append(src_top)
            if src_tree is not None and src_tree.root is src_root:
                src_tree.root = None

            src_root.parent = new_root
            new_root.children.append(src_root)

        # 对每棵被影响的源树只刷新一次 order。
        seen_top: set[int] = set()
        for top in detached_tops:
            if id(top) in seen_top:
                continue
            seen_top.add(id(top))
            top._refresh_orders()

        # 新树的 level（深拷贝时已正确，浅挂时挂入节点的子树 level 需要平移）
        # 与 order 都从根重新刷新一次，保证一致。
        new_root._refresh_levels()
        new_tree.refresh_orders()
        return new_tree

    @staticmethod
    def nodes_from_dict(data: dict) -> list[TreeNode]:
        """将形如 ``{key: value, ...}`` 的扁平 dict 转换为一组独立的叶子
        :class:`TreeNode`。

        映射规则：

        - dict 的 ``key`` → ``TreeNode.name``
        - dict 的 ``value`` → ``TreeNode.value``

        返回列表顺序与 ``data`` 的迭代顺序一致（Python 3.7+ 即键的插入顺序）。
        返回的节点彼此独立、``parent`` 均为 ``None``，可以直接配合
        :meth:`from_subtrees` / :meth:`TreeNode.add_child` /
        :meth:`replace_with_subtree` 等接口挂入树中。

        典型用法——把一组配置项一键变成一棵新树::

            children = Tree.nodes_from_dict({"a": 1, "b": 2, "c": 3})
            tree = Tree.from_subtrees("R", children, deep_copy=False)

        约束：

        - ``data`` 必须是 ``dict``，否则抛 :class:`TypeError`。
        - 空 ``dict`` 合法，返回空列表。

        .. note::
            本方法**不做递归展开**——若某个 ``value`` 本身又是 ``dict`` /
            ``list``，会原样保留在节点的 ``value`` 字段里，而不会被进一步拆成
            子节点。需要按嵌套结构反序列化整棵树请改用 :meth:`from_dict`
            （要求严格 schema：每个节点必须含 ``"value"`` 键）。
        """
        if not isinstance(data, dict):
            raise TypeError(
                f"data 必须是 dict，收到 {type(data).__name__}"
            )
        return [TreeNode(value=v, name=k) for k, v in data.items()]

    @classmethod
    def tree_from_dict(
        cls,
        root_value: Any,
        data: dict,
        *,
        root_name: Optional[Any] = None,
    ) -> "Tree":
        """将形如 ``{key: value, ...}`` 的扁平 dict 一步直接构造成一棵新树。

        - ``data`` 中的每一项映射为新树根节点下的一个直接孩子：
          ``key`` → ``TreeNode.name``，``value`` → ``TreeNode.value``。
        - 根节点的 ``value`` / ``name`` 由 ``root_value`` / ``root_name`` 指定；
          ``root_name`` 缺省时退化为 ``root_value``。

        约束与边界（继承自 :meth:`nodes_from_dict` / :meth:`from_subtrees`）：

        - ``data`` 必须是 ``dict``，否则抛 :class:`TypeError`。
        - 空 ``dict`` 合法，结果是一棵只有孤根的树。
        - ``data`` 的 ``value`` 若本身又是 ``dict`` / ``list`` **不会**被递归展开；
          需要严格嵌套 schema 请改用 :meth:`from_dict`。
        - 子节点的顺序与 ``data`` 的迭代顺序一致（Python 3.7+ 即键的插入顺序）。

        本方法等价于::

            children = Tree.nodes_from_dict(data)
            tree = Tree.from_subtrees(
                root_value, children,
                root_name=root_name, deep_copy=False,
            )

        但提供一个直接的入口避免上层重复样板代码。
        """
        children = cls.nodes_from_dict(data)
        return cls.from_subtrees(
            root_value, children, root_name=root_name, deep_copy=False
        )

    def insert_parent(
        self,
        value: Any,
        target_value: Any = _MISSING,
        *,
        name: Optional[Any] = None,
        predicate: Optional[Callable[[TreeNode], bool]] = None,
    ) -> TreeNode:
        """在指定子节点之上插入一个新父节点（"套一层壳"）。

        把目标节点 ``T`` 套上一层新父节点 ``P`` —— 树形上等价于在 ``T`` 与原父节点
        之间插入一层：

        - 若 ``T`` 不是根：``P`` 占据 ``T`` 在原父节点 ``children`` 列表中的**原
          索引位置**（保留兄弟顺序），``T`` 成为 ``P`` 的唯一孩子。
        - 若 ``T`` 是根：``P`` 成为新的根，原根 ``T`` 成为 ``P`` 的唯一孩子。

        定位 ``T`` 的规则（优先级从高到低）：

        1. 若提供 ``predicate``：按谓词 BFS 找首个匹配；找不到抛
           :class:`ValueError`。
        2. 否则若 ``target_value`` **缺省**：直接以**当前根**作为目标，等价于
           "在根之上提升一层"的语义糖。
        3. 否则按 ``target_value`` BFS 匹配；显式传入 ``None`` / ``0`` / ``""``
           等 falsy 值同样会被当作合法查询键（与 :meth:`add` 的 ``parent_value``
           处理一致），找不到抛 :class:`ValueError`。

        参数：

        - ``value`` / ``name``：新父节点 ``P`` 的 ``value`` / ``name``；``name``
          缺省时等于 ``value``。

        返回新创建的父节点 ``P``；空树调用抛 :class:`ValueError`。

        插入完成后整棵树的 ``level`` / ``order`` 会被自动刷新——``T`` 子树整体
        下沉一层，其它分支不受影响。
        """
        if self.root is None:
            raise ValueError("空树无法插入父节点")

        if predicate is not None:
            target = self.find(predicate=predicate)
            if target is None:
                raise ValueError("未找到符合 predicate 的目标节点")
        elif target_value is _MISSING:
            target = self.root
        else:
            target = self.find(target_value)
            if target is None:
                raise ValueError(f"未找到目标节点: {target_value!r}")

        new_parent = TreeNode(value, name=name)

        if target is self.root:
            # 新父节点替换为根，原根挂到它下面；level 0 由构造时 parent=None 决定，
            # 调用 _refresh_levels 会把 target 子树整体 +1。
            target.parent = new_parent
            new_parent.children.append(target)
            self.root = new_parent
            new_parent._refresh_levels()
            self.refresh_orders()
            return new_parent

        parent = target.parent
        assert parent is not None
        idx = parent.children.index(target)
        new_parent.parent = parent
        parent.children[idx] = new_parent
        target.parent = new_parent
        new_parent.children.append(target)
        # _refresh_levels 会先基于 new_parent.parent 重算 new_parent.level，
        # 再 BFS 把 target 子树的 level 全部下移一层。
        new_parent._refresh_levels()
        self.refresh_orders()
        return new_parent

    def update_value(self, value: Any, new_value: Any) -> bool:
        """将**值**等于 ``value`` 的第一个节点的 ``value`` 改为 ``new_value``。

        查找按 BFS 返回首个匹配；找到并修改成功返回 ``True``，否则 ``False``。
        仅修改节点的 ``value``，不影响 ``name``。
        """
        node = self.find(value)
        if node is None:
            return False
        node.value = new_value
        return True

    def update_value_all(
        self,
        value: Any = None,
        new_value: Any = None,
        *,
        predicate: Optional[Callable[[TreeNode], bool]] = None,
    ) -> int:
        """批量将**所有**匹配节点的 ``value`` 改为 ``new_value``，返回修改数量。"""
        nodes = self.find_all(value, predicate=predicate)
        for node in nodes:
            node.value = new_value
        return len(nodes)

    def update_name(self, value: Any, new_name: Any) -> bool:
        """将**值**等于 ``value`` 的第一个节点的 ``name`` 改为 ``new_name``。

        查找按 BFS 返回首个匹配；找到并修改成功返回 ``True``，否则 ``False``。
        仅修改节点的 ``name``，不影响 ``value``。
        """
        node = self.find(value)
        if node is None:
            return False
        node.name = new_name
        return True

    def update_name_all(
        self,
        value: Any = None,
        new_name: Any = None,
        *,
        predicate: Optional[Callable[[TreeNode], bool]] = None,
    ) -> int:
        """批量将**所有**匹配节点的 ``name`` 改为 ``new_name``，返回修改数量。"""
        nodes = self.find_all(value, predicate=predicate)
        for node in nodes:
            node.name = new_name
        return len(nodes)

    def find(
        self,
        value: Any = None,
        *,
        predicate: Optional[Callable[[TreeNode], bool]] = None,
    ) -> Optional[TreeNode]:
        """按值或自定义谓词查找节点（BFS，返回第一个匹配）。

        传入 ``predicate`` 时按谓词匹配，否则按 ``value`` 匹配。签名与
        :meth:`find_all` 一致，因此仅传 ``predicate`` 即可。
        """
        if self.root is None:
            return None
        check = predicate if predicate is not None else (lambda n: n.value == value)
        for node in self.bfs():
            if check(node):
                return node
        return None

    def find_all(
        self,
        value: Any = None,
        *,
        predicate: Optional[Callable[[TreeNode], bool]] = None,
    ) -> list[TreeNode]:
        """按值或自定义谓词查找**所有**匹配节点（BFS 顺序）。

        传入 ``predicate`` 时按谓词匹配，否则按 ``value`` 匹配。
        """
        if self.root is None:
            return []
        check = predicate if predicate is not None else (lambda n: n.value == value)
        return [node for node in self.bfs() if check(node)]

    # ---------- 遍历 ----------

    def bfs(self, start: Optional[TreeNode] = None) -> Iterator[TreeNode]:
        """广度优先遍历。"""
        start = start if start is not None else self.root
        if start is None:
            return
        queue: deque[TreeNode] = deque([start])
        while queue:
            node = queue.popleft()
            yield node
            queue.extend(node.children)

    def dfs_preorder(self, start: Optional[TreeNode] = None) -> Iterator[TreeNode]:
        """深度优先——先序遍历（迭代实现，避免深树爆栈）。"""
        start = start if start is not None else self.root
        if start is None:
            return
        stack: list[TreeNode] = [start]
        while stack:
            node = stack.pop()
            yield node
            # 逆序入栈，确保出栈顺序等于原来的左到右。
            stack.extend(reversed(node.children))

    def dfs_postorder(self, start: Optional[TreeNode] = None) -> Iterator[TreeNode]:
        """深度优先——后序遍历（迭代实现，避免深树爆栈）。"""
        start = start if start is not None else self.root
        if start is None:
            return
        stack: list[tuple[TreeNode, bool]] = [(start, False)]
        while stack:
            node, visited = stack.pop()
            if visited:
                yield node
            else:
                stack.append((node, True))
                for child in reversed(node.children):
                    stack.append((child, False))

    def traverse_by_order(
        self, start: Optional[TreeNode] = None
    ) -> Iterator[TreeNode]:
        """按 ``order`` 升序、``level`` 降序遍历节点。

        - 主排序键：``order`` 从小到大。
        - 次排序键（``order`` 相同时）：``level`` 从深到浅（数值大的先出）。
        - ``start`` 缺省时从根节点开始；否则只遍历以 ``start`` 为根的子树
          （使用的是节点当前全局 ``order``，并不会为子树重新编号）。

        在当前"叶子 DFS 先序全局编号 + 非叶节点取 max(children.order)"的 ``order``
        方案下，这个顺序与 DFS 后序遍历结果一致，但语义上按属性排序更直观。
        复杂度 O(N log N)（排序主导）。
        """
        start = start if start is not None else self.root
        if start is None:
            return
        nodes = list(self.bfs(start))
        nodes.sort(key=lambda n: (n.order, -n.level))
        yield from nodes

    # ---------- 序列化 / 持久化 ----------

    def to_dict(self, node: Optional[TreeNode] = None) -> Optional[dict]:
        """序列化为嵌套 dict（迭代实现，避免深树爆栈）。

        包含 ``value`` / ``name`` / ``level`` / ``order`` / ``children`` 五个字段。
        其中 ``level`` 和 ``order`` 仅用于展示；``from_dict`` 加载时会忽略它们，
        由结构重新推导，所以手工编辑 JSON 时无需担心写错这两个值。
        """
        node = node if node is not None else self.root
        if node is None:
            return None
        dict_of: dict[int, dict] = {}
        stack: list[tuple[TreeNode, bool]] = [(node, False)]
        while stack:
            cur, visited = stack.pop()
            if visited:
                dict_of[id(cur)] = {
                    "value": cur.value,
                    "name": cur.name,
                    "level": cur.level,
                    "order": cur.order,
                    "children": [dict_of[id(c)] for c in cur.children],
                }
            else:
                stack.append((cur, True))
                for child in reversed(cur.children):
                    stack.append((child, False))
        return dict_of[id(node)]

    def save(
        self,
        directory: str | Path = "save_tree",
        *,
        overwrite: bool = True,
    ) -> Path:
        """将当前树按根节点 ``name`` 保存为 JSON 文件到指定文件夹（默认 ``save_tree``）。

        文件名为 ``<根节点 name>.json``；``name`` 中不允许出现在文件名里的字符会被
        替换为 ``_``。若 ``name`` 清理后为空串，则回退为 ``"unknown_tree"``。返回保存后的
        文件路径。

        ``overwrite`` 默认为 ``True``，即保持历史行为：同名文件会被直接覆盖。
        传入 ``overwrite=False`` 时，遇到同名文件会抛 :class:`FileExistsError`，
        调用方可据此避免不小心覆盖他人的保存。
        """
        if self.root is None:
            raise ValueError("空树无法保存")

        save_dir = Path(directory)
        save_dir.mkdir(parents=True, exist_ok=True)

        raw_name = str(self.root.name)
        safe_name = "".join(
            c if c not in '<>:"/\\|?*' else "_" for c in raw_name
        ).strip() or "unknown_tree"
        file_path = save_dir / f"{safe_name}.json"

        if not overwrite and file_path.exists():
            raise FileExistsError(
                f"目标文件已存在: {file_path}（传 overwrite=True 可强制覆盖）"
            )

        with file_path.open("w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, ensure_ascii=False, indent=2)
        return file_path

    @classmethod
    def from_dict(cls, data: Optional[dict]) -> "Tree":
        """由 ``to_dict`` 生成的嵌套 dict 还原为 :class:`Tree` 对象。

        结构为 ``{"value": ..., "name": ..., "children": [...]}``；``name`` 和
        ``children`` 可缺省，``data`` 为 ``None`` 时返回空树。``level`` / ``order``
        字段若存在会被**忽略**——树构建完成后会根据结构自动重新推导这两个属性。
        """
        tree = cls()
        if data is None:
            return tree
        tree.root = cls._build_node(data, parent=None)
        tree.refresh_orders()
        return tree

    @staticmethod
    def _build_node(data: dict, parent: Optional[TreeNode]) -> TreeNode:
        """由嵌套 dict 迭代构建子树，挂到 ``parent`` 下。"""
        if not isinstance(data, dict) or "value" not in data:
            raise ValueError(f"非法节点数据: {data!r}")
        root_node = TreeNode(data["value"], name=data.get("name"), parent=parent)
        stack: list[tuple[dict, TreeNode]] = [(data, root_node)]
        while stack:
            d, n = stack.pop()
            for child_data in d.get("children", []) or []:
                if not isinstance(child_data, dict) or "value" not in child_data:
                    raise ValueError(f"非法节点数据: {child_data!r}")
                child_node = TreeNode(
                    child_data["value"],
                    name=child_data.get("name"),
                    parent=n,
                )
                n.children.append(child_node)
                stack.append((child_data, child_node))
        return root_node

    @classmethod
    def load(cls, path: str | Path, *, directory: str | Path = "save_tree") -> "Tree":
        """从 JSON 文件加载树。

        ``path`` 可以是：

        * 完整路径（如 ``save_tree/根.json``）；
        * 仅文件名（如 ``根.json``）；
        * 仅根节点名（如 ``根``），自动补全 ``.json`` 后缀。

        当 ``path`` 不含目录部分时，会在 ``directory``（默认 ``save_tree``）下查找。
        """
        file_path = Path(path)
        if file_path.suffix == "":
            file_path = file_path.with_suffix(".json")
        if str(file_path.parent) == ".":
            file_path = Path(directory) / file_path.name

        if not file_path.is_file():
            raise FileNotFoundError(f"未找到树文件: {file_path}")

        with file_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return cls.from_dict(data)

    def _iter_nodes(self) -> Iterator[TreeNode]:
        return self.dfs_preorder()

    def __iter__(self) -> Iterator[Any]:
        for node in self._iter_nodes():
            yield node.value

    def __len__(self) -> int:
        return self.size()

    def __contains__(self, value: Any) -> bool:
        return self.find(value) is not None

    # ---------- 可视化 ----------

    def to_str(self) -> str:
        """以字典格式返回树的字符串表示（空树返回 ``"<空树>"``）。"""
        if self.root is None:
            return "<空树>"
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2)

    def __str__(self) -> str:
        return self.to_str()

    def __repr__(self) -> str:
        return f"Tree(size={self.size()}, height={self.height()})"


if __name__ == "__main__":
    tree = Tree("1", root_name="根节点")
    tree.add("2", parent_value="1", name="左分支")
    tree.add("3", parent_value="1", name="右分支")
    tree.add("4", parent_value="1")
    tree.add("5", parent_value="2")
    tree.add("6", parent_value="2")
    tree.add("7", parent_value="3")
    tree.add("8", parent_value="5")

    print(tree)
    print(f"\n节点总数: {len(tree)}")
    print(f"树的高度: {tree.height()}")
    print(f"是否包含 '5': {'5' in tree}")
    print(f"BFS 值: {[n.value for n in tree.bfs()]}")
    print(f"BFS 名: {[n.name for n in tree.bfs()]}")
    print(f"先序: {[n.value for n in tree.dfs_preorder()]}")
    print(f"后序: {[n.value for n in tree.dfs_postorder()]}")
    print(f"各节点层级: {[(n.value, n.level) for n in tree.bfs()]}")
    print(f"各节点顺序: {[(n.value, n.order) for n in tree.bfs()]}")
    print(f"按 order/level 遍历: {[n.value for n in tree.traverse_by_order()]}")

    saved_path = tree.save()
    print(f"\n已保存到: {saved_path}")

    loaded = Tree.load("根节点")
    print("\n从 JSON 加载回来的树：")
    print(loaded)
    print(f"加载后节点总数: {len(loaded)}")

    tree.update_value("4", "四")
    tree.update_name("3", "新右分支")
    print("\n将值 '4' 改为 '四'、将值为 '3' 的节点名改为 '新右分支' 之后：")
    print(tree)

    tree.remove("2")
    print("\n删除值为 '2' 的节点之后：")
    print(tree)

    # ========== 批量操作演示 ==========
    # 构造一棵含有重复 value 的树，以便看清楚批量操作效果
    batch_tree = Tree.from_dict({
        "value": 0,
        "name": "根",
        "children": [
            {
                "value": 1,
                "name": "A",
                "children": [
                    {"value": 1, "name": "A1"},    # value 重复 → 会被批量命中
                    {"value": 2, "name": "A2"},
                ],
            },
            {
                "value": 1,                        # value 重复
                "name": "B",
                "children": [
                    {"value": 3, "name": "B1"},
                ],
            },
            {"value": 2, "name": "C"},             # value 重复
        ],
    })

    print("\n" + "=" * 40)
    print("批量操作演示（初始树）")
    print("=" * 40)
    print(batch_tree)

    # 1) 批量查找：按值
    print("\n[find_all] 按 value == 1 查找：")
    ones = batch_tree.find_all(1)
    for n in ones:
        print(f"  - name={n.name!r}, value={n.value!r}")

    # 2) 批量查找：用 predicate 表达更复杂的条件
    print("\n[find_all] 按 predicate 查找所有叶子节点：")
    leaves = batch_tree.find_all(predicate=lambda n: n.is_leaf())
    for n in leaves:
        print(f"  - name={n.name!r}, value={n.value!r}")

    # 3) 批量改值：把所有 value == 1 的改成 "ONE"
    n_changed = batch_tree.update_value_all(1, "ONE")
    print(f"\n[update_value_all] 将 value==1 的节点 value 改为 'ONE'，共 {n_changed} 个")

    # 4) 批量改名：用 predicate 给所有叶子节点加前缀
    n_renamed = batch_tree.update_name_all(
        predicate=lambda n: n.is_leaf(),
        new_name="LEAF",
    )
    print(f"[update_name_all] 将所有叶子节点 name 改为 'LEAF'，共 {n_renamed} 个")
    print("\n批量修改后的树：")
    print(batch_tree)

    # 5a) 深拷贝：新树结构完全独立，改新树不影响原树
    sub_deep = batch_tree.deep_copy_subtree(predicate=lambda n: n.name == "B")
    assert sub_deep is not None
    print("\n[deep_copy_subtree]新树：")
    print(sub_deep)
    sub_deep.save()
    print("\n原树保持不变：")
    print(batch_tree)

    # 5b) 浅拷贝：新树与原树共享节点，改新树会同步影响原树
    sub_shallow = batch_tree.shallow_copy_subtree(predicate=lambda n: n.value == 2)
    assert sub_shallow is not None
    print("\n[shallow_copy_subtree]新树：")
    print(sub_shallow)
    sub_shallow.save()
    print("\n原树对应位置也同步变化（因为节点共享）：")
    print(batch_tree)

    # 6) 批量删除：删掉原树里所有 value == "ONE" 的子树
    #    匹配到 A、B、A1，但 A1 是 A 的后代，会随 A 一并删除，实际删除 2 棵
    removed = batch_tree.remove_all(predicate=lambda n: n.value == "ONE")
    print(f"\n[remove_all] 批量删除 value=='ONE' 的子树，实际删除 {removed} 棵")
    print(batch_tree)

    # 7) 用子树替换某个节点
    replace_tree = Tree("root")
    replace_tree.add("a", parent_value="root")
    replace_tree.add("b", parent_value="root")
    replace_tree.add("b-1", parent_value="b")
    replace_tree.add("b-2", parent_value="b")
    replace_tree.add("c", parent_value="root")

    new_sub = Tree("X", root_name="新子树根")
    new_sub.add("X1", parent_value="X")
    new_sub.add("X2", parent_value="X")

    print("\n" + "=" * 40)
    print("replace_with_subtree 演示（替换前）")
    print("=" * 40)
    print(replace_tree)

    ok = replace_tree.replace_with_subtree("b", new_sub)
    print(f"\n[replace_with_subtree] 用新子树替换 value=='b' 的节点：{ok}")
    print(replace_tree)

    # ========== from_subtrees 演示（多个散落子节点 → 戴新根成新树） ==========
    print("\n" + "=" * 40)
    print("from_subtrees 演示")
    print("=" * 40)

    # 准备三块"散落"的素材：两棵独立的小 Tree + 一个游离的 TreeNode
    sub_a = Tree("a", root_name="子树A")
    sub_a.add("a1", parent_value="a")
    sub_a.add("a2", parent_value="a")
    sub_a.add("a1-1", parent_value="a1")

    sub_b = Tree("b", root_name="子树B")
    sub_b.add("b1", parent_value="b")

    loose = TreeNode("c", name="游离节点C")
    loose.add_child("c1")
    loose.add_child("c2")

    print("\n素材 1：sub_a")
    print(sub_a)
    print("\n素材 2：sub_b")
    print(sub_b)
    print(f"\n素材 3：loose TreeNode = {loose!r}（含 2 个孩子）")

    # 1) 默认 deep_copy=True：把三块素材戴上新根 R 组成新树，源数据保持不动
    merged = Tree.from_subtrees("R", [sub_a, sub_b, loose], root_name="统一新根")
    print("\n[from_subtrees] 默认深拷贝合并后的新树：")
    print(merged)
    print(f"合并后节点总数: {len(merged)}（= 1 根 + 4 + 2 + 3）")
    print(f"新树高度: {merged.height()}")
    print(f"BFS: {[n.value for n in merged.bfs()]}")
    print("\n源 sub_a 未受影响（深拷贝）：")
    print(sub_a)

    # 2) deep_copy=False：把现有 Tree 的根直接抽走挂到新根下，源 Tree 会变空
    detach_src = Tree("d", root_name="待抽走的子树D")
    detach_src.add("d1", parent_value="d")
    detach_src.add("d2", parent_value="d")
    print("\n抽走前的 detach_src：")
    print(detach_src)

    moved = Tree.from_subtrees(
        "R2", [detach_src, sub_b], root_name="新根R2", deep_copy=False
    )
    print("\n[from_subtrees deep_copy=False] 抽走 detach_src 与 sub_b 后形成的新树：")
    print(moved)
    print(f"原 detach_src 是否变空: {detach_src.is_empty()}")
    print(f"原 sub_b 是否变空:     {sub_b.is_empty()}")
