# -*- coding: utf-8 -*-
"""
Tree Iteration.

* :any:`PreOrderIter`: iterate over storage_tree using pre-order strategy (self, children)
* :any:`PostOrderIter`: iterate over storage_tree using post-order strategy (children, self)
* :any:`LevelOrderIter`: iterate over storage_tree using level-order strategy
* :any:`LevelOrderGroupIter`: iterate over storage_tree using level-order strategy returning group for every level
* :any:`ZigZagGroupIter`: iterate over storage_tree using level-order strategy returning group for every level
"""

from .abstractiter import AbstractIter  # noqa
from .levelordergroupiter import LevelOrderGroupIter  # noqa
from .levelorderiter import LevelOrderIter  # noqa
from .postorderiter import PostOrderIter  # noqa
from .preorderiter import PreOrderIter  # noqa
from .zigzaggroupiter import ZigZagGroupIter  # noqa
