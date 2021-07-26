import asyncio
import json
from nodes.nodes import AbstractNode

from visitors.execution_visitor import ExecutionVisitor


class NodeExecutor:
    def __init__(self, ctx, tree, websocket):
        self.ctx = ctx
        self.tree: dict[int, AbstractNode] = tree
        self.websocket = websocket

    async def find_leafs(self):
        leafs = []
        for node in self.tree.values():
            if node.is_leaf():
                leafs.append(node)

        return leafs

    async def run(self):
        leafs = await self.find_leafs()
        visitor = ExecutionVisitor(self.ctx, self.websocket)
        try:
            for head in leafs:
                print(f"visiting {head}")
                await visitor.visit(head, None)
        except Exception as e:
            await self.websocket.send(json.dumps({"type": "error", "title": "Exception during query execution", "data": str(e)}))
