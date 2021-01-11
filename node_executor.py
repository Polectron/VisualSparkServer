import asyncio
import json

from visitors.execution_visitor import ExecutionVisitor


class NodeExecutor:
    def __init__(self, ctx, tree, websocket):
        self.ctx = ctx
        self.tree = tree
        self.websocket = websocket

    async def find_leafs(self):
        leafs = []
        for n in self.tree:
            if n.is_leaf():
                leafs.append(n)

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
