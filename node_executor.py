import asyncio

from visitors.execution_visitor import ExecutionVisitor


class NodeExecutor:
    def __init__(self, ctx, tree, limit, websocket):
        self.ctx = ctx
        self.tree = tree
        self.limit = limit
        self.websocket = websocket

    async def find_leafs(self):
        leafs = []
        for n in self.tree:
            if n.is_leaf():
                leafs.append(n)

        return leafs

    async def run(self):
        leafs = await self.find_leafs()
        visitor = ExecutionVisitor(self.ctx, self.limit, self.websocket)

        for head in leafs:
            await visitor.visit(head, None)
