import json
import urllib.request

from pyspark.sql import DataFrame

from nodes.nodes import *
from visitors import visitor


class ExecutionVisitor:
    def __init__(self, ctx, limit, websocket):
        self.ctx = ctx
        self.limit = limit
        self.websocket = websocket

    @visitor.on('node')
    def visit(self, node):
        pass

    @visitor.when(JDBCSource)
    async def visit(self, node: JDBCSource, data):
        if node.value is None:
            # TODO: Add connection support for psql
            db = node.url.split(":")[0]
            node.value = self.ctx.read.jdbc(node.url, node.table,
                                            properties={"driver": "com.mysql.cj.jdbc.Driver", "serverTimezone": "UTC",
                                                        "user": node.user, "password": node.password})

    @visitor.when(LocalCSVSource)
    async def visit(self, node: CSVSource, data):
        if node.value is None:
            node.value = self.ctx.read.format('csv').options(header=True, inferSchema=True,
                                                             sep=node.separator).load(node.source)

    @visitor.when(CSVSource)
    async def visit(self, node: CSVSource, data):
        if node.value is None:
            file_name, headers = urllib.request.urlretrieve(node.source)
            node.value = self.ctx.read.format('csv').options(header=True, inferSchema=True,
                                                             sep=node.separator).load(file_name)

    @visitor.when(GroupBy)
    async def visit(self, node: GroupBy, data):
        if node.value is None:
            await node.df.accept(self, data)
            node.value = node.df.value.groupBy(node.sources)

    @visitor.when(Aggregation)
    async def visit(self, node: Aggregation, data):
        if node.value is None:
            await node.df.accept(self, data)
            cols = []
            for agg in node.aggs:
                await agg.accept(self, data)
                cols.append(agg.value)

            node.value = node.df.value.agg(*cols)

    @visitor.when(CountNode)
    async def visit(self, node: CountNode, data):
        from pyspark.sql.functions import count
        if node.value is None:
            node.value = count(node.column)

    @visitor.when(AvgNode)
    async def visit(self, node: AvgNode, data):
        from pyspark.sql.functions import avg
        if node.value is None:
            node.value = avg(node.column)

    @visitor.when(MinNode)
    async def visit(self, node: AvgNode, data):
        from pyspark.sql.functions import min
        if node.value is None:
            node.value = min(node.column)

    @visitor.when(MaxNode)
    async def visit(self, node: AvgNode, data):
        from pyspark.sql.functions import max
        if node.value is None:
            node.value = max(node.column)

    @visitor.when(SumNode)
    async def visit(self, node: AvgNode, data):
        from pyspark.sql.functions import sum
        if node.value is None:
            node.value = sum(node.column)

    @visitor.when(FilterNode)
    async def visit(self, node: FilterNode, data):
        if node.value is None:
            await node.df.accept(self, data)
            node.value = node.df.value.filter(node.condition)

    @visitor.when(SubtractionNode)
    async def visit(self, node: SubtractionNode, data):
        if node.value is None:
            await node.df.accept(self, data)
            await node.df2.accept(self, data)
            node.value = node.df.value.subtract(node.df2.value)

    @visitor.when(TableNode)
    async def visit(self, node: TableNode, data):
        await node.df.accept(self, data)
        v = list(map(lambda row: row.asDict(), node.df.value.limit(self.limit).collect()))
        message = {"type": "table", "id": node.id, "data": v}

        if self.websocket is not None:
            await self.websocket.send(json.dumps(message))
        else:
            print(json.dumps(message))
