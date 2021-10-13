import json
import urllib.request
import urllib.parse

from nodes.nodes import *

from functools import singledispatchmethod


class ExecutionVisitor:
    def __init__(self, ctx, websocket):
        self.ctx = ctx
        self.websocket = websocket

    @singledispatchmethod
    async def visit(self, node):
        pass

    @visit.register
    async def _(self, node: JDBCSource, data):
        if node.value is None:
            # TODO: Add connection support for psql

            if node.driver == "mysql":
                driver = "com.mysql.cj.jdbc.Driver"
            else:
                driver = "org.postgresql.Driver"

            uri = f"jdbc:{node.driver}://{node.url}/{node.database}"
            node.value = self.ctx.read.jdbc(uri, node.table,
                                            properties={"driver": driver, "serverTimezone": "UTC",
                                                        "user": node.user, "password": node.password})

    @visit.register
    async def _(self, node: MongoDBSource, data):
        if node.value is None:
            url = f"mongodb://{urllib.parse.quote(node.user)}:{urllib.parse.quote(node.password)}@{node.url}/{node.database}.{node.table}?authSource=admin"
            node.value = self.ctx.read.format("mongo").option("uri", url).load()

    @visit.register
    async def _(self, node: CSVSource, data):
        if node.value is None:
            node.value = self.ctx.read.format('csv').options(header=True, inferSchema=True,
                                                             sep=node.separator).load(node.source)

    @visit.register
    async def _(self, node: CSVSource, data):
        if node.value is None:
            file_name, headers = urllib.request.urlretrieve(node.source)
            node.value = self.ctx.read.format('csv').options(header=True, inferSchema=True,
                                                         sep=node.separator).load(file_name)

    @visit.register
    async def _(self, node: GroupBy, data):
        if node.value is None:
            await node.df.accept(self, data)
            node.value = node.df.value.groupBy(node.sources)

    @visit.register
    async def _(self, node: Aggregation, data):
        if node.value is None:
            await node.df.accept(self, data)
            cols = []
            for agg in node.aggs:
                await agg.accept(self, data)
                cols.append(agg.value)

            node.value = node.df.value.agg(*cols)

    @visit.register
    async def _(self, node: CountNode, data):
        from pyspark.sql.functions import count
        if node.value is None:
            node.value = count(node.column)

    @visit.register
    async def _(self, node: AvgNode, data):
        from pyspark.sql.functions import sum
        if node.value is None:
            node.value = sum(node.column)

    @visit.register
    async def _(self, node: AvgNode, data):
        from pyspark.sql.functions import avg
        if node.value is None:
            node.value = avg(node.column)

    @visit.register
    async def _(self, node: AvgNode, data):
        from pyspark.sql.functions import min
        if node.value is None:
            node.value = min(node.column)

    @visit.register
    async def _(self, node: AvgNode, data):
        from pyspark.sql.functions import max
        if node.value is None:
            node.value = max(node.column)

    @visit.register
    async def _(self, node: FilterNode, data):
        if node.value is None:
            await node.df.accept(self, data)
            node.value = node.df.value.filter(node.condition)

    @visit.register
    async def _(self, node: LimitNode, data):
        if node.value is None:
            await node.df.accept(self, data)
            node.value = node.df.value.limit(node.limit)

    @visit.register
    async def _(self, node: SubtractionNode, data):
        if node.value is None:
            await node.df.accept(self, data)
            await node.df2.accept(self, data)
            node.value = node.df.value.subtract(node.df2.value)

    @visit.register
    async def _(self, node: TableNode, data):
        await node.df.accept(self, data)
        # v = list(map(lambda row: row.asDict(), node.df.value.collect()))
        v = list(map(lambda x: json.loads(x), node.df.value.toJSON().collect()))
        message = {"type": "table", "id": node.id, "data": v}
        await self.websocket.send(json.dumps(message))

    @visit.register
    async def _(self, node: CounterNode, data):
        await node.df.accept(self, data)
        v = node.df.value.count()
        message = {"type": "counter", "id": node.id, "data": v}
        await self.websocket.send(json.dumps(message))

    @visit.register
    async def _(self, node: MapNode, data):
        await node.df.accept(self, data)
        # v = list(map(lambda row: row.asDict(), node.df.value.collect()))
        v = list(map(lambda x: json.loads(x), node.df.value.toJSON().collect()))
        message = {"type": "map", "id": node.id, "data": v, "latitude": node.latitude, "longitude": node.longitude, "color": node.color}
        await self.websocket.send(json.dumps(message))

    @visit.register
    async def _(self, node: GraphNode, data):
        await node.df.accept(self, data)
        # v = list(map(lambda row: row.asDict(), node.df.value.collect()))
        v = list(map(lambda x: json.loads(x), node.df.value.toJSON().collect()))
        message = {"type": "graph", "id": node.id, "data": v, "x": node.x, "y": node.y, "graph_type": node.type}
        await self.websocket.send(json.dumps(message))
