import asyncio
import random
import urllib
import datetime

import websockets
import json

from pyspark.sql import SparkSession
from node_executor import NodeExecutor
from nodes.nodes import *
from querybuilder import QueryBuilder


class QueryServer:

    def __init__(self, ctx):
        self.ctx = ctx
        self.queue = asyncio.Queue()

    async def test(self, websocket, message):
        command = json.loads(message)

        if command["action"] == "columns":

            file_name, headers = urllib.request.urlretrieve(command["source"])
            df = self.ctx.read.format('csv').options(header=True, inferSchema=True,
                                                     sep=";").load(file_name)

            response = {"type": "columns", "data": df.columns}
            await websocket.send(json.dumps(response))
        elif command["action"] == "query":

            cn = CSVSource("https://www.ine.es/jaxiT3/files/t/es/csv_bdsc/9689.csv", separator=";")
            fn = FilterNode(cn, "Provincias != 'Total Nacional'")
            fn2 = FilterNode(fn, "`Grupo quinquenal de edad` != 'Total'")
            gb = GroupBy(fn, ["Sexo", "Provincias"])
            ag = Aggregation(gb, [MinNode("Total"), AvgNode("Total"), MaxNode("Total")])
            pn = TableNode(fn2, 1)
            pn2 = TableNode(ag, 2)

            tree = [cn, fn, fn2, gb, ag, pn2, pn]

            await websocket.send(json.dumps({"type": "info", "data": "running query"}))

            ne = NodeExecutor(self.ctx, tree, int(command["limit"]), websocket)
            await ne.run()

    async def producer_handler(self, websocket, path):
        async for message in websocket:
            # produce an item
            print('producing {}'.format(message))

            command = json.loads(message)

            qb = QueryBuilder(command["nodes"])

            tree = qb.build_query()

            await websocket.send(json.dumps({"type": "info", "data": "running query"}))

            ne = NodeExecutor(self.ctx, tree, int(command["limit"]), websocket)

            await self.queue.put(ne)

    async def consumer_handler(self, websocket, path):
        while True:
            # wait for an item from the producer
            item = await self.queue.get()

            # process the item
            print('consuming {}...'.format(item))
            # simulate i/o operation using sleep
            await item.run()
            # await websocket.send(json.dumps({"type": "info", "data": "query finished"}))
            # Notify the queue that the item has been processed
            self.queue.task_done()

    async def handler(self, websocket, path):
        consumer_task = asyncio.ensure_future(self.consumer_handler(websocket, path))
        producer_task = await self.producer_handler(websocket, path)

        await self.queue.join()
        consumer_task.cancel()

    def start_server(self):
        start_server = websockets.serve(self.handler, "0.0.0.0", 8765)
        asyncio.get_event_loop().run_until_complete(start_server)
        asyncio.get_event_loop().run_forever()


if __name__ == "__main__":
    ctx = SparkSession.builder.config("spark.jars.packages", "mysql:mysql-connector-java:8.0.22").getOrCreate()
    server = QueryServer(ctx)
    server.start_server()
