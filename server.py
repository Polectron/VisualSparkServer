import asyncio
import urllib

import websockets
import json

from pyspark.sql import SparkSession
from node_executor import NodeExecutor
from nodes.nodes import *


class QueryServer:

    def __init__(self, ctx):
        self.ctx = ctx

    async def run_query(self, websocket, path):
        async for message in websocket:
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

                ne = NodeExecutor(self.ctx, tree, command["limit"], websocket)
                await ne.run()

    def start_server(self):
        start_server = websockets.serve(self.run_query, "0.0.0.0", 8765)
        asyncio.get_event_loop().run_until_complete(start_server)
        asyncio.get_event_loop().run_forever()


if __name__ == "__main__":
    ctx = ctx = SparkSession.builder.getOrCreate()
    server = QueryServer(ctx)
    server.start_server()
