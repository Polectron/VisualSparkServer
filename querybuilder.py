from nodes.nodes import AbstractNode, CountNode, CounterNode, JDBCSource, FilterNode, MapNode, TableNode, Aggregation, AvgNode, SumNode, MinNode, \
    MaxNode, GroupBy, SubtractionNode, CSVSource, MongoDBSource, LimitNode


def get_control(name, controls):
    return list(filter(lambda x: x["name"] == name, controls))[0]["value"]


class QueryBuilder:
    def __init__(self, nodes):
        self.nodes = nodes

    def build_query(self):
        query: dict[int, AbstractNode] = {}

        for node in self.nodes.values():
            n: AbstractNode
            print(node)
            if node["type"] == "csvsource":
                source = list(filter(lambda x: x["name"] == "source", node["controls"]))[0]["value"]
                separator = list(filter(lambda x: x["name"] == "separator", node["controls"]))[0]["value"]
                n = CSVSource(node["id"], source, separator)
            elif node["type"] == "jdbcsource":
                driver = get_control("driver", node["controls"])
                url = get_control("url", node["controls"])
                database = get_control("database", node["controls"])
                table = get_control("table", node["controls"])
                username = get_control("user", node["controls"])
                password = get_control("password", node["controls"])
                n = JDBCSource(node["id"], driver, url, database, table, username, password)
            elif node["type"] == "mongodbsource":
                url = get_control("url", node["controls"])
                database = get_control("database", node["controls"])
                table = get_control("table", node["controls"])
                username = get_control("user", node["controls"])
                password = get_control("password", node["controls"])
                n = MongoDBSource(node["id"], url, database, table, username, password)
            elif node["type"] == "filter":
                condition = list(filter(lambda x: x["name"] == "condition", node["controls"]))[0]["value"]
                n = FilterNode(node["id"], condition)
            elif node["type"] == "subtraction":
                n = SubtractionNode(node["id"])
            elif node["type"] == "groupby":
                columns = list(map(lambda x: x.strip(), get_control("columns", node["controls"]).split(",")))
                n = GroupBy(node["id"], columns)
            elif node["type"] == "aggregation":
                n = Aggregation(node["id"])
            elif node["type"] == "count":
                column = list(filter(lambda x: x["name"] == "column", node["controls"]))[0]["value"]
                n = CountNode(node["id"], column)
            elif node["type"] == "sum":
                column = list(filter(lambda x: x["name"] == "column", node["controls"]))[0]["value"]
                n = SumNode(node["id"], column)
            elif node["type"] == "avg":
                column = list(filter(lambda x: x["name"] == "column", node["controls"]))[0]["value"]
                n = AvgNode(node["id"], column)
            elif node["type"] == "min":
                column = list(filter(lambda x: x["name"] == "column", node["controls"]))[0]["value"]
                n = MinNode(node["id"], column)
            elif node["type"] == "max":
                column = list(filter(lambda x: x["name"] == "column", node["controls"]))[0]["value"]
                n = MaxNode(node["id"], column)
            elif node["type"] == "limit":
                limit = get_control("limit", node["controls"])
                n = LimitNode(node["id"], limit)
            elif node["type"] == "table":
                n = TableNode(node["id"])
            elif node["type"] == "counter":
                n = CounterNode(node["id"])
            elif node["type"] == "map":
                latitude = get_control("latitude", node["controls"])
                longitude = get_control("longitude", node["controls"])
                color = get_control("color", node["controls"])
                n = MapNode(node["id"], latitude, longitude, color)
            else:
                raise NotImplementedError(f"{node['type']} not implemented")
            query[n.id] = n

        for node in self.nodes.values():
            if "inputs" in node and len(node["inputs"]) > 0:
                query[node["id"]].df = query[node["inputs"][0]["connects_to"][0]]
                if node["type"] == "subtraction":
                    query[node["id"]].df2 = query[node["inputs"][1]["connects_to"][0]]

            if node["aggs"]:
                for agg in node["aggs"]["connects_to"]:
                    query[node["id"]].aggs.append(query[agg])

        return query
