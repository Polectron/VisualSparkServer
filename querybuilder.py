from nodes.nodes import JDBCSource, FilterNode, TableNode, Aggregation, AvgNode, SumNode, MinNode, \
    MaxNode, GroupBy, SubtractionNode, CSVSource


def get_control(name, controls):
    return list(filter(lambda x: x["name"] == name, controls))[0]["value"]


class QueryBuilder:
    def __init__(self, nodes):
        self.nodes = nodes

    def build_query(self):
        query = []

        for node in self.nodes:
            n = None
            if node["type"] == "csvsource":
                source = list(filter(lambda x: x["name"] == "source", node["controls"]))[0]["value"]
                separator = list(filter(lambda x: x["name"] == "separator", node["controls"]))[0]["value"]
                n = CSVSource(node["id"], source, separator)
            elif node["type"] == "jdbcsource":
                url = list(filter(lambda x: x["name"] == "url", node["controls"]))[0]["value"]
                table = list(filter(lambda x: x["name"] == "table", node["controls"]))[0]["value"]
                username = get_control("user", node["controls"])
                password = get_control("password", node["controls"])
                n = JDBCSource(node["id"], url, table, username, password)
            elif node["type"] == "filter":
                condition = list(filter(lambda x: x["name"] == "condition", node["controls"]))[0]["value"]
                n = FilterNode(node["id"], condition)
            elif node["type"] == "subtraction":
                n = SubtractionNode(node["id"])
            elif node["type"] == "table":
                n = TableNode(node["id"])
            elif node["type"] == "groupby":
                columns = list(map(lambda x: x.strip(), get_control("columns", node["controls"]).split(",")))
                n = GroupBy(node["id"], columns)
            elif node["type"] == "aggregation":
                n = Aggregation(node["id"])
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

            query.append(n)

        for node in self.nodes:
            if "inputs" in node and len(node["inputs"]) > 0:
                query[node["id"]].df = query[node["inputs"][0]["connects_to"][0]]
                if node["type"] == "subtraction":
                    query[node["id"]].df2 = query[node["inputs"][1]["connects_to"][0]]

            if node["aggs"]:
                for agg in node["aggs"]["connects_to"]:
                    query[node["id"]].aggs.append(query[agg])

        return query
