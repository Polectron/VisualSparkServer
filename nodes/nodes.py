from abc import ABC, abstractmethod


class AbstractNode(ABC):
    def __init__(self, nid):
        self.id = nid
        self.df = None

    def is_leaf(self):
        return False

    @abstractmethod
    async def accept(self, visitor, data):
        raise NotImplementedError()


class GroupBy(AbstractNode):
    def __init__(self, nid, sources):
        self.id = nid
        self.df = None
        self.sources = sources
        self.value = None

    async def accept(self, visitor, data):
        await visitor.visit(self, data)


class Aggregation(AbstractNode):
    def __init__(self, nid):
        self.id = nid
        self.df = None
        self.aggs = []
        self.value = None

    async def accept(self, visitor, data):
        await visitor.visit(self, data)


class CountNode(AbstractNode):
    def __init__(self, nid, column):
        self.id = nid
        self.column = column
        self.value = None

    async def accept(self, visitor, data):
        await visitor.visit(self, data)


class AvgNode(AbstractNode):
    def __init__(self, nid, column):
        self.id = nid
        self.column = column
        self.value = None

    async def accept(self, visitor, data):
        await visitor.visit(self, data)


class MinNode(AbstractNode):
    def __init__(self, nid, column):
        self.id = nid
        self.column = column
        self.value = None

    async def accept(self, visitor, data):
        await visitor.visit(self, data)


class MaxNode(AbstractNode):
    def __init__(self, nid, column):
        self.id = nid
        self.column = column
        self.value = None

    async def accept(self, visitor, data):
        await visitor.visit(self, data)


class SumNode(AbstractNode):
    def __init__(self, nid, column):
        self.id = nid
        self.column = column
        self.value = None

    async def accept(self, visitor, data):
        await visitor.visit(self, data)


class LocalCSVSource(AbstractNode):
    def __init__(self, nid, source, separator=","):
        self.id = nid
        self.separator = separator
        self.source = source
        self.value = None

    async def accept(self, visitor, data):
        await visitor.visit(self, data)


class CSVSource(AbstractNode):
    def __init__(self, nid, source, separator=","):
        self.id = nid
        self.separator = separator
        self.source = source
        self.value = None

    async def accept(self, visitor, data):
        await visitor.visit(self, data)


class JDBCSource(AbstractNode):
    def __init__(self, nid, driver, url, database, table, user, password):
        self.id = nid
        self.driver = driver
        self.url = url
        self.user = user
        self.password = password
        self.database = database
        self.table = table
        self.value = None

    async def accept(self, visitor, data):
        await visitor.visit(self, data)


class MongoDBSource(AbstractNode):
    def __init__(self, nid, url, database, table, user, password):
        self.id = nid
        self.url = url
        self.user = user
        self.password = password
        self.database = database
        self.table = table
        self.value = None

    async def accept(self, visitor, data):
        await visitor.visit(self, data)


class FilterNode(AbstractNode):
    def __init__(self, nid, condition):
        self.id = nid
        self.df = None
        self.condition = condition
        self.value = None

    async def accept(self, visitor, data):
        await visitor.visit(self, data)


class LimitNode(AbstractNode):
    def __init__(self, nid, limit: int):
        self.id = nid
        self.df = None
        self.limit = limit
        self.value = None

    async def accept(self, visitor, data):
        await visitor.visit(self, data)


class SampleNode(AbstractNode):
    def __init__(self, nid, portion: int):
        self.id = nid
        self.df = None
        self.portion = portion
        self.value = None

    async def accept(self, visitor, data):
        await visitor.visit(self, data)


class SubtractionNode(AbstractNode):
    def __init__(self, nid):
        self.id = nid
        self.df = None
        self.df2 = None
        self.value = None

    async def accept(self, visitor, data):
        await visitor.visit(self, data)


class OutputNode(AbstractNode, ABC):
    def is_leaf(self):
        return True


class TableNode(OutputNode):
    async def accept(self, visitor, data):
        await visitor.visit(self, data)


class CounterNode(OutputNode):
    async def accept(self, visitor, data):
        await visitor.visit(self, data)


class MapNode(OutputNode):
    def __init__(self, nid, latitude: str, longitude: str, color: str):
        super().__init__(nid)
        self.latitude = latitude
        self.longitude = longitude
        self.color = color

    async def accept(self, visitor, data):
        await visitor.visit(self, data)


class GraphNode(OutputNode):
    def __init__(self, nid, x: str, y: str, type: str):
        super().__init__(nid)
        self.x = x
        self.y = y
        self.type = type

    async def accept(self, visitor, data):
        await visitor.visit(self, data)
