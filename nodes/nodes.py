from abc import ABC, abstractmethod


class AbstractNode(ABC):
    @abstractmethod
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

    def is_leaf(self):
        return False


class Aggregation(AbstractNode):
    def __init__(self, nid):
        self.id = nid
        self.df = None
        self.aggs = []
        self.value = None

    async def accept(self, visitor, data):
        await visitor.visit(self, data)

    def is_leaf(self):
        return False


class CountNode(AbstractNode):
    def __init__(self, nid, column):
        self.id = nid
        self.column = column
        self.value = None

    async def accept(self, visitor, data):
        await visitor.visit(self, data)

    def is_leaf(self):
        return False


class AvgNode(AbstractNode):
    def __init__(self, nid, column):
        self.id = nid
        self.column = column
        self.value = None

    async def accept(self, visitor, data):
        await visitor.visit(self, data)

    def is_leaf(self):
        return False


class MinNode(AbstractNode):
    def __init__(self, nid, column):
        self.id = nid
        self.column = column
        self.value = None

    async def accept(self, visitor, data):
        await visitor.visit(self, data)

    def is_leaf(self):
        return False


class MaxNode(AbstractNode):
    def __init__(self, nid, column):
        self.id = nid
        self.column = column
        self.value = None

    async def accept(self, visitor, data):
        await visitor.visit(self, data)

    def is_leaf(self):
        return False


class SumNode(AbstractNode):
    def __init__(self, nid, column):
        self.id = nid
        self.column = column
        self.value = None

    async def accept(self, visitor, data):
        await visitor.visit(self, data)

    def is_leaf(self):
        return False


class LocalCSVSource(AbstractNode):
    def __init__(self, nid, source, separator=","):
        self.id = nid
        self.separator = separator
        self.source = source
        self.value = None

    async def accept(self, visitor, data):
        await visitor.visit(self, data)

    def is_leaf(self):
        return False


class CSVSource(AbstractNode):
    def __init__(self, nid, source, separator=","):
        self.id = nid
        self.separator = separator
        self.source = source
        self.value = None

    async def accept(self, visitor, data):
        await visitor.visit(self, data)

    def is_leaf(self):
        return False


class JDBCSource(AbstractNode):
    def __init__(self, nid, url, table, user, password):
        self.id = nid
        self.url = url
        self.user = user
        self.password = password
        self.table = table
        self.value = None

    async def accept(self, visitor, data):
        await visitor.visit(self, data)

    def is_leaf(self):
        return False


class FilterNode(AbstractNode):
    def __init__(self, nid, condition):
        self.id = nid
        self.df = None
        self.condition = condition
        self.value = None

    async def accept(self, visitor, data):
        await visitor.visit(self, data)

    def is_leaf(self):
        return False


class SubtractionNode(AbstractNode):
    def __init__(self, nid):
        self.id = nid
        self.df = None
        self.df2 = None
        self.value = None

    async def accept(self, visitor, data):
        await visitor.visit(self, data)

    def is_leaf(self):
        return False


class TableNode(AbstractNode):
    def __init__(self, nid):
        self.id = nid
        self.df = None

    async def accept(self, visitor, data):
        await visitor.visit(self, data)

    def is_leaf(self):
        return True
