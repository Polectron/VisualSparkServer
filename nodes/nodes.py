from abc import ABC, abstractmethod


class AbstractNode(ABC):
    @abstractmethod
    def is_leaf(self):
        return False

    @abstractmethod
    async def accept(self, visitor, data):
        raise NotImplementedError()


class GroupBy(AbstractNode):
    def __init__(self, df, sources):
        self.df = df
        self.sources = sources
        self.value = None

    async def accept(self, visitor, data):
        await visitor.visit(self, data)

    def is_leaf(self):
        return False


class Aggregation(AbstractNode):
    def __init__(self, gd, aggs):
        self.gd = gd
        self.aggs = aggs
        self.value = None

    async def accept(self, visitor, data):
        await visitor.visit(self, data)

    def is_leaf(self):
        return False


class CountNode(AbstractNode):
    def __init__(self, column):
        self.column = column
        self.value = None

    async def accept(self, visitor, data):
        await visitor.visit(self, data)

    def is_leaf(self):
        return False

class AvgNode(AbstractNode):
    def __init__(self, column):
        self.column = column
        self.value = None

    async def accept(self, visitor, data):
        await visitor.visit(self, data)

    def is_leaf(self):
        return False


class MinNode(AbstractNode):
    def __init__(self, column):
        self.column = column
        self.value = None

    async def accept(self, visitor, data):
        await visitor.visit(self, data)

    def is_leaf(self):
        return False


class MaxNode(AbstractNode):
    def __init__(self, column):
        self.column = column
        self.value = None

    async def accept(self, visitor, data):
        await visitor.visit(self, data)

    def is_leaf(self):
        return False


class CSVSource(AbstractNode):
    def __init__(self, source, separator=","):
        self.separator = separator
        self.source = source
        self.value = None

    async def accept(self, visitor, data):
        await visitor.visit(self, data)

    def is_leaf(self):
        return False


class FilterNode(AbstractNode):
    def __init__(self, df, condition):
        self.df = df
        self.condition = condition
        self.value = None

    async def accept(self, visitor, data):
        await visitor.visit(self, data)

    def is_leaf(self):
        return False


class TableNode(AbstractNode):
    def __init__(self, a, id):
        self.a = a
        self.id = id

    async def accept(self, visitor, data):
        await visitor.visit(self, data)

    def is_leaf(self):
        return True
