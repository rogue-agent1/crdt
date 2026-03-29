#!/usr/bin/env python3
"""crdt - Conflict-free Replicated Data Types (G-Counter, PN-Counter, OR-Set)."""
import sys

class GCounter:
    def __init__(self, node_id, nodes):
        self.node_id = node_id
        self.counts = {n: 0 for n in nodes}
    def increment(self, amount=1):
        self.counts[self.node_id] += amount
    def value(self):
        return sum(self.counts.values())
    def merge(self, other):
        for n in self.counts:
            self.counts[n] = max(self.counts[n], other.counts.get(n, 0))

class PNCounter:
    def __init__(self, node_id, nodes):
        self.p = GCounter(node_id, nodes)
        self.n = GCounter(node_id, nodes)
    def increment(self, amount=1):
        self.p.increment(amount)
    def decrement(self, amount=1):
        self.n.increment(amount)
    def value(self):
        return self.p.value() - self.n.value()
    def merge(self, other):
        self.p.merge(other.p)
        self.n.merge(other.n)

class GSet:
    def __init__(self):
        self.elements = set()
    def add(self, elem):
        self.elements.add(elem)
    def contains(self, elem):
        return elem in self.elements
    def merge(self, other):
        self.elements |= other.elements
    def value(self):
        return frozenset(self.elements)

class ORSet:
    def __init__(self, node_id):
        self.node_id = node_id
        self.elements = {}  # elem -> set of (node, counter)
        self.counter = 0
    def add(self, elem):
        self.counter += 1
        tag = (self.node_id, self.counter)
        if elem not in self.elements:
            self.elements[elem] = set()
        self.elements[elem].add(tag)
    def remove(self, elem):
        self.elements.pop(elem, None)
    def contains(self, elem):
        return elem in self.elements and len(self.elements[elem]) > 0
    def value(self):
        return {e for e, tags in self.elements.items() if tags}
    def merge(self, other):
        all_elems = set(self.elements) | set(other.elements)
        for elem in all_elems:
            mine = self.elements.get(elem, set())
            theirs = other.elements.get(elem, set())
            self.elements[elem] = mine | theirs

def test():
    nodes = ["A", "B"]
    a = GCounter("A", nodes)
    b = GCounter("B", nodes)
    a.increment(3)
    b.increment(5)
    a.merge(b)
    assert a.value() == 8
    # PN-Counter
    pa = PNCounter("A", nodes)
    pb = PNCounter("B", nodes)
    pa.increment(10)
    pb.decrement(3)
    pa.merge(pb)
    assert pa.value() == 7
    # OR-Set
    sa = ORSet("A")
    sb = ORSet("B")
    sa.add("x")
    sb.add("y")
    sa.merge(sb)
    assert sa.value() == {"x", "y"}
    sa.remove("x")
    assert not sa.contains("x")
    assert sa.contains("y")
    print("OK: crdt")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        test()
    else:
        print("Usage: crdt.py test")
