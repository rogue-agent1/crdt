#!/usr/bin/env python3
"""CRDTs — conflict-free replicated data types."""
import sys

class GCounter:
    def __init__(self, node_id, n_nodes):
        self.node_id = node_id
        self.counts = [0] * n_nodes
    def increment(self, amount=1):
        self.counts[self.node_id] += amount
    def value(self):
        return sum(self.counts)
    def merge(self, other):
        for i in range(len(self.counts)):
            self.counts[i] = max(self.counts[i], other.counts[i])

class PNCounter:
    def __init__(self, node_id, n_nodes):
        self.p = GCounter(node_id, n_nodes)
        self.n = GCounter(node_id, n_nodes)
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

class LWWRegister:
    def __init__(self):
        self.value_ = None
        self.timestamp = 0
    def set(self, value, timestamp):
        if timestamp > self.timestamp:
            self.value_ = value
            self.timestamp = timestamp
    def get(self):
        return self.value_
    def merge(self, other):
        if other.timestamp > self.timestamp:
            self.value_ = other.value_
            self.timestamp = other.timestamp

def test():
    a = GCounter(0, 3); b = GCounter(1, 3)
    a.increment(5); b.increment(3)
    a.merge(b)
    assert a.value() == 8
    pn1 = PNCounter(0, 2); pn2 = PNCounter(1, 2)
    pn1.increment(10); pn2.decrement(3)
    pn1.merge(pn2)
    assert pn1.value() == 7
    s1 = GSet(); s2 = GSet()
    s1.add("a"); s1.add("b"); s2.add("b"); s2.add("c")
    s1.merge(s2)
    assert s1.value() == frozenset({"a", "b", "c"})
    r1 = LWWRegister(); r2 = LWWRegister()
    r1.set("old", 1); r2.set("new", 2)
    r1.merge(r2)
    assert r1.get() == "new"
    print("  crdt: ALL TESTS PASSED")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "test": test()
    else: print("CRDTs — conflict-free replicated data types")
