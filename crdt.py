#!/usr/bin/env python3
"""CRDTs — Conflict-free Replicated Data Types for distributed systems."""
import sys, time, uuid

class GCounter:
    """Grow-only counter."""
    def __init__(self, node_id=None):
        self.node_id = node_id or str(uuid.uuid4())[:8]
        self.counts = {}
    def increment(self, n=1):
        self.counts[self.node_id] = self.counts.get(self.node_id, 0) + n
    def value(self): return sum(self.counts.values())
    def merge(self, other):
        for k, v in other.counts.items():
            self.counts[k] = max(self.counts.get(k, 0), v)

class PNCounter:
    """Positive-Negative counter (increment and decrement)."""
    def __init__(self, node_id=None):
        nid = node_id or str(uuid.uuid4())[:8]
        self.p, self.n = GCounter(nid), GCounter(nid)
    def increment(self, n=1): self.p.increment(n)
    def decrement(self, n=1): self.n.increment(n)
    def value(self): return self.p.value() - self.n.value()
    def merge(self, other): self.p.merge(other.p); self.n.merge(other.n)

class LWWRegister:
    """Last-Writer-Wins Register."""
    def __init__(self): self.value_, self.ts = None, 0
    def set(self, val, ts=None):
        ts = ts or time.monotonic()
        if ts > self.ts: self.value_, self.ts = val, ts
    def get(self): return self.value_
    def merge(self, other):
        if other.ts > self.ts: self.value_, self.ts = other.value_, other.ts

class GSet:
    """Grow-only set."""
    def __init__(self): self.items = set()
    def add(self, item): self.items.add(item)
    def __contains__(self, item): return item in self.items
    def merge(self, other): self.items |= other.items

class ORSet:
    """Observed-Remove Set — add and remove without conflicts."""
    def __init__(self): self.elements = {}; self.tombstones = set()
    def add(self, item):
        tag = str(uuid.uuid4())[:8]
        self.elements.setdefault(item, set()).add(tag)
    def remove(self, item):
        if item in self.elements:
            self.tombstones |= self.elements.pop(item)
    def __contains__(self, item):
        return item in self.elements and bool(self.elements[item] - self.tombstones)
    def values(self):
        return {k for k, tags in self.elements.items() if tags - self.tombstones}
    def merge(self, other):
        for k, tags in other.elements.items():
            self.elements.setdefault(k, set()).update(tags)
        self.tombstones |= other.tombstones

if __name__ == "__main__":
    # PN-Counter demo
    c1, c2 = PNCounter("node1"), PNCounter("node2")
    c1.increment(5); c2.increment(3); c2.decrement(1)
    c1.merge(c2); c2.merge(c1)
    print(f"PNCounter: node1={c1.value()}, node2={c2.value()} (converged: {c1.value()==c2.value()})")
    # OR-Set demo
    s1, s2 = ORSet(), ORSet()
    s1.add("x"); s1.add("y"); s2.add("y"); s2.add("z")
    s1.remove("y")
    s1.merge(s2)
    print(f"ORSet after merge: {s1.values()}")
