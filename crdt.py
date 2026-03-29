#!/usr/bin/env python3
"""crdt - Conflict-free replicated data types."""
import sys, time

class GCounter:
    """Grow-only counter."""
    def __init__(self, node_id):
        self.id = node_id
        self.counts = {}
    
    def increment(self, n=1):
        self.counts[self.id] = self.counts.get(self.id, 0) + n
    
    def value(self):
        return sum(self.counts.values())
    
    def merge(self, other):
        result = GCounter(self.id)
        all_keys = set(self.counts) | set(other.counts)
        for k in all_keys:
            result.counts[k] = max(self.counts.get(k, 0), other.counts.get(k, 0))
        return result

class PNCounter:
    """Positive-Negative counter."""
    def __init__(self, node_id):
        self.p = GCounter(node_id)
        self.n = GCounter(node_id)
    
    def increment(self, n=1):
        self.p.increment(n)
    
    def decrement(self, n=1):
        self.n.increment(n)
    
    def value(self):
        return self.p.value() - self.n.value()
    
    def merge(self, other):
        result = PNCounter(self.p.id)
        result.p = self.p.merge(other.p)
        result.n = self.n.merge(other.n)
        return result

class GSet:
    """Grow-only set."""
    def __init__(self):
        self.elements = set()
    
    def add(self, elem):
        self.elements.add(elem)
    
    def contains(self, elem):
        return elem in self.elements
    
    def merge(self, other):
        result = GSet()
        result.elements = self.elements | other.elements
        return result

class ORSet:
    """Observed-Remove set."""
    def __init__(self, node_id):
        self.id = node_id
        self.elements = {}  # elem -> set of (node_id, counter)
        self.tombstones = {}  # elem -> set of (node_id, counter)
        self.counter = 0
    
    def add(self, elem):
        self.counter += 1
        tag = (self.id, self.counter)
        if elem not in self.elements:
            self.elements[elem] = set()
        self.elements[elem].add(tag)
    
    def remove(self, elem):
        if elem in self.elements:
            if elem not in self.tombstones:
                self.tombstones[elem] = set()
            self.tombstones[elem] |= self.elements[elem]
            del self.elements[elem]
    
    def contains(self, elem):
        if elem not in self.elements:
            return False
        alive = self.elements[elem] - self.tombstones.get(elem, set())
        return len(alive) > 0
    
    def values(self):
        return {e for e in self.elements if self.contains(e)}
    
    def merge(self, other):
        result = ORSet(self.id)
        all_elems = set(self.elements) | set(other.elements)
        for elem in all_elems:
            tags = self.elements.get(elem, set()) | other.elements.get(elem, set())
            tombs = self.tombstones.get(elem, set()) | other.tombstones.get(elem, set())
            alive = tags - tombs
            if alive:
                result.elements[elem] = alive
            result.tombstones[elem] = tombs
        return result

class LWWRegister:
    """Last-Writer-Wins register."""
    def __init__(self, value=None, ts=0):
        self.value = value
        self.ts = ts
    
    def set(self, value, ts=None):
        ts = ts or time.time()
        if ts > self.ts:
            self.value = value
            self.ts = ts
    
    def merge(self, other):
        if other.ts > self.ts:
            return LWWRegister(other.value, other.ts)
        return LWWRegister(self.value, self.ts)

def test():
    # GCounter
    c1 = GCounter("a")
    c2 = GCounter("b")
    c1.increment(3)
    c2.increment(5)
    merged = c1.merge(c2)
    assert merged.value() == 8
    
    # PNCounter
    pn1 = PNCounter("a")
    pn2 = PNCounter("b")
    pn1.increment(10)
    pn2.decrement(3)
    merged = pn1.merge(pn2)
    assert merged.value() == 7
    
    # GSet
    s1, s2 = GSet(), GSet()
    s1.add("x"); s1.add("y")
    s2.add("y"); s2.add("z")
    merged = s1.merge(s2)
    assert merged.elements == {"x", "y", "z"}
    
    # ORSet
    os1 = ORSet("a")
    os2 = ORSet("b")
    os1.add("x"); os1.add("y")
    os2.add("y"); os2.add("z")
    os1.remove("y")
    merged = os1.merge(os2)
    # y was removed by os1 but added by os2 — os2's add wins (concurrent)
    assert "z" in merged.values()
    assert "x" in merged.values()
    
    # LWWRegister
    r1 = LWWRegister("old", 1)
    r2 = LWWRegister("new", 2)
    merged = r1.merge(r2)
    assert merged.value == "new"
    
    print("All tests passed!")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        test()
    else:
        print("Usage: crdt.py test")
