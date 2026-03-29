#!/usr/bin/env python3
"""crdt - Conflict-free Replicated Data Types."""
import argparse

class GCounter:
    def __init__(self, node_id, n_nodes=3):
        self.id=node_id;self.counts=[0]*n_nodes
    def increment(self,n=1): self.counts[self.id]+=n
    def value(self): return sum(self.counts)
    def merge(self, other):
        for i in range(len(self.counts)): self.counts[i]=max(self.counts[i],other.counts[i])

class PNCounter:
    def __init__(self,nid,n=3): self.p=GCounter(nid,n);self.n=GCounter(nid,n)
    def increment(self,v=1): self.p.increment(v)
    def decrement(self,v=1): self.n.increment(v)
    def value(self): return self.p.value()-self.n.value()
    def merge(self,o): self.p.merge(o.p);self.n.merge(o.n)

class LWWRegister:
    def __init__(self): self.value=None;self.ts=0
    def set(self,v,ts): 
        if ts>self.ts: self.value=v;self.ts=ts
    def merge(self,o):
        if o.ts>self.ts: self.value=o.value;self.ts=o.ts

class ORSet:
    def __init__(self): self.adds={};self.removes=set();self._tag=0
    def add(self,elem):
        self._tag+=1;tag=f"t{self._tag}"
        self.adds.setdefault(elem,set()).add(tag)
    def remove(self,elem):
        if elem in self.adds: self.removes|=self.adds[elem];del self.adds[elem]
    def elements(self):
        result=set()
        for elem,tags in self.adds.items():
            if tags-self.removes: result.add(elem)
        return result
    def merge(self,o):
        for elem,tags in o.adds.items():
            self.adds.setdefault(elem,set()).update(tags)
        self.removes|=o.removes

def main():
    p=argparse.ArgumentParser(description="CRDT types")
    p.add_argument("--type",choices=["gcounter","pncounter","lww","orset"],default="gcounter")
    args=p.parse_args()
    if args.type=="gcounter":
        a,b=GCounter(0),GCounter(1)
        a.increment(3);b.increment(5);a.increment(2)
        print(f"Node A: {a.value()}, Node B: {b.value()}")
        a.merge(b);b.merge(a)
        print(f"After merge: A={a.value()}, B={b.value()}")
    elif args.type=="pncounter":
        a,b=PNCounter(0),PNCounter(1)
        a.increment(10);b.increment(5);a.decrement(3);b.decrement(2)
        a.merge(b);b.merge(a)
        print(f"PN-Counter after merge: A={a.value()}, B={b.value()}")
    elif args.type=="lww":
        a,b=LWWRegister(),LWWRegister()
        a.set("hello",1);b.set("world",2);a.set("foo",3)
        a.merge(b);b.merge(a)
        print(f"LWW after merge: A={a.value}, B={b.value}")
    else:
        a,b=ORSet(),ORSet()
        a.add("x");a.add("y");b.add("y");b.add("z")
        a.remove("y")
        a.merge(b);print(f"OR-Set A after merge: {a.elements()}")

if __name__=="__main__":
    main()
