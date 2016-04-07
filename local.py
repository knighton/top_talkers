import heapq


class TopTalkerTrackerItem(object):
    def __init__(self, key, count, data):
        self.key = key
        self.count = count
        self.data = data

    def __eq__(self, other):
        return self.count == other.count

    def __lt__(self, other):
        return self.count < other.count


class TopTalkerTracker(object):
    def __init__(self, size):
        self.size = size
        self.is_saturated = False
        self.key2entry = {}
        self.heap = []

    def is_full(self):
        if self.is_saturated:
            return True

        self.is_saturated = len(self.heap) >= self.size
        return self.is_saturated

    def get(self, key):
        return self.key2entry[key]

    def contains(self, key):
        e = self.key2entry.get(key)
        return bool(e)

    def add(self, key, data):
        item = self.key2entry.get(key)
        if item:
            item.count += 1
            item.data = data
            heapq.heapreplace(self.heap, item)
            return

        if self.is_full():
            old = heapq.heappop(self.heap)
            del self.key2entry[old.key]
            old.key = key
            old.count += 1
            old.data = data
            self.key2entry[key] = old
            heapq.heappush(self.heap, old)
            return

        item = TopTalkerTrackerItem(key, 1, data)
        heapq.heappush(self.heap, item)
        self.key2entry[key] = item

    def top_n(self, n):
        members = heapq.nlargest(n, self.heap)
        if self.is_full():
            the_min = self.heap[0].count - 1
        else:
            the_min = 0
        return map(lambda m:
            TopTalkerTrackerItem(m.key, m.count - the_min, m.data), members)


def main():
    t = TopTalkerTracker(16384)
    t.add('cat', 1389235982398)
    print map(lambda a: a.key, t.top_n(5))
    t.add('dog', 39013923592)
    t.add('cat', 1389235982398)
    print map(lambda a: a.key, t.top_n(5))
    t.add('parrot', 23901)
    t.add('mouse', 1091091)
    t.add('cow', 20912901)
    t.add('horse', 149014901)
    print map(lambda a: a.key, t.top_n(5))
    print t.get('cat').count
    print t.get('horse').count


if __name__ == '__main__':
    main()
