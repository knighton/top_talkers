from redis import StrictRedis


class RedisTopTalkerTracker(object):
    def __init__(self, size=16384, redis_host='localhost', redis_port=6379,
                 redis_table='top_talkers'):
        self.size = size
        self.redis_table = 'top_talkers'
        self.client = StrictRedis(host=redis_host, port=redis_port)

        self.is_saturated = self.decide_is_saturated()

    def decide_is_saturated(self):
        return self.client.zcard(self.redis_table) >= self.size

    def clear(self):
        self.client.zremrangebyrank(self.redis_table, 0, -1)
        self.is_saturated = self.decide_is_saturated()

    def is_full(self):
        if self.is_saturated:
            return True

        self.is_saturated = self.decide_is_saturated()
        return self.is_saturated

    def get(self, key):
        count = self.client.zscore(self.redis_table, key)
        if count is None:
            return count

        return int(count)

    def contains(self, key):
        count = self.client.zscore(self.redis_table, key)
        return count is not None

    def add(self, key):
        # If it's already in there, increment its count and we're done.
        count = self.client.zscore(self.redis_table, key)
        if count is not None:
            self.client.zincrby(self.redis_table, key, 1)
            return

        # Else if the key is new to us but we're full, pop the lowest key/count
        # pair and insert the new key as count + 1.
        if self.is_full():
            keys_counts = self.client.zrange(
                self.redis_table, 0, 0, withscores=True, score_cast_func=int)
            old_count = keys_counts[0][1]
            self.client.zremrangebyrank(self.redis_table, 0, 0)
            new_count = old_count + 1
            self.client.zadd(self.redis_table, new_count, key)
            return

        # Or if the key is new to us and we have space, just insert it.
        self.client.zadd(self.redis_table, 1, key)

    def top_n_keys(self, n):
        return self.client.zrevrange(
            self.redis_table, 0, n - 1, score_cast_func=int)

    def top_n_keys_counts(self, n):
        keys_counts = self.client.zrevrange(
            self.redis_table, 0, n - 1, withscores=True, score_cast_func=int)
        if self.is_full():
            lowest_keys_counts = self.client.zrange(
                self.redis_table, 0, 0, withscores=True, score_cast_func=int)
            the_min = lowest_keys_counts[0][1] - 1
        else:
            the_min = 0
        return map(lambda (key, count): (key, count - the_min), keys_counts)


def main():
    t = RedisTopTalkerTracker(size=4)

    t.clear()

    assert not t.is_full()
    assert not t.contains('cat')
    assert t.top_n_keys(3) == []
    assert t.get('cat') is None
    assert t.top_n_keys_counts(3) == []

    t.add('cat')
    assert not t.is_full()
    assert t.get('cat') == 1
    assert t.contains('cat') 
    assert t.top_n_keys(3) == ['cat']
    assert t.top_n_keys_counts(3) == [('cat', 1)]

    t.add('cat')
    assert not t.is_full()
    assert t.get('cat') == 2
    assert t.contains('cat') 
    assert t.top_n_keys(3) == ['cat']
    assert t.top_n_keys_counts(3) == [('cat', 2)]

    t.add('dog')
    assert not t.is_full()
    assert t.get('cat') == 2
    assert t.contains('cat') 
    assert t.get('dog') == 1
    assert t.contains('dog') 
    assert t.top_n_keys(3) == ['cat', 'dog']
    assert t.top_n_keys_counts(3) == [('cat', 2), ('dog', 1)]

    t.add('llama')
    assert not t.is_full()
    assert t.top_n_keys(3) == ['cat', 'llama', 'dog']
    assert t.top_n_keys_counts(3) == [('cat', 2), ('llama', 1), ('dog', 1)]

    t.add('goose')
    assert t.is_full()
    assert t.top_n_keys(3) == ['cat', 'llama', 'goose']
    assert t.top_n_keys_counts(3) == [('cat', 2), ('llama', 1), ('goose', 1)]

    t.add('dog')
    assert t.is_full()
    assert t.top_n_keys(3) == ['dog', 'cat', 'llama']
    assert t.top_n_keys_counts(3) == [('dog', 2), ('cat', 2), ('llama', 1)]

    t.add('mouse')
    assert t.is_full()
    assert t.top_n_keys(3) == ['mouse', 'dog', 'cat']
    assert t.top_n_keys_counts(3) == [('mouse', 2), ('dog', 2), ('cat', 2)]

    t.add('llama')
    assert t.is_full()
    assert t.top_n_keys(3) == ['mouse', 'llama', 'dog']
    assert t.top_n_keys_counts(3) == [('mouse', 1), ('llama', 1), ('dog', 1)]

    t.add('goose')
    assert t.is_full()
    assert t.top_n_keys(3) == ['goose', 'mouse', 'llama']
    assert t.top_n_keys_counts(3) == [('goose', 2), ('mouse', 1), ('llama', 1)]


if __name__ == '__main__':
    main()
