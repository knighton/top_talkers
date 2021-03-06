from redis import StrictRedis


LOCK_PREFIX = 'LOCK:'

TIMEOUT = 10


class RedisTopTalkerTracker(object):
    def __init__(self, redis_host='localhost', redis_port=6379):
        self.client = StrictRedis(host=redis_host, port=redis_port)
        self.locks = {}

    def get_lock(self, redis_table):
        r = self.locks.get(redis_table)
        if r:
            return r

        r = self.client.lock(LOCK_PREFIX + redis_table, timeout=TIMEOUT)
        self.locks[redis_table] = r
        return r

    def is_full_inner(self, redis_table, redis_size):
        return self.client.zcard(redis_table) >= redis_size

    def clear(self, redis_table):
        """
        table -> None
        """
        lock = self.get_lock(redis_table)
        lock.acquire()
        self.client.zremrangebyrank(redis_table, 0, -1)
        lock.release()

    def is_full(self, redis_table, redis_size):
        """
        (table, size) -> bool
        """
        lock = self.get_lock(redis_table)
        lock.acquire()
        r = self.is_full_inner(redis_table, redis_size)
        lock.release()
        return r

    def get(self, redis_table, key):
        """
        (table, key) -> count or None
        """
        lock = self.get_lock(redis_table)
        lock.acquire()
        count = self.client.zscore(redis_table, key)
        lock.release()

        if count is None:
            return count

        return int(count)

    def contains(self, redis_table, key):
        """
        (table, key) -> bool
        """
        lock = self.get_lock(redis_table)
        lock.acquire()
        count = self.client.zscore(redis_table, key)
        lock.release()
        return count is not None

    def add(self, redis_table, redis_size, key):
        """
        (table, size, key) -> None
        """
        lock = self.get_lock(redis_table)
        lock.acquire()

        # If it's already in there, increment its count and we're done.
        count = self.client.zscore(redis_table, key)
        if count is not None:
            self.client.zincrby(redis_table, key, 1)
            lock.release()
            return

        # Else if the key is new to us but we're full, pop the lowest key/count
        # pair and insert the new key as count + 1.
        if self.is_full_inner(redis_table, redis_size):
            keys_counts = self.client.zrange(
                redis_table, 0, 0, withscores=True, score_cast_func=int)
            old_count = keys_counts[0][1]
            self.client.zremrangebyrank(redis_table, 0, 0)
            new_count = old_count + 1
            self.client.zadd(redis_table, new_count, key)
            lock.release()
            return

        # Or if the key is new to us and we have space, just insert it.
        self.client.zadd(redis_table, 1, key)
        lock.release()

    def top_n_keys(self, redis_table, n):
        """
        (table, n) -> list of keys
        """
        lock = self.get_lock(redis_table)
        lock.acquire()
        rr = self.client.zrevrange(
            redis_table, 0, n - 1, score_cast_func=int)
        lock.release()
        return rr

    def top_n_keys_counts(self, redis_table, redis_size, n):
        """
        (table, size, n) -> list of (key, count)
        """
        lock = self.get_lock(redis_table)
        lock.acquire()
        keys_counts = self.client.zrevrange(
            redis_table, 0, n - 1, withscores=True, score_cast_func=int)
        if self.is_full_inner(redis_table, redis_size):
            lowest_keys_counts = self.client.zrange(
                redis_table, 0, 0, withscores=True, score_cast_func=int)
            the_min = lowest_keys_counts[0][1] - 1
        else:
            the_min = 0
        lock.release()
        return map(lambda (key, count): (key, count - the_min), keys_counts)


def main():
    redis_table = 'top_talkers'
    redis_size = 4

    t = RedisTopTalkerTracker()

    t.clear(redis_table)

    assert not t.is_full(redis_table, redis_size)
    assert not t.contains(redis_table, 'cat')
    assert t.top_n_keys(redis_table, 3) == []
    assert t.get(redis_table, 'cat') is None
    assert t.top_n_keys_counts(redis_table, redis_size, 3) == []

    t.add(redis_table, redis_size, 'cat')
    assert not t.is_full(redis_table, redis_size)
    assert t.get(redis_table, 'cat') == 1
    assert t.contains(redis_table, 'cat')
    assert t.top_n_keys(redis_table, 3) == ['cat']
    assert t.top_n_keys_counts(redis_table, redis_size, 3) == [('cat', 1)]

    t.add(redis_table, redis_size, 'cat')
    assert not t.is_full(redis_table, redis_size)
    assert t.get(redis_table, 'cat') == 2
    assert t.contains(redis_table, 'cat')
    assert t.top_n_keys(redis_table, 3) == ['cat']
    assert t.top_n_keys_counts(redis_table, redis_size, 3) == [('cat', 2)]

    t.add(redis_table, redis_size, 'dog')
    assert not t.is_full(redis_table, redis_size)
    assert t.get(redis_table, 'cat') == 2
    assert t.contains(redis_table, 'cat')
    assert t.get(redis_table, 'dog') == 1
    assert t.contains(redis_table, 'dog')
    assert t.top_n_keys(redis_table, 3) == ['cat', 'dog']
    assert t.top_n_keys_counts(redis_table, redis_size, 3) == [('cat', 2), ('dog', 1)]

    t.add(redis_table, redis_size, 'llama')
    assert not t.is_full(redis_table, redis_size)
    assert t.top_n_keys(redis_table, 3) == ['cat', 'llama', 'dog']
    assert t.top_n_keys_counts(redis_table, redis_size, 3) == [('cat', 2), ('llama', 1), ('dog', 1)]

    t.add(redis_table, redis_size, 'goose')
    assert t.is_full(redis_table, redis_size)
    assert t.top_n_keys(redis_table, 3) == ['cat', 'llama', 'goose']
    assert t.top_n_keys_counts(redis_table, redis_size, 3) == [('cat', 2), ('llama', 1), ('goose', 1)]

    t.add(redis_table, redis_size, 'dog')
    assert t.is_full(redis_table, redis_size)
    assert t.top_n_keys(redis_table, 3) == ['dog', 'cat', 'llama']
    assert t.top_n_keys_counts(redis_table, redis_size, 3) == [('dog', 2), ('cat', 2), ('llama', 1)]

    t.add(redis_table, redis_size, 'mouse')
    assert t.is_full(redis_table, redis_size)
    assert t.top_n_keys(redis_table, 3) == ['mouse', 'dog', 'cat']
    assert t.top_n_keys_counts(redis_table, redis_size, 3) == [('mouse', 2), ('dog', 2), ('cat', 2)]

    t.add(redis_table, redis_size, 'llama')
    assert t.is_full(redis_table, redis_size)
    assert t.top_n_keys(redis_table, 3) == ['mouse', 'llama', 'dog']
    assert t.top_n_keys_counts(redis_table, redis_size, 3) == [('mouse', 1), ('llama', 1), ('dog', 1)]

    t.add(redis_table, redis_size, 'goose')
    assert t.is_full(redis_table, redis_size)
    assert t.top_n_keys(redis_table, 3) == ['goose', 'mouse', 'llama']
    assert t.top_n_keys_counts(redis_table, redis_size, 3) == [('goose', 2), ('mouse', 1), ('llama', 1)]

    t.clear(redis_table)


if __name__ == '__main__':
    main()
