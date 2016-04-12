from redis import StrictRedis


LUA_IS_FULL_INNER = """
local table = KEYS[1]
local size = tonumber(KEYS[2])

return redis.call('zcard', table) >= size
"""


LUA_CLEAR = """
local table = KEYS[1]

redis.call('zremrangebyrank', table, 0, -1)
"""

LUA_GET ="""
local table = KEYS[1]
local key = KEYS[2]

return redis.call('zscore', table, key)
"""


LUA_ADD = """
local table = KEYS[1]
local size = tonumber(KEYS[2])
local key = KEYS[3]

local count = redis.call('zscore', table, key)
if count ~= false then
    redis.call('zincrby', table, 1, key)
    return
end

if redis.call('zcard', table) >= size then
    local keys_counts = redis.call('zrange', table, 0, 0, 'withscores')
    local old_count = tonumber(keys_counts[2])
    redis.call('zremrangebyrank', table, 0, 0)
    local new_count = old_count + 1
    redis.call('zadd', table, new_count, key)
    return
end

redis.call('zadd', table, 1, key)
"""


LUA_TOP_N_KEYS = """
local table = KEYS[1]
local n = tonumber(KEYS[2])

return redis.call('zrevrange', table, 0, n - 1)
"""


LUA_TOP_N_KEYS_COUNTS = """
local table = KEYS[1]
local size = tonumber(KEYS[2])
local n = tonumber(KEYS[3])

local keys_counts = redis.call('zrevrange', table, 0, n - 1, 'withscores')
local the_min = nil
if redis.call('zcard', table) >= size then
    local lowest_keys_counts = redis.call('zrange', table, 0, 0, 'withscores')
    the_min = tonumber(lowest_keys_counts[2]) - 1
else
    the_min = 0
end
for i = 1, #keys_counts / 2 do
    local x = (i - 1) * 2 + 2
    keys_counts[x] = tonumber(keys_counts[x]) - the_min
end
return keys_counts
"""


class TopTalkers(object):
    def __init__(self, redis_host='localhost', redis_port=6379):
        self.client = StrictRedis(host=redis_host, port=redis_port)
        self._is_full_inner = self.client.register_script(LUA_IS_FULL_INNER)
        self._clear = self.client.register_script(LUA_CLEAR)
        self._get = self.client.register_script(LUA_GET)
        self._add = self.client.register_script(LUA_ADD)
        self._top_n_keys = self.client.register_script(LUA_TOP_N_KEYS)
        self._top_n_keys_counts = self.client.register_script(
            LUA_TOP_N_KEYS_COUNTS)

    def is_full_inner(self, redis_table, redis_size):
        r = self._is_full_inner(keys=[redis_table, redis_size])
        return bool(r)

    def clear(self, redis_table):
        """
        table -> None
        """
        self._clear(keys=[redis_table])

    def is_full(self, redis_table, redis_size):
        """
        (table, size) -> bool
        """
        return self.is_full_inner(redis_table, redis_size)

    def get(self, redis_table, key):
        """
        (table, key) -> count or None
        """
        count = self._get(keys=[redis_table, key])

        if count is None:
            return count

        return int(count)

    def contains(self, redis_table, key):
        """
        (table, key) -> bool
        """
        count = self._get(keys=[redis_table, key])
        return count is not None

    def add(self, redis_table, redis_size, key):
        """
        (table, size, key) -> None
        """
        self._add(keys=[redis_table, redis_size, key])

    def top_n_keys(self, redis_table, n):
        """
        (table, n) -> list of keys
        """
        return self._top_n_keys(keys=[redis_table, n])

    def top_n_keys_counts(self, redis_table, redis_size, n):
        """
        (table, size, n) -> list of (key, count)
        """
        rr = self._top_n_keys_counts(keys=[redis_table, redis_size, n])
        pairs = []
        for i in range(len(rr) / 2):
            pair = (rr[i * 2], int(rr[i * 2 + 1]))
            pairs.append(pair)
        return pairs


def main():
    redis_table = 'top_talkers'
    redis_size = 4

    t = TopTalkers()

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
