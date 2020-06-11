import redis
redis_con   = redis.StrictRedis(host='localhost', port=6379, db=0)

def get_redis_data(pattern, value_search=None, strict=False):
    if strict:
        data = redis_con.get(pattern)
        return data
    else:
        data = redis_con.keys(pattern=pattern)
        if len(data) == 1:
            return data[0]
        else:
            return None
