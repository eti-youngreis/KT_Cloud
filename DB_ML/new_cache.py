import redis
import pickle

r = redis.Redis(host='localhost', port=6379, db=0)

def get_cached_results(cache_key: str):
    cached_results = r.get(cache_key)
    if cached_results:
        return pickle.loads(cached_results)
    return None

def set_cached_results(cache_key: str, results: list, expiration_time: int = 3600):
    r.setex(cache_key, expiration_time, pickle.dumps(results))
