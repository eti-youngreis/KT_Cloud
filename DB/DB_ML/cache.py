import redis
import sqlite3
from hashlib import sha256
import json

# Connect to Redis
r = redis.StrictRedis(host='localhost', port=6379, db=0)

def _cache_query(query: str):
    """Looks for query results in the cache and returns if they exist"""
    query_hash = sha256(query.encode()).hexdigest()
    result = r.get(query_hash)
    if result:
        return result.decode()
    return None

def _set_cache(query: str, result: str):
    """Stores query results in the cache"""
    query_hash = sha256(query.encode()).hexdigest()
    r.set(query_hash, result)

def _run_query(db_path: str, query: str):
    """
    Executes a SQL query on the specified database with caching.

    Args:
        db_path: Path to the SQLite database file.
        query: SQL query to be executed.

    Returns:
        A list of tuples containing query results if the query retrieves data,
        or None if the query doesn't return results.
    """
    cached_result = _cache_query(query)
    if cached_result:
        return eval(cached_result)  # Use eval to convert the result back to a list of tuples

    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(query)
        if query.lstrip().upper().startswith("SELECT"):
            results = cursor.fetchall()
            _set_cache(query, str(results))  # Cache the result
            return results
        else:
            conn.commit()
            return None
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
        return []
    finally:
        if conn is not None:
            conn.close()
