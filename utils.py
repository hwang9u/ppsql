import time
import functools
def timewrapper(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        t_start = time.time()
        result = func(*args, **kwargs)
        t_end = time.time()
        print(f"[Process time] {t_end - t_start:.5f} s")
        return result
    return wrapper