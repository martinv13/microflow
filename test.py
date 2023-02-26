from functools import wraps
from concurrent import futures
import random

def decorator(func):
    num_process = 4

    func.__reduce__ = lambda: func.__qualname__

    def impl(*args, **kwargs):
        with futures.ProcessPoolExecutor() as executor:
            fs = []
            for i in range(num_process):
                fut = executor.submit(func, *args, **kwargs)
                fs.append(fut)
            result = []
            for f in futures.as_completed(fs):
                result.append(f.result())
        return result

    return impl

@decorator
def get_random_int():
    return random.randint(0, 100)


if __name__ == "__main__":
    result = get_random_int()
    print(result)