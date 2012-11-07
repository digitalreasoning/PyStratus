import signal
from cloud.cluster import TimeoutException

def timeout(seconds_before_timeout):
    """
    Borrowed from http://www.saltycrane.com/blog/2010/04/using-python-timeout-decorator-uploading-s3/
    """
    def decorate(f):
        def handler(signum, frame):
            raise TimeoutException()
        def new_f(*args, **kwargs):
            old = signal.signal(signal.SIGALRM, handler)
            signal.alarm(seconds_before_timeout)
            try:
                result = f(*args, **kwargs)
            finally:
                signal.signal(signal.SIGALRM, old)
            signal.alarm(0)
            return result
        new_f.func_name = f.func_name
        return new_f
    return decorate

