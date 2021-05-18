import logging
import queue
import threading
import time
import typing


def wait_for(
    callback: typing.Callable,
    msg: typing.Optional[str] = None,
    exit_msg: typing.Optional[str] = None,
):
    q = queue.Queue()

    def target():
        try:
            while True:
                value = callback()
                if value:
                    break
                time.sleep(0.5)
            q.put(value)
        except Exception as e:
            q.put(e)

    def check_q():
        try:
            value = q.get_nowait()
            if isinstance(value, Exception):
                raise value
            return value
        except queue.Empty:
            return None

    t = threading.Thread(target=target)
    t.start()

    i, exit_pad = 0, 0
    value = None
    level = logging.INFO
    try:
        while t.is_alive():
            value = check_q()
            if msg is None:
                continue

            dots = "." * (i + 1)
            spaces = " " * (2 - i)
            print(msg + dots + spaces, end="\r", flush=True)
            time.sleep(0.5)
            i = (i + 1) % 3

            exit_pad = max(exit_pad, len(msg + dots))

        if value is None:
            value = check_q()
    except Exception:
        exit_msg = "Encountered error in callback"
        level = logging.ERROR
        raise
    finally:
        print(" " * exit_pad, end="\r", flush=True)
        if exit_msg is not None:
            logging.log(exit_msg, level=level)
    return value
