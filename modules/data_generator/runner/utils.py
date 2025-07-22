import os
from datetime import datetime
import threading
import sys
import time
import itertools



def make_dir_for_run(base_cfg):
    """
    Создать общую директорию под текущий запуск генератора и
    вернуть путь к этой директории.
    """
    run_prefix = base_cfg["run_prefix"]
    datetime_suffix = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    run_dir = run_prefix + datetime_suffix
    history_dir = base_cfg["data_paths"]["generated"]["history"]
    run_dir_path = os.path.join(history_dir, run_dir)

    if not os.path.exists(run_dir_path):
        os.mkdir(run_dir_path)

    return run_dir_path


def notifier(text):
    def decorator(func):
        def wrapper(*args, **kwargs):
            print(f"{text} started")
            result = func(*args, **kwargs)
            print(f"{text} finished")
            return result
        return wrapper
    return decorator


def notifier_dynamic(func):
    def wrapper(self, *args, **kwargs):
        text = f"{self.drop_type} drops generation"
        print(f"{text} started")
        result = func(self, *args, **kwargs)
        print(f"{text} finished")
        return result
    return wrapper


def spinner_decorator(func):
    def wrapper(self, *args, **kwargs):
        done = False
        text = self.text

        def spinner():
            wheel =  itertools.cycle(['|', '\\', '-', '|', '-', '/'])
            while not done:
                ch = next(wheel)
                sys.stdout.write(f"\r{text} in progress... {ch}")
                sys.stdout.flush()
                time.sleep(0.1)

        thread = threading.Thread(target=spinner)
        thread.start()
        try:
            result = func(self, *args, **kwargs)
        finally:
            done = True
            thread.join()
            sys.stdout.flush() # добавил протестить. Чтоб вернуть все, удалить
            sys.stdout.write(f"\r{text}... completed.\n")
            sys.stdout.flush()
        return result
    return wrapper


