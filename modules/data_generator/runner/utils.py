import os
from datetime import datetime



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