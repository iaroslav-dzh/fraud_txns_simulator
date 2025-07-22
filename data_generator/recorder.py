import os
import pandas as pd
from data_generator.runner.utils import spinner_decorator

class AllTxnsRecorder:
    """
    Сборка и запись единого датафрейма транзакций из всех
    созданных транзакций: легальные, фрод.
    -----------
    Атрибуты:
    -----------
    base_cfg: dict. Конфиги из base.yaml
    legit_cfg: dict. Конфиги из legit.yaml
    compr_cfg: dict. Конфиги для compromised фрода из compr.yaml
    drops_cfg: dict. Конфиги для дроп фрода из drops.yaml
    run_dir: str. Название директории для хранения сгенерированных
             данных текущей генерации.
    text: str. Текст для вставки в спиннер.
    whole_df: pd.DataFrame. Собранный датафрейм со всеми транзакциями:
              легальными и фродом. По умолчанию None. Создается вызовом
              self.build_and_write().
    """
    def __init__(self, base_cfg, legit_cfg, compr_cfg, drops_cfg, \
                 run_dir):
        """
        base_cfg: dict. Конфиги из base.yaml
        legit_cfg: dict. Конфиги из legit.yaml
        compr_cfg: dict. Конфиги для compromised фрода из compr.yaml
        drops_cfg: dict. Конфиги для дроп фрода из drops.yaml
        run_dir: str. Название директории для хранения сгенерированных
                 данных текущей генерации.
        """
        self.base_cfg = base_cfg
        self.legit_cfg = legit_cfg
        self.compr_cfg = compr_cfg
        self.drops_cfg = drops_cfg
        self.run_dir = run_dir
        self.text = "Building and writing all txns dataframe"
        self.whole_df = None

    def read_legit(self):
        """
        """
        run_dir = self.run_dir
        data_storage = self.legit_cfg["data_storage"]
        folder = data_storage["folder_name"]
        file = data_storage["files"]["txns"]
        path = os.path.join(run_dir, folder, file)
        return pd.read_parquet(path, engine="pyarrow")
    

    def read_compromised(self):
        """
        """
        run_dir = self.run_dir
        data_storage = self.compr_cfg["data_storage"]
        folder = data_storage["folder_name"]
        file = data_storage["files"]["txns"]
        path = os.path.join(run_dir, folder, file)
        return pd.read_parquet(path, engine="pyarrow")
    

    def read_dist_drops(self):
        """
        """
        run_dir = self.run_dir
        data_storage = self.drops_cfg["distributor"]["data_storage"]
        folder = data_storage["folder_name"]
        file = data_storage["files"]["txns"]
        path = os.path.join(run_dir, folder, file)
        return pd.read_parquet(path, engine="pyarrow")
    

    def read_purch_drops(self):
        """
        """
        run_dir = self.run_dir
        data_storage = self.drops_cfg["purchaser"]["data_storage"]
        folder = data_storage["folder_name"]
        file = data_storage["files"]["txns"]
        path = os.path.join(run_dir, folder, file)
        return pd.read_parquet(path, engine="pyarrow")

    @spinner_decorator
    def build_and_write(self):
        """
        Собрать из всех датафреймов с транзакциями один датафрейм и записать его
        в две директории.
        """
        run_dir = self.run_dir
        file = self.base_cfg["data_storage"]["files"]["all_txns"]
        path = os.path.join(run_dir, file)
        legit_txns = self.read_legit()
        compr_txns = self.read_compromised()
        dist_drops_txns = self.read_dist_drops()
        purch_drops_txns = self.read_purch_drops()
        all_dfs = [legit_txns, compr_txns, dist_drops_txns, purch_drops_txns]

        self.whole_df = pd.concat(all_dfs, ignore_index=True).sort_values("unix_time")
        all_txns = self.whole_df

        # Одна запись в директорию для конкретно текущей генерации
        all_txns.to_parquet(path, engine="pyarrow")

        # Вторая запись в директорию для последней генерации
        path_latest = self.base_cfg["data_paths"]["generated"]["all_txns"]
        all_txns.to_parquet(path_latest, engine="pyarrow")


