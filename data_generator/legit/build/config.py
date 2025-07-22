# Конструктор класса с конфигами  и данными для генарации легальных транзакций
import pandas as pd
import geopandas as gpd
import pyarrow
import os

from data_generator.general_time import create_timestamps_range_df, get_all_time_patterns
from data_generator.utils import create_txns_df
from data_generator.configs import LegitCfg
# 1.

class LegitConfigBuilder:
    """
    Создание объекта конфиг класса LegitCfg под легальные
    транзакции.
    ---------
    Атрибуты:
    ---------
    base_cfg: dict. Общие конфиги из base.yaml
    legit_cfg: dict.
    time_cfg: dict. Общие конфиги времени из time.yaml
    run_dir: str. Путь к директории под текущую генерацию.
    clients: pd.DataFrame. Семпл клиентов для генерации
             транзакций.
    """
    def __init__(self, base_cfg: dict, legit_cfg: dict, time_cfg: dict, \
                 run_dir: str):
        """
        base_cfg: dict. Общие конфиги из base.yaml
        legit_cfg: dict. Конфиги легальныз транз. из legit.yaml
        time_cfg: dict. Общие конфиги времени из time.yaml
        run_dir: str. Путь к директории под текущую генерацию.
        """
        self.base_cfg = base_cfg
        self.legit_cfg = legit_cfg
        self.time_cfg = time_cfg
        self.run_dir = run_dir
        self.clients = None


    def make_dir(self):
        """
        Создать индивидуальную legit директорию в папке
        текущей генерации транзакций.
        """
        data_storage = self.legit_cfg["data_storage"]
        run_dir = self.run_dir
        folder_name = data_storage["folder_name"]
        path = os.path.join(run_dir, folder_name)

        if os.path.exists(path):
            return path
        
        os.mkdir(path)
        return path   


    def read_file(self, path, file=""):
        """
        path: str. Путь к директории или файлу.
        file: str. Название файла с расширением
              в path если path это директория.
        """
        if os.path.isdir(path) and file != "":
            file_type = file.split(".")[-1]
            path = os.path.join(path, file)
        if os.path.isfile(path) and file == "":
            file_type = path.split(".")[-1]

        if file_type == "csv":
            return pd.read_csv(path)
        
        if file_type == "gpkg":
            return gpd.read_file(path)
        
        if file_type == "parquet":
            return pd.read_parquet(path, engine="pyarrow")
        

    def sample_clients(self):
        """
        Семплировать клиентов под легальные транзакции
        """
        txn_num = self.legit_cfg["txn_num"] # конфиги кол-ва транзакций
        total = txn_num["total_txns"] # Примерное кол-во всех транзакций
        avg_txn_num = txn_num["avg_txn_num"] # среднее кол-во на клиента

        n_clients = total // avg_txn_num # Примерный размер выборки клиентов

        all_cl_path = self.base_cfg["data_paths"]["clients"]["clients"]
        all_clients = self.read_file(path=all_cl_path)

        # Семплируем клиентов и записываем в файл.
        clients_samp = all_clients.sample(n=n_clients, replace=False).reset_index(drop=True)
        legit_dir = self.make_dir() # создать папку под генерацию legit
        file_name = self.legit_cfg["data_storage"]["files"]["clients"]
        clients_path = os.path.join(legit_dir, file_name)
        write_to = os.path.join(clients_path)
        clients_samp.to_parquet(write_to, engine="pyarrow")
        
        self.clients = clients_samp
        return clients_samp


    def build_cfg(self):
        """
        Создать конфиг датакласс для легальных транз-ций.
        Возвращает объект LegitCfg.
        -------------
        """

        stamps_cfg = self.time_cfg["timestamps"]
        base_cfg = self.base_cfg
        weight_args = self.time_cfg["time_weights_args"]
        legit_cfg = self.legit_cfg
        base_files = base_cfg["data_paths"]["base"]

        clients = self.sample_clients()
        timestamps = create_timestamps_range_df(stamps_cfg=stamps_cfg)
        timestamps_1st = timestamps.loc[timestamps.timestamp.dt.month == timestamps.timestamp.dt.month.min()]
        txns = create_txns_df(base_cfg["txns_df"])
        client_devices = self.read_file(path=base_files["client_devices"])
        offline_merchants = self.read_file(path=base_files["offline_merchants"])
        categories = self.read_file(path=base_files["cat_stats_full"])
        online_merchant_ids = self.read_file(path=base_files["online_merchant_ids"]) \
                                  .iloc[:,0] # нужны в виде серии
        all_time_weights = get_all_time_patterns(pattern_args=weight_args)
        cities = self.read_file(path=base_files["cities"])
        min_intervals = legit_cfg["time"]["min_intervals"]
        txn_num = legit_cfg["txn_num"]
        data_paths = base_cfg["data_paths"]
        dir_category = legit_cfg["data_storage"]["category"]
        folder_name = legit_cfg["data_storage"]["folder_name"]
        key_latest = legit_cfg["data_storage"]["key_latest"]
        key_history = legit_cfg["data_storage"]["key_history"]
        run_dir = self.run_dir
        directory = self.make_dir()
        txns_file_name = legit_cfg["data_storage"]["files"]["txns"]
        prefix = legit_cfg["data_storage"]["prefix"]
        

        return LegitCfg(clients=clients, timestamps=timestamps, transactions=txns, \
                        timestamps_1st=timestamps_1st, client_devices=client_devices, \
                        offline_merchants=offline_merchants, categories=categories, \
                        online_merchant_ids=online_merchant_ids, all_time_weights=all_time_weights, \
                        cities=cities, min_intervals=min_intervals, txn_num=txn_num, \
                        data_paths=data_paths, dir_category=dir_category, \
                        folder_name=folder_name, key_latest=key_latest, key_history=key_history, \
                        run_dir=run_dir, directory=directory, txns_file_name=txns_file_name, \
                        prefix=prefix
                        )