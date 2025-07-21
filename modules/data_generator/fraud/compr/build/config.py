# Конфиг билдер для compromised client fraud

import pandas as pd
import geopandas as gpd
import os

from data_generator.general_time import create_timestamps_range_df, get_all_time_patterns
from data_generator.configs import ComprClientFraudCfg

class ComprConfigBuilder:
    """
    Создание объекта конфиг класса ComprClientFraudCfg
    для compromised clients fraud транзакций.
    ---------
    Атрибуты:
    ---------
    base_cfg: dict. Общие конфиги из base.yaml
    legit_cfg: dict.
    time_cfg: dict. Общие конфиги времени из time.yaml
    fraud_cfg: dict. Общие конфиги фрода из fraud.yaml
    compr_cfg: dict. Конфиги для compromised фрода из compr.yaml
    run_dir: str. Путь к директории под текущую генерацию.
    clients: pd.DataFrame. Семпл клиентов для генерации
             транзакций.
    """
    def __init__(self, base_cfg: dict, legit_cfg: dict, time_cfg: dict, \
                 fraud_cfg: dict, compr_cfg: dict, run_dir: str):
        """
        base_cfg: dict. Общие конфиги из base.yaml
        legit_cfg: dict.
        time_cfg: dict. Общие конфиги времени из time.yaml
        fraud_cfg: dict. Общие конфиги фрода из fraud.yaml
        compr_cfg: dict. Конфиги для compromised фрода из compr.yaml
        run_dir: str. Путь к директории под текущую генерацию.
        """
        self.base_cfg = base_cfg
        self.legit_cfg = legit_cfg
        self.time_cfg = time_cfg
        self.fraud_cfg = fraud_cfg
        self.compr_cfg = compr_cfg
        self.run_dir = run_dir
        self.clients = None


    def make_dir(self):
        """
        Создать индивидуальную директорию в папке текущей генерации
        транзакций.
        """
        data_storage = self.compr_cfg["data_storage"]
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


    def estimate_clients_count(self):
        """
        Подсчитать сколько примерно клиентов нужно для фрода.
        """
        fraud_cfg = self.fraud_cfg
        legit_storage = self.legit_cfg["data_storage"]
        leg_folder = legit_storage["folder_name"]
        leg_txn_file = legit_storage["files"]["txns"]
        legit_txns_path = os.path.join(self.run_dir, leg_folder, leg_txn_file)

        fraud_rate = fraud_cfg["fraud_rates"]["total"] # доля всего фрода от всех транзакций
        compr_share = fraud_cfg["fraud_rates"]["compr_client"] # Доля compromised client фрода

        # отсюда посчитаем количество клиентов для дроп фрода с распределением денег
        legit_txns = self.read_file(path=legit_txns_path)
        legit_count = legit_txns.shape[0]
        # подсчет количества транзакций равных 1% от всех транзакций
        # т.к. не все транзакции еще созданы, то считаем основываясь на количестве 
        # легальных транзакций и fraud rate
        one_perc = round(legit_count / ((1 - fraud_rate) * 100))
        # Абсолютное кол-во всего фрода
        fraud_abs = one_perc * fraud_rate * 100
        # Абсолютное кол-во фрод транзакций умножаем на долю транзакций compromised фрода
        clients_count = round(fraud_abs * compr_share) 
        return clients_count
    

    def get_clients_for_fraud(self):
        """
        Семплировать клиентов под фрод.
        """
        clients_count = self.estimate_clients_count()
        legit_storage = self.legit_cfg["data_storage"]
        leg_dir = legit_storage["folder_name"]  # Названия папки legit генерации
        leg_cl_file = legit_storage["files"]["clients"] # Названия файла с клиентами
        legit_cl_path = os.path.join(self.run_dir, leg_dir, leg_cl_file)
        # leg_cl_file - Клиенты использ-ные для генерации лег. транз-ций
        # Будем семплировать из них 
        leg_cl_sample = self.read_file(path=legit_cl_path)
        
        compr_samp = leg_cl_sample.sample(n=clients_count, replace=False)\
                                   .reset_index(drop=True)
        self.clients = compr_samp
        return compr_samp
    

    def build_cfg(self):
        """
        Создать конфиг датакласс для легальных транз-ций.
        Возвращает объект ComprClientFraudCfg.
        """
        
        stamps_cfg = self.time_cfg["timestamps"]
        base_cfg = self.base_cfg
        weight_args = self.time_cfg["time_weights_args"]
        compr_cfg = self.compr_cfg
        legit_storage = self.legit_cfg["data_storage"]
        leg_folder = legit_storage["folder_name"]
        leg_txn_file = legit_storage["files"]["txns"]
        legit_txns_path = os.path.join(self.run_dir, leg_folder, leg_txn_file)
        base_files = base_cfg["data_paths"]["base"]
        base_fraud_files = base_cfg["data_paths"]["base_fraud"]

        clients = self.get_clients_for_fraud()
        timestamps = create_timestamps_range_df(stamps_cfg=stamps_cfg)
        txns = self.read_file(path=legit_txns_path)
        offline_merchants = self.read_file(path=base_files["offline_merchants"])
        categories = self.read_file(path=base_files["cat_stats_full"])
        online_merchant_ids = self.read_file(path=base_files["online_merchant_ids"]) \
                                  .iloc[:,0] # нужны в виде серии
        all_time_weights = get_all_time_patterns(pattern_args=weight_args)
        rules = self.read_file(path=base_fraud_files["rules"])
        cities = self.read_file(path=base_files["cities"])
        fraud_devices = self.read_file(path=base_fraud_files["fraud_devices"])
        fraud_ips = self.read_file(path=base_fraud_files["fraud_ips"])
        fraud_amounts = self.read_file(path=base_fraud_files["cat_fraud_amts"])
        rules_cfg = compr_cfg["rules"]
        data_paths = base_cfg["data_paths"]
        dir_category = compr_cfg["data_storage"]["category"]
        folder_name = compr_cfg["data_storage"]["folder_name"]
        key_latest = compr_cfg["data_storage"]["key_latest"]
        key_history = compr_cfg["data_storage"]["key_history"]
        run_dir = self.run_dir
        directory = self.make_dir()
        txns_file_name = compr_cfg["data_storage"]["files"]["txns"]
        

        return ComprClientFraudCfg(
                        clients=clients, timestamps=timestamps, transactions=txns, \
                        offline_merchants=offline_merchants, categories=categories, rules=rules, \
                        online_merchant_ids=online_merchant_ids, all_time_weights=all_time_weights, \
                        cities=cities, fraud_devices=fraud_devices, fraud_ips=fraud_ips, \
                        fraud_amounts=fraud_amounts, rules_cfg=rules_cfg, data_paths=data_paths, \
                        dir_category=dir_category, folder_name=folder_name, key_latest=key_latest, \
                        key_history=key_history, run_dir=run_dir, directory=directory, \
                        txns_file_name=txns_file_name
                        )
    


