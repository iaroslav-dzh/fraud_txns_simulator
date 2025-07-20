# Конфиг билдер для compromised client fraud

import pandas as pd
import geopandas as gpd

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
    time_cfg: dict. Общие конфиги времени из time.yaml
    fraud_cfg: dict. Общие конфиги фрода из fraud.yaml
    compr_cfg: dict. Конфиги для compromised фрода из compr.yaml
    clients: pd.DataFrame. Семпл клиентов для генерации
             транзакций.
    """
    def __init__(self, base_cfg: dict, time_cfg: dict, \
                 fraud_cfg: dict, compr_cfg: dict):
        """
        base_cfg: dict. Общие конфиги из base.yaml
        time_cfg: dict. Общие конфиги времени из time.yaml
        fraud_cfg: dict. Общие конфиги фрода из fraud.yaml
        compr_cfg: dict. Конфиги для compromised фрода из compr.yaml
        """
        self.base_cfg = base_cfg
        self.time_cfg = time_cfg
        self.fraud_cfg = fraud_cfg
        self.compr_cfg = compr_cfg
        self.clients = None

    def read_file(self, category, file_key):
        """
        category: str. Ключ внутри yaml конфига. Категория файла.
                  Напр. 'cleaned_data', 'generated_data'.
        file_key: str. Ключ к файлу внутри категории в yaml конфиге.
        """
        path = self.base_cfg["data_paths"][category][file_key]
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

        fraud_rate = fraud_cfg["fraud_rates"]["total"] # доля всего фрода от всех транзакций
        compr_share = fraud_cfg["fraud_rates"]["compr_client"] # Доля compromised client фрода

        # отсюда посчитаем количество клиентов для дроп фрода с распределением денег
        legit_txns = self.read_file(category="generated", file_key="legit_txns")
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
        # clients_sample - Клиенты использ-ные для генерации лег. транз-ций
        # Будем семплировать из них 
        clients_sample = self.read_file(category="clients", file_key="clients_sample")
        
        compr_samp = clients_sample.sample(n=clients_count, replace=False)\
                                   .reset_index(drop=True)
        self.clients = compr_samp
        return compr_samp
    

    def build_cfg(self, run_dir):
        """
        Создать конфиг датакласс для легальных транз-ций.
        Возвращает объект ComprClientFraudCfg.
        -------------
        run_dir: str. Название общей папки для хранения всех файлов 
                 этого запуска генерации: легальных, compromised фрода,
                 дроп фрода.
        """

        stamps_cfg = self.time_cfg["timestamps"]
        base_cfg = self.base_cfg
        weight_args = self.time_cfg["time_weights_args"]
        compr_cfg = self.compr_cfg

        clients = self.get_clients_for_fraud()
        timestamps = create_timestamps_range_df(stamps_cfg=stamps_cfg)
        txns = self.read_file(category="generated", file_key="legit_txns")
        offline_merchants = self.read_file(category="base", file_key="offline_merchants")
        categories = self.read_file(category="base", file_key="cat_stats_full")
        online_merchant_ids = self.read_file(category="base", file_key="online_merchant_ids") \
                                  .iloc[:,0] # нужны в виде серии
        all_time_weights = get_all_time_patterns(pattern_args=weight_args)
        rules = self.read_file(category="base_fraud", file_key="rules")
        cities = self.read_file(category="base", file_key="cities")
        fraud_devices = self.read_file(category="base_fraud", file_key="fraud_devices")
        fraud_ips = self.read_file(category="base_fraud", file_key="fraud_ips")
        fraud_amounts = self.read_file(category="base_fraud", file_key="cat_fraud_amts")
        rules_cfg = compr_cfg["rules"]
        data_paths = base_cfg["data_paths"]
        dir_category = compr_cfg["data_storage"]["category"]
        folder_name = compr_cfg["data_storage"]["folder_name"]
        key_latest = compr_cfg["data_storage"]["key_latest"]
        key_history = compr_cfg["data_storage"]["key_history"]
        

        return ComprClientFraudCfg(clients=clients, timestamps=timestamps, transactions=txns, \
                        offline_merchants=offline_merchants, categories=categories, rules=rules, \
                        online_merchant_ids=online_merchant_ids, all_time_weights=all_time_weights, \
                        cities=cities, fraud_devices=fraud_devices, fraud_ips=fraud_ips, \
                        fraud_amounts=fraud_amounts, rules_cfg=rules_cfg, data_paths=data_paths, \
                        dir_category=dir_category, folder_name=folder_name, key_latest=key_latest, \
                        key_history=key_history, run_dir=run_dir)
    


