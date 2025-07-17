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
    """
    def __init__(self, base_cfg: dict, legit_cfg: dict, time_cfg: dict):
        """
        base_cfg: dict. Общие конфиги из base.yaml
        legit_cfg: dict. Конфиги легальныз транз. из legit.yaml
        time_cfg: dict. Общие конфиги времени из time.yaml
        clients: gdp.GeoDataframe. Семплированные клиенты для
                 для генерации транз-ций.
        """
        self.base_cfg = base_cfg
        self.legit_cfg = legit_cfg
        self.time_cfg = time_cfg
        self.clients = None


    def assert_time_limits(self):
        """
        Проверка минимальных лимитов времени между транз-циями в 
        min_intervals из time.yaml
        """
        min_inter = self.legit_cfg["time"]["min_intervals"]
        offline_time_diff = min_inter["offline_time_diff"]
        online_time_diff = min_inter["online_time_diff"]
        general_diff = min_inter["general_diff"]

        assert offline_time_diff > general_diff, \
            f"""offline_time_diff must not be lower than general_diff. 
            {offline_time_diff} vs {general_diff} Check passed arguments"""
        
        assert offline_time_diff > online_time_diff, \
            f"""offline_time_diff must not be lower than online_time_diff.
            {offline_time_diff} vs {online_time_diff} Check passed arguments"""
        
        assert general_diff > online_time_diff, \
            f"""general_diff must not be lower than online_time_diff.
            {general_diff} vs {online_time_diff} Check passed arguments"""
        

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
        

    def sample_clients(self):
        """
        Семплировать клинетов под легальные транзакции
        """
        txn_num = self.legit_cfg["txn_num"] # конфиги кол-ва транзакций
        total = txn_num["total_txns"] # Примерное кол-во всех транзакций
        avg_txn_num = txn_num["avg_txn_num"] # среднее кол-во на клиента

        n_clients = total // avg_txn_num # Примерный размер выборки клиентов

        all_clients = self.read_file(category="clients", file_key="clients")

        # Семплируем клиентов и записываем в файл.
        clients_samp = all_clients.sample(n=n_clients, replace=False).reset_index(drop=True)
        write_to = self.base_cfg["data_paths"]["clients"]["clients_sample"]
        clients_samp.to_parquet(write_to, engine="pyarrow")
        
        self.clients = clients_samp
        return clients_samp


    def build_cfg(self):
        """
        Создать конфиг датакласс для легальных транз-ций.
        Возвращает объект LegitCfg.
        """
        self.assert_time_limits()

        stamps_cfg = self.time_cfg["timestamps"]
        base_cfg = self.base_cfg
        weight_args = self.time_cfg["time_weights_args"]
        legit_cfg = self.legit_cfg

        clients = self.sample_clients()
        timestamps = create_timestamps_range_df(stamps_cfg=stamps_cfg)
        timestamps_1st = timestamps.loc[timestamps.timestamp.dt.month == timestamps.timestamp.dt.month.min()]
        txns = create_txns_df(base_cfg["txns_df"])
        client_devices = self.read_file(category="base", file_key="client_devices")
        offline_merchants = self.read_file(category="base", file_key="offline_merchants")
        categories = self.read_file(category="base", file_key="cat_stats_full")
        online_merchant_ids = self.read_file(category="base", \
                                             file_key="online_merchant_ids").iloc[:,0] # нужны в виде серии
        all_time_weights = get_all_time_patterns(pattern_args=weight_args)
        cities = self.read_file(category="base", file_key="cities")
        min_intervals = legit_cfg["time"]["min_intervals"]
        txn_num = legit_cfg["txn_num"]

        return LegitCfg(clients=clients, timestamps=timestamps, transactions=txns, \
                        timestamps_1st=timestamps_1st, client_devices=client_devices, \
                        offline_merchants=offline_merchants, categories=categories, \
                        online_merchant_ids=online_merchant_ids, all_time_weights=all_time_weights, \
                        cities=cities, min_intervals=min_intervals, txn_num=txn_num)