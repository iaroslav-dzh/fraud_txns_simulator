# Конструктор класса с конфигами  и данными для генарации легальных транзакций
import pandas as pd
import geopandas as gpd
import pyarrow
import os

from data_generator.general_time import create_timestamps_range_df
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

        all_clients = self.read_file(category="cleaned_data", file_key="clients_with_geo")

        # Семплируем клиентов и записываем в файл.
        clients_samp = all_clients.sample(n=n_clients, replace=False).reset_index(drop=True)
        write_to = self.base_cfg["data_paths"]["generated_data"]["clients_sample"]
        clients_samp.to_file(write_to, layer="layer_name", driver="GPKG")

        self.clients = clients_samp


    def build_cfg(self):
        """
        Создать конфиг датакласс для легальных транз-ций.
        Возвращает объект LegitCfg.
        """
        self.assert_time_limits()

        cleaned_data = "cleaned_data"
        generated_data = "generated_data"
        drop_cfg = self.drop_cfg
        dist_cfg = drop_cfg["distributor"]
        time_cfg = drop_cfg["time"]
        stamps_cfg = self.time_cfg["timestamps"]
        
        clients = self.get_clients_for_clients(drop_type="distributor")
        timestamps = create_timestamps_range_df(stamps_cfg=stamps_cfg)
        accounts= self.read_by_precedence(category=generated_data, file_key_01="accounts", file_key_02="accounts_default")
        outer_accounts = self.read_file(category=generated_data, file_key="outer_accounts").iloc[:,0] # нужны в виде серии
        client_devices = self.read_file(category=cleaned_data, file_key="client_devices")
        online_merchant_ids = self.read_file(category=cleaned_data, file_key="online_merchant_ids").iloc[:,0] # нужны в виде серии
        cities = self.read_file(category=cleaned_data, file_key="districts_ru")
        lag_interval = time_cfg["lag_interval"]

        return LegitCfg(clients=clients, timestamps=timestamps, accounts=accounts, \
                                  outer_accounts=outer_accounts, client_devices=client_devices, \
                                  online_merchant_ids=online_merchant_ids, cities=cities, in_lim=in_lim, 
                                  out_lim=out_lim, period_in_lim=period_in_lim, period_out_lim=period_out_lim, \
                                  lag_interval=lag_interval, two_way_delta=two_way_delta, pos_delta=pos_delta, \
                                  split_rate=split_rate, chunks=chunks, inbound_amt=inbound_amt, round=round, \
                                  trf_max=trf_max, reduce_share=reduce_share, attempts=attempts, to_clients=to_clients, \
                                  crypto_rate=crypto_rate
                                  )