import pandas as pd
import geopandas as gpd
import pyarrow
import os

from data_generator.configs import DropDistributorCfg, DropPurchaserCfg
from data_generator.general_time import create_timestamps_range_df

# 1. Конструктор объектов конфиг датаклассов
class DropConfigBuilder:
    """
    Создание объектов датаклассов с конфигами для дропов нужного типа.
    ---------
    Атрибуты:
    ---------
    base_cfg: dict. Общие конфиги из base.yaml
    time_cfg: dict. Общие конфиги времени из time.yaml
    fraud_cfg: dict. Общие конфиги фрода из fraud.yaml
    drop_cfg: dict. Конфиги дропов из drops.yaml
    drops: gdp.GeoDataframe. Семплированные клиенты для дроп фрода.
    """
    def __init__(self, base_cfg: dict, time_cfg: dict, fraud_cfg: dict, drop_cfg: dict):
        """
        base_cfg: dict. Общие конфиги из base.yaml
        time_cfg: dict. Общие конфиги времени из time.yaml
        fraud_cfg: dict. Общие конфиги фрода из fraud.yaml
        drop_cfg: dict. Конфиги дропов из drops.yaml
        """
        self.base_cfg = base_cfg
        self.time_cfg = time_cfg
        self.fraud_cfg = fraud_cfg
        self.drop_cfg = drop_cfg
        self.drops = None


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


    def estimate_drops_count(self, drop_type):
        """
        drop_type: str. Либо 'distributor' либо 'purchaser'
        """
        fraud_cfg = self.fraud_cfg
        drop_cfg = self.drop_cfg

        fraud_rate = fraud_cfg["fraud_rates"]["total"] # доля всего фрода от всех транзакций
        drop_share = fraud_cfg["fraud_rates"]["drops"][drop_type] # Доля дропов указанного типа от всего фрода

        out_lim = drop_cfg[drop_type]["out_lim"]

        # отсюда посчитаем количество клиентов для дроп фрода с распределением денег
        legit_txns = self.read_file(category="generated", file_key="legit_txns")
        legit_count = legit_txns.shape[0]
        # подсчет количества транзакций равных 1% от всех транзакций
        # т.к. не все транзакции еще созданные, то считаем основываясь на количестве легальных транзакций и fraud rate
        one_perc = round(legit_count / ((1 - fraud_rate) * 100))
        # Абсолютное кол-во всего фрода
        fraud_abs = one_perc * fraud_rate * 100
        # Абсолютное кол-во фрод транзакций умножаем на долю транзакций дропов нужного типа 
        # и делим на максимальное количество исх. транз-ций которое дроп может сделать до детекта.
        # Так находим сколько примерно дропов будет под такой фрод
        drops_count = round(fraud_abs * drop_share / out_lim) 
        return drops_count
    

    def get_clients_for_drops(self, drop_type):
        """
        drop_type: str. Тип дропов: 'distributor' или 'purchaser'
        """
        drops_count = self.estimate_drops_count(drop_type=drop_type)
        clients_sample = self.read_file(category="clients", file_key="clients_sample")
        all_clients = self.read_file(category="clients", file_key="clients")

        # Проверка наличия дропов другого типа. Чтобы не семплировать их случайно.
        if drop_type == "distributor":
            other_type_key = "purchase_drops"
            file_key = "dist_drops"

        elif drop_type == "purchaser":
            other_type_key = "dist_drops"
            file_key = "purchase_drops"

        path = self.base_cfg["data_paths"]["clients"][other_type_key]

        if os.path.exists(path):
            other_drops = self.read_file(category="clients", file_key=other_type_key)
        else:
            other_drops = pd.DataFrame({"client_id": pd.Series(dtype="int64")}) # заглушка

        # Фильруем клиентов не использованных для легальных транз., для compromised фрода
        # и для дроп фрода другого типа  
        not_used_clients =  all_clients.loc[~all_clients.client_id.isin(clients_sample.client_id) &
                                            ~(all_clients.client_id.isin(other_drops.client_id))].copy()
        
        drops_samp = not_used_clients.sample(n=drops_count, replace=False).reset_index(drop=True)
        write_to = self.base_cfg["data_paths"]["clients"][file_key]
        drops_samp.to_parquet(write_to, engine="pyarrow")
        self.drops = drops_samp
        return drops_samp


    def read_by_precedence(self, category, file_key_01, file_key_02):
        """
        Чтение файла по приоритету и наличию.
        Проверить наличие файла file_key_01, если он есть, то загрузить его.
        Если его нет то проверить начличие файла file_key_02 и загрузить его если он есть.
        ------------------
        category: str. Напр. 'cleaned_data', 'generated_data'.
        file_key_01: str. Ключ к файлу в yaml конфиге.
        file_key_02: str. Ключ к файлу в yaml конфиге.
        """
        path_01 = self.base_cfg["data_paths"][category][file_key_01]
        path_02 = self.base_cfg["data_paths"][category][file_key_02]

        if os.path.exists(path_01):
            return self.read_file(category=category, file_key=file_key_01)
        elif os.path.exists(path_02):
            return self.read_file(category=category, file_key=file_key_02)
        else:
            raise ValueError(f"""No files found under: {category}.{file_key_01}
            or {category}.{file_key_02}""")


    def build_dist_cfg(self):
        """
        Создать конфиг датакласс для дропов распределителей (distributors).
        Возвращает объект DropDistributorCfg.
        """
        drop_cfg = self.drop_cfg
        dist_cfg = drop_cfg["distributor"]
        time_cfg = drop_cfg["time"]
        stamps_cfg = self.time_cfg["timestamps"]
        
        clients = self.get_clients_for_drops(drop_type="distributor")
        timestamps = create_timestamps_range_df(stamps_cfg=stamps_cfg)
        accounts= self.read_by_precedence(category="base", file_key_01="accounts", file_key_02="accounts_default")
        outer_accounts = self.read_file(category="base", file_key="outer_accounts").iloc[:,0] # нужны в виде серии
        client_devices = self.read_file(category="base", file_key="client_devices")
        online_merchant_ids = self.read_file(category="base", file_key="online_merchant_ids").iloc[:,0] # нужны в виде серии
        cities = self.read_file(category="base", file_key="cities")
        in_lim = dist_cfg["in_lim"]
        out_lim = dist_cfg["out_lim"]
        period_in_lim = dist_cfg["period_in_lim"]
        period_out_lim = dist_cfg["period_out_lim"]
        inbound_amt = drop_cfg["inbound_amt"]
        split_rate = dist_cfg["split_rate"]
        chunks = dist_cfg["chunks"]
        trf_max = dist_cfg["trf_max"]
        reduce_share = dist_cfg["reduce_share"]
        round = dist_cfg["round"]
        lag_interval = time_cfg["lag_interval"]
        two_way_delta = time_cfg["two_way_delta"]
        pos_delta = time_cfg["pos_delta"]
        attempts = dist_cfg["attempts"]
        to_drops = dist_cfg["to_drops"]
        crypto_rate = dist_cfg["crypto_rate"]

        return DropDistributorCfg(clients=clients, timestamps=timestamps, accounts=accounts, \
                                  outer_accounts=outer_accounts, client_devices=client_devices, \
                                  online_merchant_ids=online_merchant_ids, cities=cities, in_lim=in_lim, 
                                  out_lim=out_lim, period_in_lim=period_in_lim, period_out_lim=period_out_lim, \
                                  lag_interval=lag_interval, two_way_delta=two_way_delta, pos_delta=pos_delta, \
                                  split_rate=split_rate, chunks=chunks, inbound_amt=inbound_amt, round=round, \
                                  trf_max=trf_max, reduce_share=reduce_share, attempts=attempts, to_drops=to_drops, \
                                  crypto_rate=crypto_rate
                                  )


    def build_purch_cfg(self):
        """
        Создать конфиг датакласс для дропов покупателей (purchasers).
        Возвращает объект DropPurchaserCfg.
        """
        drop_cfg = self.drop_cfg
        purch_cfg = drop_cfg["purchaser"]
        time_cfg = drop_cfg["time"]
        stamps_cfg = self.time_cfg["timestamps"]

        clients = self.get_clients_for_drops(drop_type="purchaser")
        timestamps = create_timestamps_range_df(stamps_cfg=stamps_cfg)
        accounts= self.read_by_precedence(category="base", file_key_01="accounts", file_key_02="accounts_default")
        client_devices = self.read_file(category="base", file_key="client_devices")
        online_merchant_ids = self.read_file(category="base", \
                                             file_key="online_merchant_ids").iloc[:,0] # нужны в виде серии
        categories = self.read_file(category="base_fraud", file_key="drop_purch_cats")
        cities = self.read_file(category="base", file_key="cities")
        in_lim = purch_cfg["in_lim"]
        out_lim = purch_cfg["out_lim"]
        period_in_lim = purch_cfg["period_in_lim"]
        period_out_lim = purch_cfg["period_out_lim"]
        inbound_amt = drop_cfg["inbound_amt"]
        split_rate = purch_cfg["split_rate"]
        chunks = purch_cfg["chunks"]
        amt_max = purch_cfg["amt_max"]
        reduce_share = purch_cfg["reduce_share"]
        round = purch_cfg["round"]
        lag_interval = time_cfg["lag_interval"]
        two_way_delta = time_cfg["two_way_delta"]
        pos_delta = time_cfg["pos_delta"]
        attempts = purch_cfg["attempts"]

        return DropPurchaserCfg(clients=clients, timestamps=timestamps, accounts=accounts, \
                                client_devices=client_devices, \
                                online_merchant_ids=online_merchant_ids, categories=categories, 
                                cities=cities, in_lim=in_lim, out_lim=out_lim, period_in_lim=period_in_lim, \
                                period_out_lim=period_out_lim, lag_interval=lag_interval, \
                                two_way_delta=two_way_delta, pos_delta=pos_delta, split_rate=split_rate, \
                                chunks=chunks, inbound_amt=inbound_amt, round=round, \
                                amt_max=amt_max, reduce_share=reduce_share, attempts=attempts
                                )