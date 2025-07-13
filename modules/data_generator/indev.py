import pandas as pd
import geopandas as gpd
import numpy as np
import pyarrow
import os
from dataclasses import dataclass
from typing import Union

from data_generator.configs import DropDistributorCfg, DropPurchaserCfg
from data_generator.fraud.drops.base import DropAccountHandler, DropAmountHandler
from data_generator.fraud.drops.behavior import DistBehaviorHandler, PurchBehaviorHandler
from  data_generator.fraud.txndata import DropTxnPartData
from data_generator.fraud.drops.time import DropTimeHandler


class DropConfigBuilder:
    """
    Создание объектов датаклассов с конфигами.
    ---------
    Атрибуты:
    ---------
    base_cfg: dict. Конфиги из base.yaml
    fraud_cfg: dict. Конфиги из fraud.yaml
    drop_cfg: dict. Конфиги из drops.yaml
    drops: gdp.GeoDataframe. Семплированные клиенты для дроп фрода.
    """
    def __init__(self, base_cfg, fraud_cfg, drop_cfg):
        """
        base_cfg: dict. Конфиги из base.yaml
        fraud_cfg: dict. Конфиги из fraud.yaml
        drop_cfg: dict. Конфиги из drops.yaml
        """
        self.base_cfg = base_cfg
        self.fraud_cfg = fraud_cfg
        self.drop_cfg = drop_cfg
        self.drops = None

    def read_file(self, category, file_key):
        """
        category: str. Напр. 'cleaned_data', 'generated_data'.
        file_key: str. Ключ к файлу в yaml конфиге.
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
        legit_txns = self.read_file(category="generated_data", file_key="legit_txns")
        legit_count = legit_txns.shape[0]
        # подсчет количества транзакций равных 1% от всех транзакций
        # т.к. не все транзакции еще созданные, то считаем основываясь на количестве легальных транзакций и fraud rate
        one_perc = round(legit_count / ((1 - fraud_rate) * 100))
        # Один процент транзакций умножаем на долю транзакций дропов нужного типа 
        # и делим на максимальное количество исх. транз-ций которое дроп может сделать до детекта.
        # Так находим сколько примерно дропов будет под такой фрод
        drops_count = round(one_perc * drop_share / out_lim) 
        return drops_count
    

    def get_clients_for_drops(self, drop_type):
        """
        drop_type: str. Тип дропов: 'distributor' или 'purchaser'
        """
        drops_count = self.estimate_drops_count(drop_type=drop_type)
        # legit_txns = self.read_file(category="generated_data", file_key="legit_txns")
        clients_sample = self.read_file(category="cleaned_data", file_key="clients_sample")
        all_clients = self.read_file(category="cleaned_data", file_key="clients_with_geo")

        # Проверка наличия дропов другого типа. Чтобы не семплировать их случайно.
        if drop_type == "distributor":
            other_type_key = "purchase_drops"
            file_key = "dist_drops"

        elif drop_type == "purchaser":
            other_type_key = "dist_drops"
            file_key = "purchase_drops"

        path = self.base_cfg["data_paths"]["generated_data"][other_type_key]

        if os.path.exists(path):
            other_drops = self.read_file(category="generated_data", file_key=other_type_key)
        else:
            other_drops = pd.DataFrame({"client_id": pd.Series(dtype="int64")}) # заглушка

        # Фильруем клиентов не использованных для легальных транз., для compromised фрода
        # и для дроп фрода другого типа  
        not_used_clients =  all_clients.loc[~all_clients.client_id.isin(clients_sample.client_id) &
                                            ~(all_clients.client_id.isin(other_drops.client_id))].copy()
        
        drops_samp = not_used_clients.sample(n=drops_count, replace=False).reset_index(drop=True)
        write_to = self.base_cfg["data_paths"]["generated_data"][file_key]
        drops_samp.to_file(write_to, layer="layer_name", driver="GPKG")
        self.drops = drops_samp
        return drops_samp


    def build_dist_cfg(self):
        """
        Создать конфиг датакласс для дропов распределителей (distributors).
        Возвращает объект DropDistributorCfg.
        """
        cleaned_data = "cleaned_data"
        generated_data = "generated_data"
        drop_cfg = self.drop_cfg
        dist_cfg = drop_cfg["distributor"]
        time_cfg = drop_cfg["time"]
        
        clients = self.get_clients_for_drops(drop_type="distributor")
        timestamps = self.read_file(category=generated_data, file_key="timestamps")
        accounts = self.read_file(category=generated_data, file_key="accounts")
        outer_accounts = self.read_file(category=generated_data, file_key="outer_accounts").iloc[:,0] # нужны в виде серии
        client_devices = self.read_file(category=cleaned_data, file_key="client_devices")
        online_merchant_ids = self.read_file(category=cleaned_data, file_key="online_merchant_ids").iloc[:,0] # нужны в виде серии
        cities = self.read_file(category=cleaned_data, file_key="districts_ru")
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
        cleaned_data = "cleaned_data"
        generated_data = "generated_data"
        drop_cfg = self.drop_cfg
        dist_cfg = drop_cfg["purchaser"]
        time_cfg = drop_cfg["time"]
        
        clients = self.get_clients_for_drops(drop_type="purchaser")
        timestamps = self.read_file(category=generated_data, file_key="timestamps")
        accounts = self.read_file(category=generated_data, file_key="accounts")
        outer_accounts = self.read_file(category=generated_data, file_key="outer_accounts").iloc[:,0] # нужны в виде серии
        client_devices = self.read_file(category=cleaned_data, file_key="client_devices")
        online_merchant_ids = self.read_file(category=cleaned_data, file_key="online_merchant_ids").iloc[:,0] # нужны в виде серии
        cities = self.read_file(category=cleaned_data, file_key="districts_ru")
        in_lim = dist_cfg["in_lim"]
        out_lim = dist_cfg["out_lim"]
        period_in_lim = dist_cfg["period_in_lim"]
        period_out_lim = dist_cfg["period_out_lim"]
        inbound_amt = drop_cfg["inbound_amt"]
        split_rate = dist_cfg["split_rate"]
        chunks = dist_cfg["chunks"]
        amt_max = dist_cfg["amt_max"]
        reduce_share = dist_cfg["reduce_share"]
        round = dist_cfg["round"]
        lag_interval = time_cfg["lag_interval"]
        two_way_delta = time_cfg["two_way_delta"]
        pos_delta = time_cfg["pos_delta"]
        attempts = dist_cfg["attempts"]
        to_drops = dist_cfg["to_drops"]
        crypto_rate = dist_cfg["crypto_rate"]

        return DropPurchaserCfg(clients=clients, timestamps=timestamps, accounts=accounts, \
                                  outer_accounts=outer_accounts, client_devices=client_devices, \
                                  online_merchant_ids=online_merchant_ids, cities=cities, in_lim=in_lim, 
                                  out_lim=out_lim, period_in_lim=period_in_lim, period_out_lim=period_out_lim, \
                                  lag_interval=lag_interval, two_way_delta=two_way_delta, pos_delta=pos_delta, \
                                  split_rate=split_rate, chunks=chunks, inbound_amt=inbound_amt, round=round, \
                                  amt_max=amt_max, reduce_share=reduce_share, attempts=attempts
                                  )
    

    # . Агрегатор базовых классов для дропов

    @dataclass
    class DropBaseClasses:
        """
        acc_hand: DropAccountHandler. Управление счетами транзакций.
        amt_hand: DropAmountHandler. Управление суммами транзакций.
        part_data: DropTxnPartData. Генерация части данных транзакции:
                гео, ip, город, мерчант id и т.п.
        time_hand: DropTimeHandler. Генерация времени транзакций.
        behav_hand: DistBehaviorHandler| PurchBehaviorHandler. Управление поведением дропа
        """
        acc_hand: DropAccountHandler
        amt_hand: DropAmountHandler
        part_data: DropTxnPartData
        time_hand: DropTimeHandler
        behav_hand: Union[DistBehaviorHandler, PurchBehaviorHandler]

        

