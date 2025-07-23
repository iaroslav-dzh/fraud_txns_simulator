import pandas as pd
import geopandas as gpd
import pyarrow
import os
from pathlib import Path

from data_generator.utils import create_txns_df
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
    drops: pd.DataFrame. Семплированные клиенты для дроп фрода.
    run_dir: str. Путь к директории под текущую генерацию.
    """
    def __init__(self, base_cfg: dict, legit_cfg: dict, time_cfg: dict, fraud_cfg: dict, \
                 drop_cfg: dict, run_dir: str):
        """
        base_cfg: dict. Общие конфиги из base.yaml
        legit_cfg: dict.
        time_cfg: dict. Общие конфиги времени из time.yaml
        fraud_cfg: dict. Общие конфиги фрода из fraud.yaml
        drop_cfg: dict. Конфиги дропов из drops.yaml
        run_dir: str. Путь к директории под текущую генерацию.
        """
        self.base_cfg = base_cfg
        self.legit_cfg = legit_cfg
        self.time_cfg = time_cfg
        self.fraud_cfg = fraud_cfg
        self.drop_cfg = drop_cfg
        self.run_dir = run_dir
        self.drops = None


    def make_dir(self, drop_type):
        """
        Создать индивидуальную директорию в папке текущей генерации
        транзакций.
        ---------
        drop_type: str. Либо 'distributor' либо 'purchaser'. Должно совпадать
                   с ключом в drops.yaml.
        """
        data_storage = self.drop_cfg[drop_type]["data_storage"]
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


    def estimate_drops_count(self, drop_type):
        """
        drop_type: str. Либо 'distributor' либо 'purchaser'
        """
        fraud_cfg = self.fraud_cfg
        drop_cfg = self.drop_cfg
        legit_storage = self.legit_cfg["data_storage"]
        leg_folder = legit_storage["folder_name"]
        leg_txn_file = legit_storage["files"]["txns"]
        legit_txns_path = os.path.join(self.run_dir, leg_folder, leg_txn_file)

        fraud_rate = fraud_cfg["fraud_rates"]["total"] # доля всего фрода от всех транзакций
        drop_share = fraud_cfg["fraud_rates"]["drops"][drop_type] # Доля дропов указанного типа от всего фрода

        out_lim = drop_cfg[drop_type]["out_lim"]

        # отсюда посчитаем количество клиентов для дроп фрода с распределением денег
        legit_txns = self.read_file(path=legit_txns_path)
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
        run_dir = self.run_dir
        legit_storage = self.legit_cfg["data_storage"]
        leg_dir = legit_storage["folder_name"]  # Названия папки legit генерации
        leg_cl_file = legit_storage["files"]["clients"] # Названия файла с клиентами
        legit_cl_path = os.path.join(self.run_dir, leg_dir, leg_cl_file)
        leg_cl_sample = self.read_file(path=legit_cl_path)

        all_cl_path = self.base_cfg["data_paths"]["clients"]["clients"]
        all_clients = self.read_file(path=all_cl_path)

        drops_cfg = self.drop_cfg
        # Проверка наличия дропов другого типа. Чтобы не семплировать их случайно.
        if drop_type == "distributor":
            other_data_storage = drops_cfg["purchaser"]["data_storage"]
            other_drops_folder = other_data_storage["folder_name"]
            other_file_name = other_data_storage["files"]["clients"]

        elif drop_type == "purchaser":
            other_data_storage = drops_cfg["distributor"]["data_storage"]
            other_drops_folder = other_data_storage["folder_name"]
            other_file_name = other_data_storage["files"]["clients"]

        path = os.path.join(run_dir, other_drops_folder, other_file_name)

        if os.path.exists(path):
            other_drops = self.read_file(path=path)
        else:
            other_drops = pd.DataFrame({"client_id": pd.Series(dtype="int64")}) # заглушка

        # Фильтруем клиентов не использованных для легальных транз., для compromised фрода
        # и для дроп фрода другого типа  
        not_used_clients =  all_clients.loc[~all_clients.client_id.isin(leg_cl_sample.client_id) &
                                            ~(all_clients.client_id.isin(other_drops.client_id))].copy()
        
        drops_count = self.estimate_drops_count(drop_type=drop_type)
        drops_samp = not_used_clients.sample(n=drops_count, replace=False).reset_index(drop=True)

        data_storage = drops_cfg[drop_type]["data_storage"]
        file_name = data_storage["files"]["clients"]
        drops_dir = self.make_dir(drop_type=drop_type) # создать папку под генерацию дропов
        write_to = os.path.join(drops_dir, file_name)

        drops_samp.to_parquet(write_to, engine="pyarrow")
        self.drops = drops_samp
        return drops_samp


    def read_by_precedence(self, path_01, path_02, file_type):
        """
        Чтение файла по приоритету и наличию.
        Проверить наличие файла по пути path_01, если он есть, то загрузить его.
        Если его нет то проверить начличие файла по пути path_02 и загрузить его 
        если он есть.
        ------------------
        path_01: str. Путь к файлу у которого приоритет.
        path_02: str. Путь к альтернативному файлу.
        file_type: str. Тип файла: 'csv' или 'parquet'.
        """

        if os.path.exists(path_01) and file_type == "csv":
            return pd.read_csv(path_01)
        elif os.path.exists(path_01) and file_type == "parquet":
            return pd.read_parquet(path_01, engine="pyarrow")
        
        elif os.path.exists(path_02) and file_type == "csv":
            return pd.read_csv(path_02)
        elif os.path.exists(path_02) and file_type == "parquet":
            return pd.read_parquet(path_02, engine="pyarrow")
        
        raise ValueError(f"""No files in paths: {path_01}
            or {path_02}""")


    def build_dist_cfg(self):
        """
        Создать конфиг датакласс для дропов распределителей (distributors).
        Возвращает объект DropDistributorCfg.
        """
        base_cfg = self.base_cfg
        drop_cfg = self.drop_cfg
        dist_cfg = drop_cfg["distributor"]
        time_cfg = drop_cfg["time"]
        stamps_cfg = self.time_cfg["timestamps"]
        base_files = base_cfg["data_paths"]["base"]
        acc_path_01 = Path(self.run_dir) / "accounts.csv"
        acc_path_02 = base_files["accounts_default"]
        
        clients = self.get_clients_for_drops(drop_type="distributor")
        timestamps = create_timestamps_range_df(stamps_cfg=stamps_cfg)
        txns = create_txns_df(base_cfg["txns_df"])
        accounts = self.read_by_precedence(path_01=acc_path_01, path_02=acc_path_02, file_type="csv")
        outer_accounts = self.read_file(path=base_files["outer_accounts"]).iloc[:,0] # нужны в виде серии
        client_devices = self.read_file(path=base_files["client_devices"])
        online_merchant_ids = self.read_file(path=base_files["online_merchant_ids"]) \
                                  .iloc[:,0] # нужны в виде серии
        cities = self.read_file(path=base_files["cities"])
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
        data_paths = base_cfg["data_paths"]
        dir_category = drop_cfg["distributor"]["data_storage"]["category"]
        folder_name = drop_cfg["distributor"]["data_storage"]["folder_name"]
        key_latest = drop_cfg["distributor"]["data_storage"]["key_latest"]
        key_history = drop_cfg["distributor"]["data_storage"]["key_history"]
        run_dir = self.run_dir
        directory = self.make_dir(drop_type="distributor")
        txns_file_name = dist_cfg["data_storage"]["files"]["txns"]

        return DropDistributorCfg(clients=clients, timestamps=timestamps, transactions=txns, accounts=accounts, \
                                  outer_accounts=outer_accounts, client_devices=client_devices, \
                                  online_merchant_ids=online_merchant_ids, cities=cities, in_lim=in_lim, 
                                  out_lim=out_lim, period_in_lim=period_in_lim, period_out_lim=period_out_lim, \
                                  lag_interval=lag_interval, two_way_delta=two_way_delta, pos_delta=pos_delta, \
                                  split_rate=split_rate, chunks=chunks, inbound_amt=inbound_amt, round=round, \
                                  trf_max=trf_max, reduce_share=reduce_share, attempts=attempts, to_drops=to_drops, \
                                  crypto_rate=crypto_rate, data_paths=data_paths, dir_category=dir_category, \
                                  folder_name=folder_name, key_latest=key_latest, key_history=key_history, \
                                  run_dir=run_dir, directory=directory, txns_file_name=txns_file_name
                                  )


    def build_purch_cfg(self):
        """
        Создать конфиг датакласс для дропов покупателей (purchasers).
        Возвращает объект DropPurchaserCfg.
        """
        base_cfg = self.base_cfg
        drop_cfg = self.drop_cfg
        purch_cfg = drop_cfg["purchaser"]
        time_cfg = drop_cfg["time"]
        stamps_cfg = self.time_cfg["timestamps"]
        base_files = base_cfg["data_paths"]["base"]
        base_fraud_files = base_cfg["data_paths"]["base_fraud"]
        acc_path_01 = Path(self.run_dir) / "accounts.csv"
        acc_path_02 = base_files["accounts_default"]

        clients = self.get_clients_for_drops(drop_type="purchaser")
        timestamps = create_timestamps_range_df(stamps_cfg=stamps_cfg)
        txns = create_txns_df(base_cfg["txns_df"])
        accounts = self.read_by_precedence(path_01=acc_path_01, path_02=acc_path_02, file_type="csv")
        client_devices = self.read_file(path=base_files["client_devices"])
        online_merchant_ids = self.read_file(path=base_files["online_merchant_ids"]) \
                                  .iloc[:,0] # нужны в виде серии
        categories = self.read_file(path=base_fraud_files["drop_purch_cats"])
        cities = self.read_file(path=base_files["cities"])
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
        data_paths = base_cfg["data_paths"]
        dir_category = drop_cfg["purchaser"]["data_storage"]["category"]
        folder_name = drop_cfg["purchaser"]["data_storage"]["folder_name"]
        key_latest = drop_cfg["purchaser"]["data_storage"]["key_latest"]
        key_history = drop_cfg["purchaser"]["data_storage"]["key_history"]
        run_dir = self.run_dir
        directory = self.make_dir(drop_type="purchaser")
        txns_file_name = purch_cfg["data_storage"]["files"]["txns"]

        return DropPurchaserCfg(clients=clients, timestamps=timestamps, accounts=accounts, \
                                transactions=txns, client_devices=client_devices, \
                                online_merchant_ids=online_merchant_ids, categories=categories, 
                                cities=cities, in_lim=in_lim, out_lim=out_lim, period_in_lim=period_in_lim, \
                                period_out_lim=period_out_lim, lag_interval=lag_interval, \
                                two_way_delta=two_way_delta, pos_delta=pos_delta, split_rate=split_rate, \
                                chunks=chunks, inbound_amt=inbound_amt, round=round, \
                                amt_max=amt_max, reduce_share=reduce_share, attempts=attempts, \
                                data_paths=data_paths, dir_category=dir_category, folder_name=folder_name, \
                                key_latest=key_latest, key_history=key_history, run_dir=run_dir, \
                                directory=directory, txns_file_name=txns_file_name
                                )