


# ------------------------ ВСТАВИТЬ КОД СЮДА ---------------------------



# Временный импорт
import os
import yaml
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

os.chdir("..")

# Общие настройки
with open("./config/base.yaml") as f:
    base_cfg = yaml.safe_load(f)
# Общие настройки фрода
with open("./config/fraud.yaml") as f:
    fraud_cfg = yaml.safe_load(f)
# Настройки compromised client фрода
with open("./config/compr.yaml") as f:
    compr_cfg = yaml.safe_load(f)
# Настройки времени
with open("./config/time.yaml") as f:
    time_cfg = yaml.safe_load(f)

transactions = create_txns_df(base_cfg["txns_df"])
clients = pd.read_parquet("./data/clients/clients.parquet")
clients_sample = pd.read_parquet("./data/clients/clients_sample.parquet")
cat_fraud_amts = pd.read_csv("./data/base_fraud/cat_fraud_amts.csv")

from data_generator.fraud.compr.build.config import ComprConfigBuilder
from data_generator.fraud.compr.time import generate_time_fast_geo_jump
from data_generator.fraud.compr.txndata import FraudTxnPartData, TransAmount
from data_generator.general_time import pd_timestamp_to_unix
from data_generator.utils import create_txns_df, calc_distance
from data_generator.fraud.compr.txns import trans_freq_wrapper

transactions = create_txns_df(base_cfg["txns_df"])
compr_builder = ComprConfigBuilder(base_cfg=base_cfg, time_cfg=time_cfg, \
                                  fraud_cfg=fraud_cfg, compr_cfg=compr_cfg)
configs = compr_builder.build_cfg()
part_data = FraudTxnPartData(configs)
txn_amt = TransAmount(configs)

for client in clients_sample.loc[[1]].itertuples():
    client_info = client
client_id = client_info.client_id

client_txns_df = configs.transactions.query("client_id == @client_id")
part_data.client_info = client_info
client_info