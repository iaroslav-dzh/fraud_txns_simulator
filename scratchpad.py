from data_generator.indev import DropConfigBuilder
from data_generator.fraud.drops.behavior import DistBehaviorHandler
from data_generator.fraud.drops.base import DropAmountHandler

drop_cfg_build = DropConfigBuilder(base_cfg=base_cfg, fraud_cfg=fraud_cfg, drop_cfg=drops_cfg)
config3 = drop_cfg_build.build_dist_cfg()
drop_amt1 = DropAmountHandler(configs=config3)
dist_behav1 = DistBehaviorHandler(configs=config3, amt_hand=drop_amt1)

cfg_builder = CfgBuild(base_cfg, fraud_cfg, drops_cfg)
configs = cfg_builder.build_cfg() # метод в зависимости от типа дропа
acc_hand = AccHand(configs)
amt_hand = AmtHand(configs)
time_hand = TimeHand(configs)
behav_hand = BehavHand(configs, amt_hand) # класс в зависимости от типа дропа
part_data = PartData(configs)
base_classes = BaseCls(acc_hand, amt_hand, time_hand, behav_hand, part_data) # индивидуальный behav_hand

create_txn = CreateTxn(configs, base_classes) 

from data_generator.fraud.txndata import DropTxnPartData
from data_generator.indev import DropConfigBuilder, DropBaseClasses
from data_generator.fraud.drops.txns import CreateDropTxn

drop_cfg_build = DropConfigBuilder(base_cfg=base_cfg, fraud_cfg=fraud_cfg, drop_cfg=drops_cfg)
purch_configs = drop_cfg_build.build_purch_cfg()


own_id = acc_hand1.client_id


# Временный импорт
import os
import yaml
import pandas as pd
import numpy as np
os.chdir("..")

# Общие настройки
with open("./config/base.yaml") as f:
    base_cfg = yaml.safe_load(f)
# Настройки фрода
with open("./config/fraud.yaml") as f:
    fraud_cfg = yaml.safe_load(f)
# Настройки фрода для дропов
with open("./config/drops.yaml", encoding="utf8") as f:
    drops_cfg = yaml.safe_load(f)
# Настройки времени
with open("./config/time.yaml") as f:
    time_cfg = yaml.safe_load(f)


# LEGIT
# Временный импорт
import os
import yaml
import pandas as pd
import numpy as np
os.chdir("..")
# Общие настройки
with open("./config/base.yaml") as f:
    base_cfg = yaml.safe_load(f)
# Настройки легальных транзакций
with open("./config/legit.yaml") as f:
    legit_cfg = yaml.safe_load(f)
# Настройки времени
with open("./config/time.yaml") as f:
    time_cfg = yaml.safe_load(f)

    
offline_merchants = gpd.read_file("./data/cleaned_data/offline_merchants_points.gpkg")
online_merchant_ids = pd.read_csv("./data/cleaned_data/online_merchant_ids.csv").iloc[:, 0]
client_devices = pd.read_csv("./data/cleaned_data/client_devices.csv")
fraud_devices = pd.read_csv("./data/cleaned_data/fraud_devices.csv")
districts_ru = gpd.read_file("./data/cleaned_data/district_ru.gpkg")
city_centers = gpd.read_file("./data/cleaned_data/city_centers.gpkg")
clients_with_geo = gpd.read_file("./data/cleaned_data/clients_with_geo.gpkg") 
fraud_ips = gpd.read_file("./data/cleaned_data/fraud_ips.gpkg")
cat_stats_full = pd.read_csv("./data/cleaned_data/cat_stats_full.csv")

from data_generator.general_time import *
from data_generator.utils import create_txns_df
from data_generator.legit.time.time import check_min_interval_from_near_txn

transactions = create_txns_df(base_cfg["txns_df"])