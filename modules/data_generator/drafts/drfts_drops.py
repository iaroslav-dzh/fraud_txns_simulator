# Наброски

# 1. Управление активностью дропа от ее начала до завершения

# ВОЗМОЖНО НА КАНВАСЕ БОЛЕЕ АКТУАЛЬНАЯ ВЕРСИЯ
class DropLifecycleManager:
    """
    Управление активностью дропа от ее начала до завершения
    -------
    ВОЗМОЖНО НА КАНВАСЕ БОЛЕЕ АКТУАЛЬНАЯ ВЕРСИЯ
    """
    def __init__():
        self.Behav = BehavHand
        self.AccHand = AccHand
        self.AmtHand = AmtHand
        self.CreatTxn = CreateTxn
        self.declined = False # МБ переписать в метод property как ГПТ советовал
        self.alltxns = []
    
    def process_batch():
        BehavHand.sample_scen() # выбрать сценарий
        BehavHand.in_chunks_val() # транзакции по частям или нет
    
        while AmtHand.balance > 0:
            if online and np.random.uniform(0, 1) < crypto_rate:
                part_out = CreateTxn.purchase(declined=declined)
            else:
                part_out = CreateTxn.trf_or_atm(receive=False,
                                            declined=declined)
            all_txns5.append(part_out)
            Behav.deduct_attempts(declined=declined, receive=False)
            
    # Решение об остановке после первой отклоненной транзакции
            if BehavHand.stop_after_decline(declined=declined):
                break
            # Лимит достигнут: следующая транзакция будет отклонена     
            self.declined = CreateTxn.limit_reached()
    
    def reset_cache():
        """
        Сброс кэшей когда активность дропа закончена совсем
        """
        self.declined = False
        self.BehavHand.reset_cache()
        self.TimeHand.reset_cache()
        self.CreatTxn.reset_cache()
        self.CreatTxn.PartData.reset_cache()
        
    def run_drop_lifecycle():
        # создать счет дропа, записать is_drop = True
        AccHand.get_acc(own) # получить номер счета дропа. Пишется в атрибут AccHand.account
        AccHand.label_drop() # помечаем клиента как дропа в таблице accounts
    
        while True:
            decline = self.decline # статус транзакции. будет ли она отклонена
            receive_txn = CreateTxn.one_txn(receive=True) # входящая транзакция. Новый батч денег.
            self.alltxns.append(receive_txn)
            # если у дропа достингут лимит то транзакции отклоняются. 
            # Если входящая отклонена, дропу больше не пытаются послать деньги
            if decline: 
                break
            
            # обработка полученного батча
            self.process_batch() 
            
            # сброс кэша после завершения обработки батча
            self.AmtHand.reset_cache(chunk_size=True, batch_txns=True)
            self.BehavHand.reset_cache(all=False)
            
        self.reset_cache() # сброс всего кэша после завершения активности дропа


# import data_generator.fraud.drops.base
# import data_generator.indev
# import data_generator.configs

# importlib.reload(data_generator.indev)
# importlib.reload(data_generator.configs)
# importlib.reload(data_generator.fraud.drops.base)
# from data_generator.configs import DropDistributorCfg

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


class DropBaseClasses:
    """
    acc_hand: DropAccountHandler. Управление счетами транзакций.
    amt_hand: DropAmountHandler. Управление суммами транзакций.
    part_data: DropTxnPartData. Генерация части данных транзакции:
            гео, ip, город, мерчант id и т.п.
    time_hand: DropTimeHandler. Генерация времени транзакций.
    behav_hand: DistBehaviorHandler| PurchBehaviorHandler. Управление поведением дропа
    """
    def __init__(self, configs: DropConfigBuilder):
        self.acc_hand = configs