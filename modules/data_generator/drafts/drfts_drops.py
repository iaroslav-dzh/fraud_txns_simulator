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


# Возможный метод для DistBehavior
@property
def to_crypto(self):
    """
    """
    # Если не онлайн, то невозможен перевод в крипту
    if not self.online: 
        return
    
    drop_rate = self.to_drop_rate
    # Возвращаем True или False
    return np.random.uniform(0,1) < drop_rate


def switch_online(self, declined):
    """
    """
    if not declined:
        return
    scen = self.scen
    if scen in ["atm", "atm+transfer"]:
        pass

def switch_online(self, prev_declined):
    """
    Направляет выполнение сценария.
    Записывает True или False в self.online с точки зрения какая 
    должна быть транзакция: онлайн или оффлайн (перевод или снятие).
    ------------
    """
    if not prev_declined:
        return
    
    scen = self.scen
    {"method01->method02":0.6,
     "method01->method01":0.4}
    # В atm+transfer только первая транзакция может быть atm(оффлайн)
    if scen == "atm+transfer" and not self.last_online:
        self.online = True
    elif scen == "atm+transfer"and self.last_online:
        self.online = False
    elif scen == "atm":
        self.online = True
    elif scen in ["split_transfer", "transfer"]:
        self.online = True


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


# `split_rate` = 0.7  
# Тест `large_balance`: баланс больше чем amt_max

# Вероятности сценариев для large_balance
split_rate = 0.7
p_atm_and_trf = split_rate * 0.5
p_split_trf = split_rate * 0.5
p_atm = 1 - split_rate
print(f"""atm+transfer prob: {p_atm_and_trf}
split_transfer prob: {p_split_trf}
atm prob: {p_atm:.3f}""")
assert sum([p_atm_and_trf, p_split_trf, p_atm]) == 1

all_scens1 = []
i = 0
while i < 2000:
    drop_amt1.reset_cache()
    drop_amt1.balance = dist_behav1.trf_max + 1000
    assert drop_amt1.balance > dist_behav1.trf_max, "Balance is below trf_max"
    dist_behav1.sample_scenario()
    # print(dist_behav1.scen)
    all_scens1.append(dist_behav1.scen)
    i += 1
all_scens_ser1 = pd.Series(all_scens1)

all_scens_ser1.value_counts(normalize=True)


# `split_rate` = 0.7  
# Тест `split_eligible`: баланс больше чем `self.amt_min * 2`,  
# но меньше чем `self.amt_max`

# Вероятности сценариев для split_eligible
split_rate = 0.7
p_split_trf = split_rate
p_trf = 1 - split_rate
print(f"""split_transfer prob: {p_split_trf}
transfer prob: {round(p_trf, 3)}""")
assert sum([p_split_trf, p_trf]) == 1

all_scens3 = []
i = 0
while i < 3000:
    drop_amt1.reset_cache()
    drop_amt1.balance = dist_behav1.trf_min * 2
    assert drop_amt1.balance < dist_behav1.atm_min, "Balance exceeds atm_min"
    assert drop_amt1.balance < dist_behav1.trf_max, "Balance exceeds trf_max"
    dist_behav1.sample_scenario()
    all_scens3.append(dist_behav1.scen)
    i += 1
all_scens_ser3 = pd.Series(all_scens3)

all_scens_ser3.value_counts(normalize=True)

# `split_rate` = 0.7  
# баланс меньше чем `DistBehaviorHandler.trf_min * 2`

all_scens4 = []
i = 0
while i < 3000:
    drop_amt1.reset_cache()
    drop_amt1.balance = dist_behav1.trf_min + 1000
    assert drop_amt1.balance < dist_behav1.trf_min * 2, "Balance exceeds the limit"
    dist_behav1.sample_scenario()
    all_scens4.append(dist_behav1.scen)
    i += 1
all_scens_ser4 = pd.Series(all_scens4)

all_scens_ser4.value_counts(normalize=True)


# **`in_chunks_val`**

# Список сценариев
# "split_money"
# "one_purchase"

dist_behav1.scen = "split_money"
dist_behav1.in_chunks_val()
dist_behav1.in_chunks

# **`stop_after_decline`**

dist_behav1.attempts = 2
dist_behav1.stop_after_decline(declined=True)

# **`reset_cache`**

dist_behav1.attempts = 3
dist_behav1.scen = "split_money"
dist_behav1.online = True
dist_behav1.in_chunks = False
dist_behav1.reset_cache(all=True)
dist_behav1.attempts, dist_behav1.scen, dist_behav1.online, dist_behav1.in_chunks

# **`attempts_after_decline`**  
# declined = True/False

att_vals1 = []

for _ in range(100):
    dist_behav1.attempts_after_decline(declined=True)
    att_vals1.append(dist_behav1.attempts)
att_vals_ser1 = pd.Series(att_vals1)

att_vals_ser1.agg(["min","mean", "max"])

# **`deduct_attempts`**

dist_behav1.attempts = 1
dist_behav1.deduct_attempts(declined=True, receive=True)
dist_behav1.attempts

purch_behav1