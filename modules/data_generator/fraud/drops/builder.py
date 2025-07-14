from dataclasses import dataclass
from typing import Union

from data_generator.configs import DropDistributorCfg, DropPurchaserCfg
from data_generator.fraud.drops.base import DropAccountHandler, DropAmountHandler
from data_generator.fraud.drops.behavior import DistBehaviorHandler, PurchBehaviorHandler
from  data_generator.fraud.txndata import DropTxnPartData
from data_generator.fraud.drops.time import DropTimeHandler

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