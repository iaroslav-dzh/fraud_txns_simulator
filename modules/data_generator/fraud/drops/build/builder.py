import pandas as pd
import geopandas as gpd
import numpy as np
import pyarrow
import os
from dataclasses import dataclass
from typing import Union

from data_generator.configs import DropDistributorCfg, DropPurchaserCfg
# from data_generator.fraud.drops.build.config import DropConfigBuilder
from data_generator.fraud.drops.base import DropAccountHandler, DropAmountHandler
from data_generator.fraud.drops.behavior import DistBehaviorHandler, PurchBehaviorHandler
from  data_generator.fraud.txndata import DropTxnPartData
from data_generator.fraud.drops.time import DropTimeHandler

# 1. Агрегатор базовых классов для дропов


class DropBaseClasses:
    """
    Создание объектов основных классов для дропов.
    Можно создать выборочно, можно сразу все
    Объекты пишутся в свои атрибуты.
    --------
    drop_type: str. 'distributor' или 'purchaser'
    configs: DropDistributorCfg | DropPurchaserCfg.
             Данные для создания транзакций: отсюда берем номера 
             счетов клиентов и внешних счетов.
    acc_hand: DropAccountHandler. Генератор номеров счетов входящих/исходящих транзакций.
              Учет использованных счетов.
    amt_hand: DropAmountHandler. Генератор сумм входящих/исходящих транзакций, сумм снятий.
              Управление балансом текущего дропа.
    time_hand: DropTimeHandler.
               Управление временем транзакций дропа.
    behav_hand: DistBehaviorHandler | PurchBehaviorHandler.
                Управление поведением дропа: распределителя или покупателя.
    part_data: DropTxnPartData.
               Генерация части данных о транзакции дропа.
    """
    def __init__(self, drop_type, configs):
        """
        drop_type: str. 'distributor' или 'purchaser'
        configs: DropDistributorCfg | DropPurchaserCfg.
                 Параметры и конфиги для генерации фрода.
        """
        self.drop_type = drop_type
        self.configs = configs
        self.acc_hand = None
        self.amt_hand = None
        self.time_hand = None
        self.behav_hand = None
        self.part_data = None

    def build_acc_hand(self):
        """
        Создать объект DropAccountHandler.
        Объект пишется в атрибут acc_hand.
        """
        self.acc_hand = DropAccountHandler(self.configs)

    def build_amt_hand(self):
        """
        Создать объект DropAmountHandler.
        Объект пишется в атрибут amt_hand.
        """
        self.amt_hand = DropAmountHandler(self.configs)

    def build_time_hand(self):
        """
        Создать объект DropTimeHandler
        Объект пишется в атрибут time_hand.
        """
        self.time_hand = DropTimeHandler(self.configs)

    def build_behav_hand(self):
        """
        Создать объект DistBehaviorHandler или PurchBehaviorHandler.
        Зависит от переданного drop_type при создании этого класса.
        Объект пишется в атрибут behav_hand.
        """
        self.build_amt_hand()
        amt_hand = self.amt_hand
        drop_type = self.drop_type
        configs = self.configs
        
        if drop_type == "distributor":
            self.behav_hand = DistBehaviorHandler(configs=configs, amt_hand=amt_hand)
            
        elif drop_type == "purchaser":
            self.behav_hand = PurchBehaviorHandler(configs=configs, amt_hand=amt_hand)
            
    def build_part_data(self):
        """
        Создать объект DropTxnPartData.
        Объект пишется в атрибут part_data.
        """
        self.part_data = DropTxnPartData(self.configs)

    
    def build_all(self):
        """
        Создать объекты всех классов и записать их в атрибуты.
        """
        self.build_acc_hand()
        self.build_amt_hand()
        self.build_time_hand()
        self.build_behav_hand()
        self.build_part_data()