import pandas as pd
import numpy as np
from typing import Union

from data_generator.configs import DropDistributorCfg, DropPurchaserCfg
from data_generator.fraud.drops.build.builder import DropBaseClasses
from data_generator.utils import build_transaction

class CreateDropTxn:
    """
    Создание транзакций дропа распределителя под разное поведение.
    -----------------
    drop_type: str. 'distributor' или 'purchaser'
    configs: DropDistributorCfg | DropPurchaserCfg.
             Конфиги и данные для создания дроп транзакций.
    txn_part_data: DropTxnPartData. Генератор части данных транзакции - мерчант,
                    гео, ip, девайс и т.п.
    amt_hand: DropAmountHandler. Генератор активности дропов: суммы, счета, баланс.
    acc_hand: DropAccountHandler. Генератор номеров счетов входящих/исходящих транзакций.
                Учет использованных счетов.
    time_hand: DropTimeHandler. Управление временем транзакций дропа
    behav_hand: DistBehaviorHandler. Управление поведением дропа.
    categories: pd.DataFrame. Категории товаров с весами. Для дропов покупателей.
    in_txns: int. Количество входящих транзакций.
    out_txns: int. Количество исходящих транзакций.
    in_lim: int. Лимит входящих транзакций. Транзакции клиента совершенные после 
            достижения этого лимита отклоняются.
    out_lim: int. Лимит исходящих транзакций. Транзакции клиента совершенные 
                после достижения этого лимита отклоняются.
    last_txn: dict. Полные данные последней транзакции. По умолчанию None
    """
    def __init__(self, configs: Union[DropDistributorCfg, DropPurchaserCfg], base: DropBaseClasses):
        """
        configs: DropDistributorCfg | DropPurchaserCfg.
                 Конфиги и данные для создания дроп транзакций.
        base: DropBaseClasses. Объекты основных классов для дропов.
        """
        self.drop_type = base.drop_type
        self.configs = configs
        self.txn_part_data = base.part_data
        self.amt_hand = base.amt_hand
        self.acc_hand = base.acc_hand
        self.time_hand = base.time_hand
        self.behav_hand = base.behav_hand
        self.in_txns = 0
        self.out_txns = 0
        self.in_lim = configs.in_lim
        self.out_lim = configs.out_lim
        if isinstance(self.configs, DropPurchaserCfg):
            self.categories = configs.categories
        self.last_txn = None

    def category_and_channel(self):
        """
        Генерация категории и канала транзакции
        ---------------
        """
        drop_type = self.drop_type
        
        # Перевод на криптобиржу
        if drop_type == "distributor":
            channel = "crypto_exchange"
            category_name = "balance_top_up"
            return channel, category_name
        
        assert drop_type == "purchaser", \
            f"""'ecom' and categories sampling work only for self.drop_type as 'purchaser'.
            But {self.drop_type} was passed"""
        
        # Покупка в интернете
        channel = "ecom"
        category_name = self.categories.category \
                                .sample(1, weights=self.categories.weight).iat[0]
        return channel, category_name

    def status_and_rule(self, declined):
        """
        Статус транзакции, флаг is_fraud и правило.
        Зависит от типа дропа self.drop_type
        -----------------
        declined: bool. Будет ли текущая транзакция отклонена.
        """
        drop_type = self.drop_type

        if declined and drop_type == "distributor":
            status = "declined"
            is_fraud = True
            rule = "drop_flow_cashout"
            return status, is_fraud, rule
        
        if not declined and drop_type == "distributor":
            status = "approved"
            is_fraud = False
            rule = "not applicable"
            return status, is_fraud, rule
        
        if declined and drop_type == "purchaser":
            status = "declined"
            is_fraud = True
            rule = "drop_purchaser"
            return status, is_fraud, rule
        
        if not declined and drop_type == "purchaser":
            status = "approved"
            is_fraud = False
            rule = "not applicable"
            return status, is_fraud, rule
        

    def trf_or_atm(self, declined, to_drop, receive=False):
        """
        Один входящий/исходящий перевод либо одно снятие в банкомате.
        ---------------------
        dist: bool. Тип дропа. True - distributor. False - purchaser.
        declined: bool. Будет ли текущая транзакция отклонена.
        receive: входящий перевод или нет.
        """
        client_id = self.txn_part_data.client_info.client_id # берем из namedtuple
        
        # Время транзакции. Оно должно быть создано до увеличения счетчика self.in_txns
        txn_time, txn_unix = self.time_hand.get_txn_time(receive=receive, in_txns=self.in_txns)

        online = self.behav_hand.online
        in_chunks = self.behav_hand.in_chunks

        # перевод дропу
        if receive:
            self.in_txns += 1
            amount = self.amt_hand.receive(declined=declined)
            account = self.acc_hand.account
            online = True # Тут отдельно прописываем т.к. это вне сценариев поведения самого дропа
        # перевод от дропа    
        elif not receive and online:
            self.out_txns += 1
            account = self.acc_hand.get_account(to_drop=to_drop)
        # снятие дропом
        elif not receive and not online:
            account = self.acc_hand.account
            self.out_txns += 1
        
        # Генерация части данных транзакции. Здесь прописываются аргументы online и receive
        merchant_id, trans_lat, trans_lon, trans_ip, trans_city, device_id, channel, type = \
                                                self.txn_part_data.original_data(online=online, receive=receive)
        
        # Генерация суммы если исходящая транзакция
        # т.к. этот метод и для входящих транзакций
        # а у входящих транзакций своя генерация суммы
        if not receive:
            amount = self.amt_hand.one_operation(online=online, declined=declined, in_chunks=in_chunks)

        status, is_fraud, rule = self.status_and_rule(declined=declined)

        # Статичные характеристики
        is_suspicious = False
        category_name="not applicable"

        # Сборка всех данных в транзакцию и запись как последней транзакции
        self.last_txn = build_transaction(client_id=client_id, txn_time=txn_time, txn_unix=txn_unix, amount=amount, \
                                          type=type, channel=channel, category_name=category_name, online=online, \
                                          merchant_id=merchant_id, trans_city=trans_city, trans_lat=trans_lat, \
                                          trans_lon=trans_lon, trans_ip=trans_ip, device_id=device_id, account=account, \
                                          is_fraud=is_fraud, is_suspicious=is_suspicious, status=status, rule=rule)
        return self.last_txn


    def purchase(self, declined):
        """
        Покупка дропом. На данный момент для крипты.
        --------------
        declined: bool. Будет ли текущая транзакция отклонена.
        dist: bool. Это дроп распределитель или покупатель. 
              У распределителя будет перевод на криптобиржу.
              У покупателя - покупка в интернете.
        """
        client_id = self.txn_part_data.client_info.client_id # берем из namedtuple
        receive = False

        # Время транзакции
        txn_time, txn_unix = self.time_hand.get_txn_time(receive=receive, in_txns=self.in_txns)
        online = self.behav_hand.online
        # self.behav_hand.in_chunks_val() вызывается вовне. До захода в цикл while balance > 0
        in_chunks = self.behav_hand.in_chunks
        self.out_txns += 1

        # Брать ли данные последней транзакции. Для случаев когда это дроп распределитель
        dist = self.drop_type == "distributor"
        get_cached = self.txn_part_data.check_previous(dist=dist, last_full=self.last_txn)

        # Генерация части данных транзакции. Здесь прописывается аргумент online
        # Вместо channel нижнее подчеркивание т.к. этот метод вернет None для channel
        merchant_id, trans_lat, trans_lon, trans_ip, trans_city, device_id, _, txn_type = \
                                            self.txn_part_data.original_purchase(online=online, get_cached=get_cached)
        
        amount = self.amt_hand.one_operation(online=online, declined=declined, in_chunks=in_chunks)

        channel, category_name = self.category_and_channel()
        status, is_fraud, rule = self.status_and_rule(declined=declined)

        # Статичные характеристики
        is_suspicious = False
        account = np.nan

        # Сборка всех данных в транзакцию и запись как послдней транзакции
        self.last_txn = build_transaction(client_id=client_id, txn_time=txn_time, txn_unix=txn_unix, amount=amount, \
                                          type=txn_type, channel=channel, category_name=category_name, online=online, \
                                          merchant_id=merchant_id, trans_city=trans_city, trans_lat=trans_lat, \
                                          trans_lon=trans_lon, trans_ip=trans_ip, device_id=device_id, account=account, \
                                          is_fraud=is_fraud, is_suspicious=is_suspicious, status=status, rule=rule)
        return self.last_txn
        

    def limit_reached(self):
        """
        Проверка достижения лимитов входящих и исходящих транзакций
        Сверка с self.in_lim и self.out_lim
        ------------------------
        Вернет True если какой либо лимит достигнут
        """
        if self.in_lim <= self.in_txns:
            return True
        if self.out_lim <= self.out_txns:
            return True
        return False


    def reset_cache(self, only_counters=False):
        """
        Сброос кэшированных данных
        -------------
        only_counters: bool. Если True будут сброшены: self.in_txns, self.out_txns.
                       Если False то также сбросится информация 
                       о последней транзакции self.last_txn
        """
        self.in_txns = 0
        self.out_txns = 0
        if only_counters:
            return
        
        self.last_txn = None