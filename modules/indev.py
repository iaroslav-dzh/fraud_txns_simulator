import pandas as pd
import numpy as np

from data_generator.configs import DropDistributorCfg
from data_generator.fraud.drops.base import DropAmountHandler, DropAccountHandler
from data_generator.fraud.drops.time import DropTimeHandler
from data_generator.fraud.txndata import DropTxnPartData
from data_generator.fraud.drops.dist import DistBehaviorHandler
from data_generator.utils import build_transaction

class CreateDistTxn:
    """
    Создание транзакций дропа распределителя под разное поведение.
    -----------------
    txn_part_data: DropTxnPartData. Генератор части данных транзакции - мерчант,
                    гео, ip, девайс и т.п.
    amt_hand: DropAmountHandler. Генератор активности дропов: суммы, счета, баланс.
    acc_hand: DropAccountHandler. Генератор номеров счетов входящих/исходящих транзакций.
                Учет использованных счетов.
    time_hand: DropTimeHandler. Управление временем транзакций дропа
    behav_hand: DistBehaviorHandler. Управление поведением дропа.
    in_txns: int. Количество входящих транзакций.
    out_txns: int. Количество исходящих транзакций.
    in_lim: int. Лимит входящих транзакций. Транзакции клиента совершенные после 
            достижения этого лимита отклоняются.
    out_lim: int. Лимит исходящих транзакций. Транзакции клиента совершенные 
                после достижения этого лимита отклоняются.
    last_txn: dict. Полные данные последней транзакции. По умолчанию None
    """
    def __init__(self, configs: DropDistributorCfg, txn_part_data: DropTxnPartData, \
                 amt_hand: DropAmountHandler, acc_hand: DropAccountHandler, \
                 time_hand: DropTimeHandler, behav_hand: DistBehaviorHandler, \
                 categories=None | pd.DataFrame):
        """
        configs: DropDistributorCfg. Конфиги и данные для создания дроп транзакций.
        txn_part_data: DropTxnPartData. Генератор части данных транзакции - мерчант,
                       гео, ip, девайс и т.п.
        amt_hand: DropAmountHandler. Генератор активности дропов: суммы, счета, баланс.
        acc_hand: DropAccountHandler. Генератор номеров счетов входящих/исходящих транзакций.
                  Учет использованных счетов.
        time_hand: DropTimeHandler. Управление временем транзакций дропа
        behav_hand: DistBehaviorHandler. Управление поведением дропа.
                 после достижения этого лимита отклоняются.
        categories: pd.DataFrame. Категории товаров с весами. Для дропов покупателей.
        """
        self.txn_part_data = txn_part_data
        self.amt_hand = amt_hand
        self.acc_hand = acc_hand
        self.time_hand = time_hand
        self.behav_hand = behav_hand
        self.in_txns = 0
        self.out_txns = 0
        self.in_lim = configs.in_lim
        self.out_lim = configs.out_lim
        self.to_drop_rate = configs.to_drop_rate
        self.categories = categories
        self.last_txn = None


    def trf_or_atm(self, declined, receive=False):
        """
        Один входящий/исходящий перевод либо одно снятие в банкомате.
        ---------------------
        declined - bool. Будет ли текущая транзакция отклонена.
        receive - входящий перевод или нет.
        """
        client_id = self.txn_part_data.client_info.client_id # берем из namedtuple
        
        # Время транзакции. Оно должно быть создано до увеличения счетчика self.in_txns
        txn_time, txn_unix = self.time_hand.get_txn_time(receive=receive, in_txns=self.in_txns)

        self.behav_hand.sample_scenario()
        # self.behav_hand.in_chunks_val() вызывается вовне. До захода в цикл while balance > 0

        online = self.behav_hand.online
        in_chunks = self.behav_hand.in_chunks
        to_drop_rate = self.to_drop_rate

        # перевод дропу
        if receive:
            self.in_txns += 1
            amount = self.amt_hand.receive(declined=declined)
            account = self.acc_hand.account
        # перевод от дропа    
        elif not receive and online:
            to_drop = np.random.choice([True, False], p=[to_drop_rate, 1 - to_drop_rate])
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
            amount = self.amt_hand.one_operation(declined=declined, in_chunks=in_chunks)

        if declined:
            status = "declined"
            is_fraud = True
            rule = "drop_flow_cashout"
        else:
            status = "approved"
            is_fraud = False
            rule = "not applicable"

        # Статичные характеристики
        is_suspicious = False
        category_name="not applicable"

        # Сборка всех данных в транзакцию и запись как послдней транзакции
        self.last_txn = build_transaction(client_id=client_id, txn_time=txn_time, txn_unix=txn_unix, amount=amount, \
                                          type=type, channel=channel, category_name=category_name, online=online, \
                                          merchant_id=merchant_id, trans_city=trans_city, trans_lat=trans_lat, \
                                          trans_lon=trans_lon, trans_ip=trans_ip, device_id=device_id, account=account, \
                                          is_fraud=is_fraud, is_suspicious=is_suspicious, status=status, rule=rule)
        return self.last_txn


    def purchase(self, declined, to_crypto):
        """
        Покупка дропом. На данный момент для крипты.
        --------------
        declined: bool. Будет ли текущая транзакция отклонена.
        to_crypto: bool. Будет ли это перевод на криптобиржу.
        """
        client_id = self.txn_part_data.client_info.client_id # берем из namedtuple
        receive = False

        # Время транзакции
        txn_time, txn_unix = self.time_hand.get_txn_time(receive=receive, in_txns=self.in_txns)
        online = self.behav_hand.online
        in_chunks = self.behav_hand.in_chunks
        self.out_txns += 1

        # Генерация части данных транзакции. Здесь прописывается аргумент online
        merchant_id, trans_lat, trans_lon, trans_ip, trans_city, device_id, _, type = \
                                                self.txn_part_data.original_purchase(online=online)
        
        if to_crypto:
            channel = "crypto_exchange"
            category_name = "balance_top_up"
        else:
            channel = "ecom"
            category_name = self.categories.category \
                                .sample(1, weights=self.categories.weight).iat[0]

        amount = self.amt_hand.one_operation(declined=declined, in_chunks=in_chunks)

        if declined:
            status = "declined"
            is_fraud = True
            rule = "drop_flow_cashout"
        else:
            status = "approved"
            is_fraud = False
            rule = "not applicable"

        # Статичные характеристики
        is_suspicious = False
        account = np.nan

        # Сборка всех данных в транзакцию и запись как послдней транзакции
        self.last_txn = build_transaction(client_id=client_id, txn_time=txn_time, txn_unix=txn_unix, amount=amount, \
                                          type=type, channel=channel, category_name=category_name, online=online, \
                                          merchant_id=merchant_id, trans_city=trans_city, trans_lat=trans_lat, \
                                          trans_lon=trans_lon, trans_ip=trans_ip, device_id=device_id, account=account, \
                                          is_fraud=is_fraud, is_suspicious=is_suspicious, status=status, rule=rule)
        


    
    def limit_reached(self):
        """
        Проверка достижения лимитов входящих и исходящих транзакций
        Сверка с self.in_lim и self.out_lim
        ------------------------
        Вернет True если какой либо лимит достигнут
        """
        if self.in_lim == self.in_txns:
            return True
        if self.out_lim == self.out_txns:
            return True
        return False


    def reset_cache(self, only_counters=False):
        """
        Сброос кэшированных данных
        -------------
        only_counters: bool. Если True будут сброшены: self.in_txns, self.out_txns.
                       Если False то также сбросится информация о последней транзакции self.last_txn
        """
        
        self.in_txns = 0
        self.out_txns = 0
        if only_counters:
            return
        
        self.last_txn = {}