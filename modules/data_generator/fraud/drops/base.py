# Основной инструментарий для дропов


import pandas as pd
import numpy as np
from dataclasses import dataclass

from data_generator.utils import get_values_from_truncnorm

# . Датакласс для конфигов транзакций дропов-распределителей

@dataclass
class DropDistributorCfg:
    """
    Это данные на основе которых будут генерироваться транзакции дропов-распределителей
    ---------------------
    clients: pd.DataFrame
    timestamps: pd.DataFrame
    transactions: pd.DataFrame
    accounts: pd.DataFrame. Номера счетов клиентов. 
    outer_accounts: pd.Series. Номера внешних счетов для транзакций вне банка.
    client_devices: pd.DataFrame
    offline_merchants: pd.DataFrame
    categories: pd.DataFrame
    online_merchant_ids: pd.Series
    time_weights_dict: dict
    rules: pd.DataFrame
    cities: pd.DataFrame
    fraud_amounts: pd.DataFrame
    period_in_lim: int. Количество входящих транзакций после которых дроп уходит на паузу.
    period_out_lim: int. Количество исходящих транзакций после которых дроп уходит на паузу.
    lag_interval: int. Минуты. На сколько дроп должен делать паузу после
                       достижения лимита входящих и/или исходящих транзакций.
                       Например 1440 минут(сутки). Отсчет идет от первой транзакции в последнем периоде активности.
    two_way_delta: dict. Минимум и максимум дельты времени. Для случаев когда дельта может быть и положительной и отрицательной.
                         Эта дельта прибавляется к lag_interval, чтобы рандомизировать время возобновления активности,
                         чтобы оно не было ровным. Берется из конфига drops.yaml
    pos_delta: dict. Минимум и максимум случайной дельты времени в минутах. Для случаев когда дельта может быть только положительной.
                          Эта дельта - промежуток между транзакциями дропа в одном периоде. Просто прибавляется ко времени последней транзакции.
    chunks: dict. Характеристики для генератора сумм транзакций по частям.
    inbound_amt: dict. Настройки для сумм входящих транзакций
    round: int. Округление целой части сумм транзакций. Напр. 500 значит что суммы будут кратны 500 - кончаться на 500 или 000
    """
    clients: pd.DataFrame
    timestamps: pd.DataFrame
    transactions: pd.DataFrame
    accounts: pd.DataFrame
    outer_accounts: pd.Series
    client_devices: pd.DataFrame
    offline_merchants: pd.DataFrame
    categories: pd.DataFrame
    online_merchant_ids: pd.Series
    time_weights_dict: dict
    rules: pd.DataFrame
    cities: pd.DataFrame
    fraud_amounts: pd.DataFrame
    period_in_lim: int
    period_out_lim: int
    lag_interval: int
    two_way_delta: dict
    pos_delta: dict
    chunks: dict
    inbound_amt: dict
    round: dict


# . Датакласс для конфигов транзакций дропов-покупателей 

@dataclass
class DropPurchaserCfg: # <-------------------- in development. Совсем не откорректирован.
    """
    Это данные на основе которых будут генерироваться транзакции дропов-покупателей
    ---------------------
    clients: pd.DataFrame
    timestamps: pd.DataFrame
    transactions: pd.DataFrame
    accounts: pd.DataFrame. Номера счетов клиентов. 
    outer_accounts: pd.Series. Номера внешних счетов для транзакций вне банка.
    client_devices: pd.DataFrame
    offline_merchants: pd.DataFrame
    categories: pd.DataFrame
    online_merchant_ids: pd.Series
    time_weights_dict: dict
    rules: pd.DataFrame
    cities: pd.DataFrame
    fraud_amounts: pd.DataFrame
    period_in_lim: int. Количество входящих транзакций после которых дроп уходит на паузу.
    period_out_lim: int. Количество исходящих транзакций после которых дроп уходит на паузу.
    lag_interval: int. Минуты. На сколько дроп должен делать паузу после
                       достижения лимита входящих и/или исходящих транзакций.
                       Например 1440 минут(сутки). Отсчет идет от первой транзакции в последнем периоде активности.
    two_way_delta: dict. Минимум и максимум дельты времени. Для случаев когда дельта может быть и положительной и отрицательной.
                         Эта дельта прибавляется к lag_interval, чтобы рандомизировать время возобновления активности,
                         чтобы оно не было ровным. Берется из конфига drops.yaml
    pos_delta: dict. Минимум и максимум случайной дельты времени в минутах. Для случаев когда дельта может быть только положительной.
                          Эта дельта - промежуток между транзакциями дропа в одном периоде. Просто прибавляется ко времени последней транзакции.
    chunks: dict. Характеристики для генератора сумм транзакций по частям.
    inbound_amt: dict. Настройки для сумм входящих транзакций
    round: int. Округление целой части сумм транзакций. Напр. 500 значит что суммы будут кратны 500 - кончаться на 500 или 000
    """
    clients: pd.DataFrame
    timestamps: pd.DataFrame
    transactions: pd.DataFrame
    accounts: pd.DataFrame
    outer_accounts: pd.Series
    client_devices: pd.DataFrame
    offline_merchants: pd.DataFrame
    categories: pd.DataFrame
    online_merchant_ids: pd.Series
    time_weights_dict: dict
    rules: pd.DataFrame
    cities: pd.DataFrame
    fraud_amounts: pd.DataFrame
    period_in_lim: int
    period_out_lim: int
    lag_interval: int
    two_way_delta: dict
    pos_delta: dict
    chunks: dict
    inbound_amt: dict
    round: dict


# .

class DropAccountHandler:
    """
    Генератор номеров счетов входящих/исходящих транзакций.
    Учет использованных счетов.
    -------------
    Атрибуты:
    --------
    accounts - pd.DataFrame. Счета клиентов. Колонки: |client_id|account_id|is_drop|
    outer_accounts - pd.Series. Номера внешних счетов - вне нашего банка.
    client_id - int. id текущего дропа. По умолчанию 0.
    account - int. Номер счета текущего дропа. По умолчанию 0.
    used_accounts - pd.Series. Счета на которые дропы уже отправляли деньги.
                    По умолчанию пустая. name="account_id"
    """

    def __init__(self, configs: DropDistributorCfg):
        """
        configs - pd.DataFrame. Данные для создания транзакций: отсюда берем номера счетов клиентов и внешних счетов.
        """
        self.accounts = configs.accounts.copy()
        self.outer_accounts = configs.outer_accounts
        self.client_id = 0
        self.account = 0
        self.used_accounts = pd.Series(name="account_id")

    def get_account(self, own=False, to_drop=False):
        """
        Номер счета входящего/исходящего перевода
        ---------------------
        own - bool. Записать номер своего счета в self.account
        to_drop - bool. Перевод другому дропу в нашем банке или нет.
        """
        if own:
            self.account = self.accounts.loc[self.accounts.client_id == self.client_id, "account_id"].iat[0]
            return

        # Если отправляем/получаем из другого банка.  
        if not to_drop:
            # Семплируем номер внешнего счета который еще не использовался
            account = self.outer_accounts.loc[~(self.outer_accounts.isin(self.used_accounts))].sample(1).iat[0]
            # Добавляем этот счет в использованные как последнюю запись в серии
            self.used_accounts.loc[self.used_accounts.shape[0]] = account
            return account
        
        # Если надо отправить другому дропу в нашем банке. При условии что есть другие дропы на текущий момент
        # Фильтруем accounts исключая свой счет и использованные счета, и выбирая дропов. Для случая если to_drop
        drop_accounts = self.accounts.loc[(self.accounts.client_id != self.client_id) & (self.accounts.is_drop == True) \
                                          & ~(self.accounts.account_id.isin(self.used_accounts))]
        
        # Если счетов дропов ещё нет. Берем внешний неиспользованный счет
        if drop_accounts.empty:
            account = self.outer_accounts.loc[~(self.outer_accounts.isin(self.used_accounts))].sample(1).iat[0]
            self.used_accounts.loc[self.used_accounts.shape[0]] = account
            return account

        # Дропы есть
        account = drop_accounts.account_id.sample(1).iat[0]
        # Добавляем этот счет в использованные как последнюю запись в серии
        self.used_accounts.loc[self.used_accounts.shape[0]] = account
        return account

    def label_drop(self):
        """
        Обозначить клиента как дропа в self.accounts
        """
        self.accounts.loc[self.accounts.client_id == self.client_id, "is_drop"] = True


    def reset_cache(self):
        """
        Сброс кэшированных значений.
        Это только использованные номера счетов
        """
        self.used_accounts = pd.Series(name="account_id")

# .
class DropAmountHandler: 
    """
    Генератор сумм входящих/исходящих транзакций, сумм снятий.
    Управление балансом текущего дропа.
    -------------
    Атрибуты:
    balance: float, int. Текущий баланс дропа. По умолчанию 0.
    batch_txns: int. Счетчик транзакций сделанных в рамках распределения полученной партии денег.
                      По умолчанию 0.
    chunk_size: int, float. Последний созданный размер части баланса для перевода по частям
                             По умолчанию 0.
    chunks: dict. Содержит ключи:
        atm_min: int. Минимальная сумма для снятий в банкомате.
        atm_share: float. Доля от баланса, которую дроп снимает в случае снятия в банкомате
        low: int. Минимальная сумма исходящего перевода частями.
        high: int. Максимальная сумма исходящего перевода частями.
        step: int. Шаг возможных сумм. Чем меньше шаг, тем больше вариантов.
        rand_rate: float. От 0 до 1.
                   Процент случаев, когда каждый НЕ первый чанк будет случайным и не зависеть от предыдущего.
                   Но возможны случайные совпадаения с предыдущим размером чанка.
                   Доля случайных размеров подряд будет:
                   p(c) - вероятность взять определенный размер (зависит от размера выборки чанков)
                   p(r) - rand_rate
                   p(r) - (p(r) * p(c)). Например p(r) = 0.9; и 5 вариантов размеров чанка - p(c) = 0.20
                   0.9 - (0.9 * 0.2) = 0.72
                   В около 72% случаев размеры чанков не будут подряд одинаковыми 
    inbound_amt: dict. Настройки для сумм входящих транзакций. Содержит ключи:
        low: int
        high: int
        mean: int
        std: int
    round: int. Округление целой части сумм транзакций. Напр. 500 значит что суммы будут кратны 500 - кончаться на 500 или 000                  
    """

    def __init__(self, configs: DropDistributorCfg | DropPurchaserCfg):
        """
        configs: DropDistributorCfg | DropPurchaserCfg. Данные на основании, которых генерируются транзакции.
                 Отсюда берутся: atm_min, atm_share, min, step, rand_rate.
        """
        self.balance = 0
        self.batch_txns = 0
        self.chunk_size = 0
        self.chunks = configs.chunks
        self.inbound_amt = configs.inbound_amt
        self.round = configs.round
        # self.atm_min = configs.chunks["atm_min"]
        # self.atm_share = configs.chunks["atm_share"]
        # self.min = configs.chunks["min"]
        # self.max = configs.chunks["max"]
        # self.step = configs.chunks["step"]
        # self.rand_rate = configs.chunks["rand_rate"]

    def update_balance(self, amount, receive=False, declined=False):
        """
        Увеличить/уменьшить баланс на указанную сумму
        -------------------
        amount - float, int.
        receive - bool. Входящая ли транзакция. Прибавлять сумму или отнимать.
        declined - bool. Отклонена ли транзакция или одобрена.
        """
        # Не обновлять баланс если транзакция отклонена.
        if declined:
            return
            
        # Увеличить баланс   
        if receive:
            self.balance += amount
            return
            
        # Уменьшить баланс    
        self.balance -= amount

    def receive(self, declined):
        """
        Генерация суммы входящего перевода
        --------------------------
        declined - bool. Отклонена ли транзакция или одобрена
        """
        low = self.inbound_amt["low"]
        high = self.inbound_amt["high"]
        mean = self.inbound_amt["mean"]
        std = self.inbound_amt["std"]

        # Генерация суммы. Округление целой части при необходимости
        amount = get_values_from_truncnorm(low_bound=low, high_bound=high, mean=mean, std=std)[0] // self.round * self.round
        
        # Обновляем баланс если транзакция не отклонена
        self.update_balance(amount=amount, receive=True, declined=declined)
        
        return amount

    def get_chunk_size(self, online=False):
        """
        Вернуть случайный размер суммы перевода для перевода по частям
        либо вернуть долю от баланса для снятия/перевода по частям.
        -------------------------------
        online - bool. Онлайн или оффлайн. Перевод или банкомат. Если банкомат, то снимается доля atm_share от баланса, но не меньше atm_min
        --------------------
        Возвращает np.int64
        Результат кэшируется в self.chunk_size
        """
        # Если это не первая транзакция в серии транзакции для одной полученной дропом суммы
        # И случайное число больше rand_rate, то просто возвращаем ранее созданный размер чанка
        rand_rate = self.chunks["rand_rate"]
        if self.batch_txns != 0 and np.random.uniform(0,1) > rand_rate:
            return self.chunk_size

        # Если перевод
        if online:
            low = self.chunks["low"]
            high = self.chunks["high"]
            step = self.chunks["step"]
            # прибавим шаг, чтобы было понятнее передавать аргументы в конфиге и не учитывать исключение stop в np.arange
            sampling_array = np.arange(low, high + step, step)
            self.chunk_size = np.random.choice(sampling_array)
            return self.chunk_size
        
        # Если снятие
        atm_min = self.chunk["atm_min"]
        atm_share = self.chunk["atm_share"]
        self.chunk_size = max(atm_min, self.balance * atm_share // self.round * self.round)
        return self.chunk_size
            
        
    def one_operation(self, amount=0, declined=False, in_chunks=False):
        """
        Генерация суммы операции дропа.
        ---------
        amount - float, int. Сумма перевода если перевод по частям - in_chunks == True
        declined - bool. Отклонена ли транзакция или одобрена
        in_chunks - bool. Перевод по частям или целиком. Если False, то просто пробуем перевести все с баланса
                          При True нужно указать amount.
        """
        if in_chunks and amount <= 0:
            raise ValueError(f"""If in_chunks is True, then amount must be greater than 0.
Passed amount: {amount}""")

        # Если перевод не по частям. Пробуем перевестит все с баланса. 
        if not in_chunks:
            amount = self.balance
            self.update_balance(amount=self.balance, receive=False, declined=declined)
            # Прибавляем счетчик транзакции для текущей партии денег
            self.batch_txns += 1
            return amount

        # Иначе считаем сколько частей исходя из размера одной части
        chunks = self.balance // amount

        # Если целое число частей больше 0. Пробуем перевести одну часть
        if chunks > 0:
            self.update_balance(amount=amount, receive=False, declined=declined)
            self.batch_txns += 1
            return amount

        # Если баланс меньше одной части. Пробуем перевести то что осталось
        rest = self.balance
        self.update_balance(amount=rest, receive=False, declined=declined)
        self.batch_txns += 1
        return rest


    def reset_cache(self, balance=True, chunk_size=True, batch_txns=True):
        """
        Сброс кэшированных значений
        По умолчанию сбрасывается всё. Если что-то надо оставить, то надо выставить False
        для этого
        -----------------
        balance - bool
        chunk_size - bool
        batch_txns - bool
        """
        if balance:
            self.balance = 0
        if chunk_size:
            self.chunk_size = 0
        if batch_txns:
            self.batch_txns = 0