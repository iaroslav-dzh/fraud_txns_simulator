# Основной инструментарий для дропов


import pandas as pd
import numpy as np

from data_generator.utils import get_values_from_truncnorm
from data_generator.configs import DropDistributorCfg, DropPurchaserCfg

    
# . Управление счетами для переводов.

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


# . Управление балансом и суммами транзакций дропа

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
        self.chunks = configs.chunks.copy()
        self.inbound_amt = configs.inbound_amt.copy()
        self.round = configs.round


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
        online - bool. Онлайн или оффлайн. Перевод или банкомат. Если банкомат, то снимается доля self.chunks["atm_share"] от баланса, 
                 но не меньше self.chunks["atm_min"]
        --------------------
        Возвращает np.int64
        Результат кэшируется в self.chunk_size
        """
        # Если это не первая транзакция в серии транзакции для одной полученной дропом суммы
        # И случайное число больше rand_rate, то просто возвращаем ранее созданный размер чанка
        rand_rate = self.chunks["rand_rate"]
        if self.batch_txns != 0 and np.random.uniform(0,1) > rand_rate:
            return self.chunk_size

        atm_min = self.chunks["atm_min"]
        # Если снятие и баланс больше или равен лимиту для atm
        if not online and self.balance >= atm_min:
            atm_share = self.chunks["atm_share"]
            self.chunk_size = max(atm_min, self.balance * atm_share // self.round * self.round)
            return self.chunk_size
        
        # Если снятие и баланс меньше лимита для atm
        if not online and self.balance < atm_min:
            self.chunk_size = self.balance
            return self.chunk_size

        # Если перевод. 
        # Берем лимиты под генерацию массива чанков, в зависимости от
        # полученной дропом суммы
        small = self.chunks["rcvd_small"]
        medium = self.chunks["rcvd_medium"] 
        large = self.chunks["rcvd_large"]
        step = self.chunks["step"]

        # Обратите внимание, что отталкиваемся от текущего баланса.
        # Если будут исходящие транзакции частями, то баланс будет каждый раз разный.
        # И лимиты на чанки будут в разных диапазонах
        if self.balance <= small["limit"]:
            # print("Condition #1")
            low = min(self.balance, small["min"]) # но не больше суммы на балансе
            high = min(self.balance, small["max"]) # но не больше суммы на балансе

        elif self.balance <= medium["limit"]:
            # print("Condition #2")
            low = min(self.balance, medium["min"])
            high = min(self.balance, medium["max"])

        else:
            # print("Condition #3")
            low = min(self.balance, large["min"])
            high = min(self.balance, large["max"])
   
        # прибавим шаг к максимуму, чтобы было понятнее передавать аргументы в конфиге 
        # и не учитывать исключение значения stop в np.arange
        sampling_array = np.arange(low, high + step, step)
        # Если чанк больше бал
        self.chunk_size = np.random.choice(sampling_array)
        return self.chunk_size
        

    def one_operation(self, online, declined=False, in_chunks=False):
        """
        Генерация суммы операции дропа.
        ---------
        online - bool. Перевод или снятие в банкомате.
        declined - bool. Отклонена ли транзакция или одобрена
        in_chunks - bool. Перевод по частям или целиком. Если False, то просто пробуем перевести все с баланса
                          При True нужно указать amount.
        """

        # Если перевод не по частям. Пробуем перевести все с баланса. 
        if not in_chunks:
            amount = self.balance
            self.update_balance(amount=self.balance, receive=False, declined=declined)
            # Прибавляем счетчик транзакции для текущей партии денег
            self.batch_txns += 1
            return amount

        # Иначе генерируем размер части и считаем сколько частей исходя из размера одной части
        amount = self.get_chunk_size(online=online)
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


# .

class DistBehaviorHandler:
    """
    Управление поведением дропа распределителя. Выбор сценария.
    ----------
    Атрибуты:
    --------
    split_rate: float. Доля случаев когда полученная сумма будет распределена по частям.
                При условии что полученная сумма пройдет по лимитам.
    attempts: int. Сколько попыток совершить операцию будет сделано 
               дропом после первой отклоненной транзакции. По умолчанию 0.
    first_txn: bool. Является ли транзакция первой исходящей в текущей партии.
    low: int. Минимальное количество попыток совершить операцию после первой отклоненной
              операции.
    high: int. Максимальное количество попыток совершить операцию после первой отклоненной
              операции.
    """

    def __init__(self, configs: DropDistributorCfg, amt_hand: DropAmountHandler):
        """
        configs: DropDistributorCfg. Конфиги и данные для создания дроп транзакций.
        amt_hand: DropAmountHandler. Отсюда узнаем текущий баланс.
        
        """
        self.scen = None
        self.configs = configs
        self.amt_hand = amt_hand
        self.atm_min = amt_hand.chunks["atm_min"]
        self.trf_min = amt_hand.chunks["rcvd_small"]["min"]
        self.trf_lim = amt_hand["trf_lim"]
        self.split_rate = configs["distributor"]["split_rate"]
        self.attempts = 0
        self.in_lim = configs["distributor"]["in_lim"]
        self.out_lim = configs["distributor"]["out_lim"]
        self.first_txn = True
        self.low = configs["distributor"]["low"]
        self.high = configs["distributor"]["high"]


    def sample_scenario(self):
        """
        Выбор сценария поведения дропа с учётом текущего баланса и актуальных лимитов.
        """
        balance = self.amt_hand.balance
        split = np.random.uniform(0,1) <= self.split_rate

        # Минимальный баланс для переводов по частям будет самый маленький возможный размер чанка
        # self.trf_min умноженный на 2
        large_balance = balance > self.trf_lim
        atm_eligible = balance >= self.atm_min
        split_eligible = balance >= self.trf_min * 2

        # Список: (условие, список возможных сценариев)
        conditions = [
            (large_balance and split, ["split_transfer", "atm+transfer"]),
            (large_balance, ["atm"]),
            (atm_eligible and split, ["split_transfer", "atm+transfer"]),
            (atm_eligible, ["transfer", "atm"]),
            (split_eligible and split, ["split_transfer"])
        ]

        for cond, scen_list in conditions:
            if cond:
                self.scen = np.random.choice(scen_list)

        # Если ни одно из условий не сработало — fallback
        self.scen = "transfer"


    def guide_scenario(self):
        """
        Направляет выполнение сценария.
        Возврщает True или False с точки зрения какая должна быть транзакция:
        онлайн или оффлайн (перевод или снятие).
        """
        scen = self.scen

        # В atm+transfer только первая транзакция может быть atm(оффлайн)
        if scen == "atm+transfer" and self.first_txn:
            online = False
        elif scen == "atm+transfer":
            online = True
        elif scen == "atm":
            online = False
        elif scen in ["split_transfer", "transfer"]:
            online = True

        self.first_txn = False
        return online


    def stop_after_decline(self, declined):
        """
        Будет ли дроп пытаться еще после отклоненной операции
        или остановится.
        Подразумевается что этот метод используется в цикле перед
        методом self.limit_reached()
        ---------------
        declined: bool. Отклонена ли предыдущая операция.
                  
        """
        # Если предыдущая транзакция уже была отклонена
        if not declined:
            return
        if self.attempts == 0:
            return True
        if self.attempts > 0:
            return False

            
    def attempts_after_decline(self):
        """
        Определение количества попыток после первой отклоненной транзакции
        ---------------
        low - int. Минимальное число попыток
        high - int. Максимальное число попыток.
        """
        self.attempts = np.random.randint(self.low, self.high + 1)
            
        
    def deduct_attempts(self, declined, receive):
        """
        Вычитание попытки операции совершенной при статусе declined
        ---------------
        declined - bool. Отклоняется ли текущая транзакция
        receive - bool. Является ли транзакция входящей
        """
        if self.attempts == 0:
            return 
        if declined and not receive:
            self.attempts -= 1


    def reset_cache(self):
        """
        Сброс кэшированных данных
        -------------
        """
        self.attempts = 0