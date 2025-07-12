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
    min_drops: int. Минимальное число дропов в accounts для возможности отправки
               переводов другим дропам.
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
        self.outer_accounts = configs.outer_accounts.copy()
        self.min_drops = configs.to_drops["min_drops"]
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
        assert self.client_id != 0, \
            f"client_id is not passed. client_id is {self.client_id}"

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
        # Фильтруем accounts исключая свой счет и выбирая дропов. Для случая если to_drop
        drop_accounts = self.accounts.loc[(self.accounts.client_id != self.client_id) \
                                          & (self.accounts.is_drop == True)]
        
        # Если счетов дропов ещё нет или меньше лимита. Берем внешний неиспользованный счет
        if drop_accounts.shape[0] < self.min_drops:
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
    declined_txns: int. Счетчик отклоненных транзакций.
                      По умолчанию 0.
    chunk_size: int, float. Последний созданный размер части баланса для перевода по частям
                             По умолчанию 0.
    last_amt: int. Последняя сгенерированная сумма.
    first_decl: int. Сумма первой отклоненной транзакции.
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
        self.declined_txns = 0
        self.chunk_size = 0
        self.last_amt = 0
        self.first_decl = 0
        self.chunks = configs.chunks.copy()
        self.reduce_share = configs.reduce_share
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
        # Получение денег, увеличить баланс   
        if receive:
            self.balance += amount
            return
        # Исх. транзакция. Уменьшить баланс    
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

    @property
    def get_atm_share(self):
        """
        Получить случайный коэффициент доли баланса.
        Для снятия в банкомате.
        """
        low = self.chunks["atm_share"]["min"]
        high = self.chunks["atm_share"]["max"]
        return np.random.uniform(low, high)


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
            atm_share = self.get_atm_share
            self.chunk_size = max(atm_min, self.balance * atm_share // self.round * self.round)
            return self.chunk_size
        
        # Если снятие и баланс меньше лимита для atm
        if not online and self.balance < atm_min:
            raise ValueError(f"""If atm withdrawal the balance must be >= atm_min
            balance: {self.balance} atm_min: {atm_min}""")

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


    def count_and_cache(self, declined, amount):
        """
        Счетчик исходящих транзакций.
        Считает все транзакции и отклоненные.
        Кэширует сумму первой отклоненной транзакции
        и любой последней транзакции.
        ----------
        declined: отклонена ли текущая транзакция.
        amount: int | float. Сумма текущей транзакции.
        """
        self.batch_txns += 1
        self.last_amt = amount
        if not declined:
            return
    
        self.declined_txns += 1
        declined_txns = self.declined_txns
        if declined_txns == 1:
            self.first_decl = amount


    def reduce_amt(self, online):
        """
        Уменьшение суммы относительно первой отклоненной транзакции
        """
        balance = self.balance
        trf_min = self.chunks["rcvd_small"]["min"]
        atm_min = self.chunks["atm_min"]
        atm_eligible = balance >= atm_min
        split_eligible = balance >= trf_min * 2

        if online and not split_eligible:
            return balance
        if not online and not atm_eligible:
            return balance
        
        if online: # перевод
            reduce_share = self.reduce_share
            reduce_by = self.first_decl  * reduce_share // self.round * self.round
            reduced_amt = max(trf_min, self.last_amt - reduce_by)
            return reduced_amt
        # снятие
        reduce_share = self.reduce_share
        reduce_by = self.first_decl  * reduce_share // self.round * self.round
        reduced_amt = max(atm_min, self.last_amt - reduce_by)
        return reduced_amt


    def one_operation(self, online, declined=False, in_chunks=False):
        """
        Генерация суммы операции дропа.
        ---------
        online - bool. Перевод или снятие в банкомате.
        declined - bool. Отклонена ли транзакция или одобрена
        in_chunks - bool. Перевод по частям или целиком. Если False, то просто пробуем перевести все с баланса
                          При True нужно указать amount.
        """

        # Если это не первая отклоненная транзакция, то дроп уменьшает сумму транз.
        if self.declined_txns >= 1:
            amount = self.reduce_amt(online=online)
            self.update_balance(amount=amount, receive=False, declined=declined)
            # Прибавляем счетчик транзакций и кэшируем сумму
            self.count_and_cache(declined=declined, amount=amount)
            return amount

        # Если перевод не по частям. Пробуем перевести все с баланса. 
        if not in_chunks:
            amount = self.balance
            self.update_balance(amount=self.balance, receive=False, declined=declined)
            # Прибавляем счетчик транзакций и кэшируем сумму
            self.count_and_cache(declined=declined, amount=amount)
            return amount

        # Иначе генерируем размер части и считаем сколько частей исходя из размера одной части
        amount = self.get_chunk_size(online=online)
        chunks = self.balance // amount

        # Если целое число частей больше 0. Пробуем перевести одну часть
        if chunks > 0:
            self.update_balance(amount=amount, receive=False, declined=declined)
            self.count_and_cache(declined=declined, amount=amount)
            return amount

        # Если баланс меньше одной части. Пробуем перевести то что осталось
        rest = self.balance
        self.update_balance(amount=rest, receive=False, declined=declined)
        self.count_and_cache(declined=declined, amount=amount)
        return rest


    def reset_cache(self, life_end=False):
        """
        Сброс кэшированных значений
        -----------------
        life_end: bool. Если True - сброс всего.
            Если False, то только self.batch_txns
            и self.chunk_size
        """
        if not life_end:
            self.batch_txns = 0
            self.chunk_size = 0
            return
        
        self.batch_txns = 0
        self.balance = 0
        self.chunk_size = 0
        self.last_amt = 0
        self.first_decl = 0
        self.declined_txns = 0
            


