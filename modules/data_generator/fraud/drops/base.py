# Основной инструментарий для дропов


import pandas as pd
import numpy as np

from data_generator.utils import DropConfigs

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

    def __init__(self, configs: DropConfigs):
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


class DropAmountHandler: 
    """
    Генератор сумм входящих/исходящих транзакций, сумм снятий.
    Управление балансом текущего дропа.
    """
    def __init__(self, accounts, account, outer_accounts, balance=0, batch_txns=0, chunk_size=0, used_accounts=pd.Series(name="account_id")):
        """
        accounts - pd.DataFrame. Счета клиентов банка.
        account - int. Номер счета текущего дропа.
        outer_accounts - pd.Series. Номера счетов для входящих и исходящих переводов в/из других банков.
        balance - float. Текущий баланс дропа
        batch_txns - int. Счетчик транзакций сделанных в рамках распределения полученной партии денег
        chunk_size - int, float. Последний созданный размер части баланса для перевода по частям.
        used_accounts - pd.Series. Счета на которые дропы уже отправляли деньги.
        """
        self.accounts = accounts
        self.account = account
        self.outer_accounts = outer_accounts
        self.balance = balance
        self.batch_txns = batch_txns
        self.chunk_size = chunk_size
        self.used_accounts = used_accounts

    def update_balance(self, amount, add=False, declined=False):
        """
        Увеличить/уменьшить баланс на указанную сумму
        -------------------
        amount - float, int.
        add - bool. Прибавлять сумму или отнимать.
        declined - bool. Отклонена ли транзакция или одобрена.
        """
        # Не обновлять баланс если транзакция отклонена.
        if declined:
            return
            
        # Увеличить баланс   
        if add:
            self.balance += amount
            return
            
        # Уменьшить баланс    
        self.balance -= amount

    def receive(self, declined, low=5000, high=100000, mean=30000, std=20000, round=500):
        """
        Генерация суммы входящего перевода
        --------------------------
        declined - bool. Отклонена ли транзакция или одобрена
        low - float. Минимальная сумма
        high - float. Максимальная сумма
        mean - float. Средняя сумма
        std - float. Стандартное отклонение
        round - int. Округление целой части. По умолчанию 500. Значит числа будут либо с 500 либо с 000 на конце
                     При условии что round не больше low и high. Чтобы отменить округление, нужно выставить 1
        """

        # Генерация суммы. Округление целой части при необходимости
        amount = get_values_from_truncnorm(low_bound=low, high_bound=high, mean=mean, std=std)[0] // round * round
        
        # Обновляем баланс если транзакция не отклонена
        self.update_balance(amount=amount, add=True, declined=declined)
        
        return amount

    def get_chunk_size(self, online=False, atm_min=10000, atm_share=0.5, round=500, rand_rate=0.9, start=0, stop=0, step=0):
        """
        Вернуть случайный размер суммы перевода для перевода по частям
        либо вернуть долю от баланса для снятия/перевода по частям.
        -------------------------------
        online - bool. Онлайн или оффлайн. Перевод или банкомат. Если банкомат, то снимается доля atm_share от баланса, но не меньше atm_min
        atm_min - int, float. Минимальная сумма снятия дропом в банкомате.
        atm_share - float. Доля от баланса если снятие через банкомат.
        round - int. Округление целой части. По умолчанию 500. 
                     Значит суммы будут округлены до тысяч или пяти сотен
        rand_rate - float. От 0 до 1. Процент случаев, когда каждый НЕ первый чанк будет случайным и не зависеть от предыдущего.
                           Но возможны случайные совпадаения с предыдущим размером чанка.
                           Доля случайных размеров подряд будет:
                           p(c) - вероятность взять определенный размер (зависит от размера выборки чанков)
                           p(rr) - rand_rate
                           p(rr) - (p(rr) * p(c)). Например p(rr) = 0.9, 5 вариантов размеров чанка - p(c) = 0.20
                           0.9 - (0.9 * 0.2) = 0.72
                           В около 72% случаев размеры чанков не будут подряд одинаковыми 
        start - int. Минимальный размер. Прописываем если генерация не через share.
                     То же самое для stop и step
        stop - int. Максимальный размер - не входит в возможный выбор.
                    Максимальное генерируемое значение равно stop - step
        step - int. Шаг размеров.
        --------------------
        Возвращает np.int64
        Результат кэшируется в self.chunk_size
        """
        # Если это не первая транзакция в серии транзакции для одной полученной дропом суммы
        # И случайное число больше rand_rate, то просто возвращаем ранее созданный размер чанка
        if self.batch_txns != 0 and np.random.uniform(0,1) > rand_rate:
            return self.chunk_size

        # Если перевод
        if online:
            sampling_array = np.arange(start, stop, step)
            self.chunk_size = np.random.choice(sampling_array)
            return self.chunk_size
            
        # Если снятие    
        self.chunk_size = max(atm_min, self.balance * atm_share // round * round)
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
            self.update_balance(amount=self.balance, add=False, declined=declined)
            # Прибавляем счетчик транзакции для текущей партии денег
            self.batch_txns += 1
            return amount

        # Иначе считаем сколько частей исходя из размера одной части
        chunks = self.balance // amount

        # Если целое число частей больше 0. Пробуем перевести одну часть
        if chunks > 0:
            self.update_balance(amount=amount, add=False, declined=declined)
            self.batch_txns += 1
            return amount

        # Если баланс меньше одной части. Пробуем перевести то что осталось
        rest = self.balance
        self.update_balance(amount=rest, add=False, declined=declined)
        self.batch_txns += 1
        return rest

    def get_account(self, to_drop):
        """
        Номер счета входящего/исходящего перевода
        to_drop - bool. Перевод другому дропу в нашем банке или нет.
        """
        # Фильтруем accounts исключая свой счет и выбирая дропов. Для случая если to_drop
        drop_accounts = self.accounts.loc[(self.accounts.account_id != self.account) & (self.accounts.is_drop == True)]

        # Если надо отправить другому дропу в нашем банке. При условии что есть другие дропы на текущий момент
        if to_drop and not drop_accounts.empty: 
            account = drop_accounts.account_id.sample(1).iat[0]
            # Добавляем этот счет в использованные как последнюю запись в серии
            self.used_accounts.loc[self.used_accounts.shape[0]] = account
            return account

        # Если отправляем/получаем из другого банка.  
        # Семплируем номер внешнего счета который еще не использовался
        account = self.outer_accounts.loc[~(self.outer_accounts.isin(self.used_accounts))].sample(1).iat[0]
        # Добавляем этот счет в использованные как последнюю запись в серии
        self.used_accounts.loc[self.used_accounts.shape[0]] = account
        
        return account

    def reset_cache(self, balance=True, used_accounts=True, chunk_size=True, batch_txns=True):
        """
        Сброс кэшированных значений
        По умолчанию сбрасывается всё. Если что-то надо оставить, то надо выставить False
        для этого
        -----------------
        balance - bool
        used_accounts - bool
        chunk_size - bool
        batch_txns - bool
        """
        if balance:
            self.balance = 0
        if used_accounts:
            self.used_accounts = pd.Series(name="account_id")
        if chunk_size:
            self.chunk_size = 0
        if batch_txns:
            self.batch_txns = 0

   