# Инструменты создания дропов распределителей (distributors)


import pandas as pd
import numpy as np

from data_generator.configs import DropDistributorCfg
from data_generator.fraud.drops.base import DropAmountHandler

# .

class DistBehaviorHandler:
    """
    Управление поведением дропа распределителя. Выбор сценария.
    ----------
    Атрибуты:
    --------
    scen: str. Выбранный сценарий поведения. По умолчанию None.
    amt_hand: DropAmountHandler. Управление балансом и суммами переводов.
    atm_min: int. Минимальная сумма для снятия.
    trf_min: int. Минимальная сумма перевода.
    trf_max: int. Максимальная сумма перевода.
    split_rate: float. Доля случаев когда полученная сумма будет распределена по частям.
                При условии что полученная сумма пройдет по лимитам.
    online: bool. Какая должна быть транзакция: онлайн или оффлайн (перевод или снятие).
    in_chunks: bool. Распределяет ли дроп деньги по частям. По умолчанию None.
    attempts: int. Сколько попыток совершить операцию будет сделано 
               дропом после первой отклоненной транзакции. По умолчанию 0.
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
        self.amt_hand = amt_hand
        self.atm_min = amt_hand.chunks["atm_min"]
        self.trf_min = amt_hand.chunks["rcvd_small"]["min"]
        self.trf_max = configs.trf_max
        self.split_rate = configs.split_rate
        self.online = None
        self.in_chunks = None
        self.attempts = 0
        self.low = configs.attempts["low"]
        self.high = configs.attempts["high"]


    def sample_scenario(self):
        """
        Выбор сценария поведения дропа с учётом текущего баланса и актуальных лимитов.
        """
        balance = self.amt_hand.balance
        split = np.random.uniform(0,1) <= self.split_rate

        # Минимальный баланс для переводов по частям будет самый маленький возможный размер чанка
        # self.trf_min умноженный на 2
        large_balance = balance > self.trf_max
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
                return

        # Если ни одно из условий не сработало — fallback
        self.scen = "transfer"


    def in_chunks_val(self):
        """
        Запись значения атрибута in_chunks в зависимости от
        сценария.
        """
        if self.scen in ["transfer", "atm"]:
            self.in_chunks = False
            return
        if self.scen in ["atm+transfer", "split_transfer"]:
            self.in_chunks = True
        

    def guide_scenario(self, first_txn):
        """
        Направляет выполнение сценария.
        Записывает True или False в self.online с точки зрения какая 
        должна быть транзакция: онлайн или оффлайн (перевод или снятие).
        ------------
        first_txn: bool. Является ли транзакция первой исходящей в текущей партии.
        """
        scen = self.scen

        # В atm+transfer только первая транзакция может быть atm(оффлайн)
        if scen == "atm+transfer" and first_txn:
            self.online = False
        elif scen == "atm+transfer":
            self.online = True
        elif scen == "atm":
            self.online = False
        elif scen in ["split_transfer", "transfer"]:
            self.online = True


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
            
        
    def deduct_attempts(self, declined, receive=False):
        """
        Вычитание попытки исходящей операции совершенной при статусе declined
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
        self.scen = None
        self.online = None
        self.in_chunks = None
        self.attempts = 0
