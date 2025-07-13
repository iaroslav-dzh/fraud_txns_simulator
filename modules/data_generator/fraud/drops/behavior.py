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
    to_drop_rate: float. Доля исходящих транзакций другим дропам.
    online: bool. Какая должна быть транзакция: онлайн или оффлайн (перевод или снятие).
    in_chunks: bool. Распределяет ли дроп деньги по частям. По умолчанию None.
    attempts: int. Сколько попыток совершить операцию будет сделано 
               дропом после первой отклоненной транзакции. По умолчанию 0.
    attempts_cfg: dict. Лимиты возможных попыток: для переводов и снятий.
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
        self.to_drop_rate = configs.to_drops["rate"]
        self.crypto_rate = configs.crypto_rate
        self.online = None
        self.in_chunks = None
        self.attempts = 0
        self.attempts_cfg = configs.attempts


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
        # Если ни одно из условий не сработало
        self.scen = "transfer"


    def in_chunks_val(self):
        """
        Запись значения атрибута in_chunks в зависимости от
        сценария.
        """
        scen = self.scen

        if scen in ["transfer", "atm"]:
            self.in_chunks = False
            return
        if scen in ["atm+transfer", "split_transfer"]:
            self.in_chunks = True


    def guide_scenario(self):
        """
        Направляет выполнение сценария.
        Записывает True или False в self.online с точки зрения какая 
        должна быть транзакция: онлайн или оффлайн (перевод или снятие).
        ------------
        """
        scen = self.scen
        batch_txns = self.amt_hand.batch_txns

        # В atm+transfer только первая транзакция может быть atm(оффлайн)
        if scen == "atm+transfer" and batch_txns == 0:
            self.online = False
        elif scen == "atm+transfer":
            self.online = True
        elif scen == "atm":
            self.online = False
        elif scen in ["split_transfer", "transfer"]:
            self.online = True


    @property
    def to_drop(self):
        """
        Случайно определить будет ли транзакция
        другому дропу
        """
        if not self.online: # Если текущая транз-ция не онлайн
            return False
        
        drop_rate = self.to_drop_rate
        # Возвращаем True или False
        return np.random.uniform(0,1) < drop_rate


    @property
    def to_crypto(self):
        """
        Случайно определить будет ли онлайн
        перевод на криптобиржу
        """
        # Если не онлайн, то невозможен перевод в крипту
        if not self.online: 
            return False
        
        to_crypto_rate = self.crypto_rate
        # Возвращаем True или False
        return np.random.uniform(0,1) < to_crypto_rate


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

            
    def attempts_after_decline(self, declined):
        """
        Рандомизация количества попыток дропа совершить операцию после первой
        отклоненной транзакции.
        Зависит от self.online. Для онлайна и оффлайна можно ставить свои
        границы попыток.
        ---------------
        declined: отклоняется ли текущая транзакция.
        """
        if not declined:
            return
        
        online = self.online

        if online: # Для переводов
            trf_min = self.attempts_cfg["trf_min"]
            trf_max = self.attempts_cfg["trf_max"]
            self.attempts = np.random.randint(trf_min, trf_max + 1)
            return
        # Для снятий.
        atm_min = self.attempts_cfg["atm_min"]
        atm_max = self.attempts_cfg["atm_max"]
        self.attempts = np.random.randint(atm_min, atm_max + 1)

            
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


    def reset_cache(self, all=False):
        """
        Сброс кэшированных данных
        -------------
        all: bool. Если False, не будут сброшены self.attempts
        """
        self.scen = None
        self.online = None
        self.in_chunks = None
        if not all:
            return
        self.attempts = 0


# 2. Управление поведением дропа покупателя

class PurchBehaviorHandler:
    """
    Управление поведением дропа покупателя
    """
    def __init__(self):
        pass
