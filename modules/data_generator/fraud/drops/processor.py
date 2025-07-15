# Обработка партии (батча) полученных денег

from data_generator.fraud.drops.build.builder import DropBaseClasses
from data_generator.fraud.drops.txns import CreateDropTxn

class DropBatchHandler:
    """
    amt_hand: DropAmountHandler. Генератор сумм входящих/исходящих транзакций, сумм снятий.
              Управление балансом текущего дропа.
    behav_hand: DistBehaviorHandler | PurchBehaviorHandler.
                Управление поведением дропа: распределителя или покупателя.
    create_txn: CreateDropTxn. Создание транзакций.
    declined: bool. По умолчанию False. Отклоняются ли транзакции.
    txns_fm_batch: list. Транзакции дропа в текущем батче.
    """

    def __init__(self, base: DropBaseClasses, create_txn: CreateDropTxn):
        """
        base: DropBaseClasses. Объекты основных классов для дропов.
        create_txn: CreateDropTxn. Создание транзакций.
        """
        # self.acc_hand = base.acc_hand - наверное это в lifecycle нужно
        self.amt_hand = base.amt_hand
        # self.time_hand = base.time_hand - это тут не нужно
        self.behav_hand = base.behav_hand
        # self.part_data = base.part_data - это тут не нужно
        self.create_txn = create_txn
        self.declined = False # МБ переписать в метод property как ГПТ советовал
        self.txns_fm_batch = []

    def should_decline(self):
        """
        Проверка будет ли отклонена транзакция.
        Возвращает True или False в зависимости от достижения лимитов.
        Также записывает это значение в self.declined
        """
        self.declined = self.create_txn.limit_reached()
        # print("is_declined", self.declined)
        return self.declined

    def reset_cache(self, all=False):
        """
        Сброс кэша.
        --------
        all: bool. Если True то сбрасывает атрибуты: txns_fm_batch, declined.
             Если False то declined не сбрсывает
             Также передается в методы классов:
             DistBehaviorHandler | PurchBehaviorHandler,
             DropAmountHandler.
        """
        behav_hand = self.behav_hand
        amt_hand = self.amt_hand

        self.txns_fm_batch = []
        behav_hand.reset_cache(all=all)
        amt_hand.reset_cache(all=all)

        if not all:
            return
        
        self.declined = False


    def distributor(self):
        """
        Обработка партии(батча) денег полученных дропом
        распределителем.
        """
        behav_hand = self.behav_hand
        amt_hand = self.amt_hand
        create_txn = self.create_txn
        
    
        while amt_hand.balance > 0:
            declined = self.should_decline() # будет ли отклонена транзакция
            behav_hand.guide_scenario()

            if behav_hand.to_crypto: # перевод на криптобиржу или нет
                txn_out = create_txn.purchase(declined=declined, dist=True)
            else: # Иначе перевод/снятие
                to_drop = behav_hand.to_drop # Пробовать ли перевести другому дропу.
                txn_out = create_txn.trf_or_atm(dist=True, receive=False, 
                                    to_drop=to_drop, declined=declined)
            # Добавляем в список транз-ций батча   
            self.txns_fm_batch.append(txn_out)

            # Сколько попыток будет после первой откл. транз-ции
            behav_hand.attempts_after_decline()

            # Если это не первая отклоненная транзакция, то вычитаем попытку
            # совершить транзакцию после отклонения
            behav_hand.deduct_attempts()
            
            # Решение об остановке после отклоненной транзакции
            if behav_hand.stop_after_decline():
                break
            

    def purchaser(self):
        """
        Обработка партии(батча) денег полученных дропом
        покупателем.
        """
        behav_hand = self.behav_hand
        amt_hand = self.amt_hand
        create_txn = self.create_txn
        
        while amt_hand.balance > 0:
            declined = self.should_decline() # будет ли отклонена транзакция

            txn_out = create_txn.purchase(declined=declined, dist=False)
            self.txns_fm_batch.append(txn_out)
            
            # Сколько попыток будет после первой откл. транз-ции
            behav_hand.attempts_after_decline()

            # Если это не первая отклоненная транзакция, то вычитаем попытку
            # совершить транзакцию после отклонения
            behav_hand.deduct_attempts()
            
            # Решение об остановке после отклоненной транзакции
            if behav_hand.stop_after_decline():
                break


    def process_batch(self, dist):
        """
        Вызов соответствующего типу дропа метода для обработки
        батча денег.
        ---------
        dist: bool. True - distributor
              False - purchaser'
        """
        if dist:
            self.distributor()
        else:
            self.purchaser()