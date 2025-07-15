# Обработка партии (батча) полученных денег

from data_generator.fraud.drops.build.builder import DropBaseClasses
from data_generator.fraud.drops.txns import CreateDropTxn

class DropBatchProcessor:
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

    def is_declined(self):
        """
        Проверка будет ли отклонена транзакция.
        Возвращает True или False в зависимости от достижения лимитов.
        Также записывает это значение в self.declined
        """
        self.declined = self.create_txn.limit_reached()
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

        if all:
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
        behav_hand.sample_scen() # выбрать сценарий
        behav_hand.in_chunks_val() # транзакции по частям или нет
    
        while amt_hand.balance > 0:
            declined = self.is_declined() # будет ли отклонена транзакция
            behav_hand.attempts_after_decline()
            behav_hand.guide_scenario()

            if behav_hand.to_crypto: # перевод на криптобиржу или нет
                txn_out = create_txn.purchase(declined=declined, dist=True)
            else: # Иначе перевод/снятие
                to_drop = behav_hand.to_drop # Пробовать перевести другому дропу.
                txn_out = create_txn.trf_or_atm(receive=False, 
                                    to_drop=to_drop, declined=declined)
            # Добавляем в список транз-ций батча   
            self.txns_fm_batch.append(txn_out)

            # Если это не первая отклоненная транзакция, то вычитаем попытку
            # совершить транзакцию после отклонения
            behav_hand.deduct_attempts()
            
            # Решение об остановке после отклоненной транзакции
            if behav_hand.stop_after_decline(declined=declined):
                break

        # Работа с батчем закончена. Частично сбрасываем кэш
        self.reset_cache(self, all=False)
            

    def purchaser(self):
        """
        Обработка партии(батча) денег полученных дропом
        покупателем.
        """
        behav_hand = self.behav_hand
        amt_hand = self.amt_hand
        create_txn = self.create_txn
        behav_hand.sample_scen() # выбрать сценарий
        behav_hand.in_chunks_val() # транзакции по частям или нет
        
        while amt_hand.balance > 0:
            declined = self.is_declined() # будет ли отклонена транзакция
            behav_hand.attempts_after_decline()

            txn_out = create_txn.purchase(declined=declined, dist=False)
            self.txns_fm_batch.append(txn_out)
            
            # Если это не первая отклоненная транзакция, то вычитаем попытку
            # совершить транзакцию после отклонения
            behav_hand.deduct_attempts()
            
            # Решение об остановке после отклоненной транзакции
            if behav_hand.stop_after_decline(declined=declined):
                break

        # Работа с батчем закончена. Частично сбрасываем кэш
        self.reset_cache(self, all=False)


    def process_batch(self, drop_type):
        """
        Вызов соответствующего типу дропа метода для обработки
        батча денег.
        ---------
        drop_type: str. Тип дропа. 'distributor' или 'purchaser'
        """
        if drop_type == "distributor":
            self.distributor()

        elif drop_type == "purchaser":
            self.purchaser()