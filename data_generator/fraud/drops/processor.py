# Обработка партии (батча) полученных денег

from data_generator.fraud.drops.build.builder import DropBaseClasses
from data_generator.fraud.drops.txns import CreateDropTxn

class DropBatchHandler:
    """
    Обработка полученной дропом партии (батча) денег
    ---------------------------
    drop_type: str. 'distributor' или 'purchaser'
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
        self.drop_type = base.drop_type
        self.amt_hand = base.amt_hand
        self.behav_hand = base.behav_hand
        self.create_txn = create_txn
        self.declined = False
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
        amt_hand.reset_cache(all=all)
        behav_hand.reset_cache(all=all)

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
                txn_out = create_txn.purchase(declined=declined)
            else: # Иначе перевод/снятие
                to_drop = behav_hand.to_drop # Пробовать ли перевести другому дропу.
                txn_out = create_txn.trf_or_atm(receive=False, 
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

            txn_out = create_txn.purchase(declined=declined)
            self.txns_fm_batch.append(txn_out)
            
            # Сколько попыток будет после первой откл. транз-ции
            behav_hand.attempts_after_decline()

            # Если это не первая отклоненная транзакция, то вычитаем попытку
            # совершить транзакцию после отклонения
            behav_hand.deduct_attempts()
            
            # Решение об остановке после отклоненной транзакции
            if behav_hand.stop_after_decline():
                break


    def process_batch(self):
        """
        Вызов соответствующего типу дропа метода для обработки
        батча денег.
        Метод выбирается исходя из self.drop_type.
        ---------
        """
        drop_type = self.drop_type

        if drop_type == "distributor":
            self.distributor()
        elif drop_type == "purchaser":
            self.purchaser()