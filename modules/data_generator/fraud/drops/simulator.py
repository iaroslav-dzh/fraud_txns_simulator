# Полный жизненный цикл дропа

from data_generator.fraud.drops.build.builder import DropBaseClasses
from data_generator.fraud.drops.txns import CreateDropTxn
from data_generator.fraud.drops.processor import DropBatchHandler

class DropLifecycleManager:
    """
    Полный жизненный цикл дропа.
    """
    def __init__(self, base: DropBaseClasses, create_txn: CreateDropTxn):
        self.drop_type = base.drop_type
        self.acc_hand = base.acc_hand
        self.amt_hand = base.amt_hand
        self.time_hand = base.time_hand
        self.part_data = base.part_data
        self.behav_hand = base.behav_hand
        self.create_txn = create_txn
        self.batch_hand = DropBatchHandler(base=base, create_txn=create_txn)
        self.drop_txns = []


    def reset_all_caches(self):
        """
        Сброс кэшей когда активность дропа закончена совсем
        """
        # Сброс всего кэша batch_hand включает в себя полный сброс кэша
        # в behav_hand и amt_hand
        self.batch_hand.reset_cache(self, all=True)
        self.time_hand.reset_cache()
        self.part_data.reset_cache()
        self.create_txn.reset_cache()
        

    @property 
    def get_dist(self):
        """
        получить булево значение типа дропа
        distributor - True.
        purchaser - False.
        """
        drop_type = self.drop_type

        if drop_type == "distributor":
            return True
        elif drop_type == "purchaser":
            return False

    def run_drop_lifecycle(self):
        # создать счет дропа, записать is_drop = True в таблице acc_hand.accounts
        acc_hand = self.acc_hand
        # получить номер счета дропа. Пишется в атрибут acc_hand.account
        acc_hand.get_account(own=True) 
        acc_hand.label_drop() # помечаем клиента как дропа в таблице acc_hand.accounts
        
        behav_hand = self.behav_hand
        batch_hand = self.batch_hand
        create_txn = self.create_txn
        dist = self.get_dist # флаг явлется ли дроп распределителем

        while True:
            declined = batch_hand.declined # статус транзакции. будет ли она отклонена
            # входящая транзакция. Новый батч денег.
            receive_txn = create_txn.trf_or_atm(dist=dist, to_drop=False, receive=True) 
            drop_txns = self.drop_txns
            drop_txns.append(receive_txn)
            # если у дропа достигнут лимит то транзакции отклоняются. 
            # Если входящая отклонена, дропу больше не пытаются послать деньги
            if declined: 
                break

            behav_hand.sample_scenario() # выбрать сценарий
            behav_hand.in_chunks_val() # транзакции по частям или нет

            batch_hand.process_batch(dist=dist) # обработка полученного батча

            txns_fm_batch = self.txns_fm_batch
            drop_txns.extend(txns_fm_batch)
            # сброс кэша после завершения обработки батча
            batch_hand.reset_cache(all=False)
            
        self.reset_all_caches() # сброс всего кэша после завершения активности дропа
  