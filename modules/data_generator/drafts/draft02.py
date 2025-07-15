class DropLifecycleManager:
    def __init__(self):
        self.Behav = BehavHand
        self.AccHand = AccHand
        self.AmtHand = AmtHand
        self.CreatTxn = CreateTxn
        self.crypto_rate = configs.crypto_rate
        self.to_crypto = None
        self.declined = False # МБ переписать в метод property как ГПТ советовал
        self.alltxns = []


    def reset_all_caches():
        """
        Сброс кэшей когда активность дропа закончена совсем
        """
        self.declined = False
        self.BehavHand.reset_cache(all=True)
        self.TimeHand.reset_cache()
        self.CreatTxn.reset_cache()
        self.CreatTxn.PartData.reset_cache()

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
        # создать счет дропа, записать is_drop = True
        AccHand.get_acc(own) # получить номер счета дропа. Пишется в атрибут AccHand.account
        AccHand.label_drop() # помечаем клиента как дропа в таблице accounts
        
        batch_hand = self.batch_hand
        dist = self.get_dist
        while True:
            declined = self.declined # статус транзакции. будет ли она отклонена
            receive_txn = CreateTxn.trf_or_atm(dist=dist, receive=True) # входящая транзакция. Новый батч денег.
            all_txns = self.all_txns.append(receive_txn)
            all_txns.append(receive_txn)
            # если у дропа достигнут лимит то транзакции отклоняются. 
            # Если входящая отклонена, дропу больше не пытаются послать деньги
            if declined: 
                break

            behav_hand.sample_scenario() # выбрать сценарий
            behav_hand.in_chunks_val() # транзакции по частям или нет

            batch_hand.process_batch(dist=dist) # обработка полученного батча

            txns_fm_batch = self.txns_fm_batch
            all_txns.extend(txns_fm_batch)
            # сброс кэша после завершения обработки батча
            batch_hand.reset_cache(all=False)
            
        self.reset_all_caches() # сброс всего кэша после завершения активности дропа
  
while True:
    declined = batch_hand.declined
    receive_txn3 = cr_drop_txn1.trf_or_atm(dist=False, receive=True, to_drop=False, declined=declined)
    all_txns3.append(receive_txn3)
    if declined:
        break
        
    behav_hand1.sample_scenario() # выбрать сценарий
    behav_hand1.in_chunks_val() # транзакции по частям или нет 

    batch_hand.process_batch(dist=False)
    txns_fm_batch3 = batch_hand.txns_fm_batch
    all_txns3.extend(txns_fm_batch3)
    batch_hand.reset_cache(all=False)