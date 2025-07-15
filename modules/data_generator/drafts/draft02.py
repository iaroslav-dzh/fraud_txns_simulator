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

  
    def process_batch():
        BehavHand.sample_scen() # выбрать сценарий
        BehavHand.in_chunks_val() # транзакции по частям или нет
        
        while AmtHand.balance > 0:
            declined = self.declined
            Behav.guide_scenario() # тут надо вызвать а не в CreateTxn.trf_or_atm
            Behav.atmts_aft_decline(declined=declined) # ? здесь?
            to_drop = Behav.to_drop
            if Behav.to_crypto: # property метод
                part_out = CreateTxn.purchase(declined=declined, dist=True)
            else:
                part_out = CreateTxn.trf_or_atm(receive=False, to_drop=to_drop,
                                            declined=declined)
            all_txns5.append(part_out)
            
            # Решение об остановке после первой отклоненной транзакции
            if BehavHand.stop_after_decline(declined=declined):
                break
            # Если например 1 попытка на счетчике, то вычитаем. 
            # В следующей итерации будет сделана еще одна транзакция
            # и после нее цикл будет прерван
            Behav.deduct_attempts(declined=declined, receive=False)
            Behav.
            # Лимит достигнут - следующая транзакция будет отклонена
            self.declined = CreateTxn.limit_reached() # мб сделать property self.declined
  
    def reset_caches():
        """
        Сброс кэшей когда активность дропа закончена совсем
        """
        self.declined = False
        self.BehavHand.reset_cache(all=True)
        self.TimeHand.reset_cache()
        self.CreatTxn.reset_cache()
        self.CreatTxn.PartData.reset_cache()
      
    def run_drop_lifecycle(self):
        # создать счет дропа, записать is_drop = True
        AccHand.get_acc(own) # получить номер счета дропа. Пишется в атрибут AccHand.account
        AccHand.label_drop() # помечаем клиента как дропа в таблице accounts
        drop_type = self.drop_type

        while True:
            declined = self.declined # статус транзакции. будет ли она отклонена
            receive_txn = CreateTxn.trf_or_atm(receive=True) # входящая транзакция. Новый батч денег.
            self.alltxns.append(receive_txn)
            # если у дропа достигнут лимит то транзакции отклоняются. 
            # Если входящая отклонена, дропу больше не пытаются послать деньги
            if declined: 
                break
            
            self.process_batch(drop_type=drop_type) # обработка полученного батча
            
            # сброс кэша после завершения обработки батча
            AmtHand.reset_cache(chunk_size=True, batch_txns=True)
            # сброс scen, online, in_chunks, batch_txns 
            Behav.reset_cache(all=False)
            
            self.reset_caches() # сброс всего кэша после завершения активности дропа
  