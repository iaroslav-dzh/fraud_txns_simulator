# Наброски

# 1. Управление активностью дропа от ее начала до завершения
class DropLifecycleManager:
    """
    Управление активностью дропа от ее начала до завершения
    """
    def __init__():
        self.Behav = BehavHand
        self.AccHand = AccHand
        self.AmtHand = AmtHand
        self.CreatTxn = CreateTxn
        self.declined = False
        self.alltxns = []
    
    def process_batch():
        BehavHand.sample_scen() # выбрать сценарий
        BehavHand.in_chunks_val() # транзакции по частям или нет
    
        while AmtHand.balance > 0:
            if online and np.random.uniform(0, 1) < crypto_rate:
                part_out = CreateTxn.purchase(declined=declined)
            else:
                part_out = CreateTxn.trf_or_atm(receive=False,
                                            declined=declined)
                    
            all_txns5.append(part_out)
            Behav.deduct_attempts(declined=declined, receive=False)
            
    # Решение об остановке после первой отклоненной транзакции
            if BehavHand.stop_after_decline(declined=declined):
                break
            # Лимит достигнут - следующая транзакция будет отклонена     
            self.declined = CreateTxn.limit_reached()
    
    def reset_cache():
        """
        Сброс кэшей когда активность дропа закончена совсем
        """
        self.declined = False
        self.BehavHand.reset_cache()
        self.TimeHand.reset_cache()
        self.CreatTxn.reset_cache()
        self.CreatTxn.PartData.reset_cache()
        
    def run_drop_lifecycle():
        # создать счет дропа, записать is_drop = True
        AccHand.get_acc(own) # получить номер счета дропа. Пишется в атрибут AccHand.account
        AccHand.label_drop() # помечаем клиента как дропа в таблице accounts
    
        while True:
            decline = self.decline # статус транзакции. будет ли она отклонена
            receive_txn = CreateTxn.one_txn(receive=True) # входящая транзакция. Новый батч денег.
            self.alltxns.append(receive_txn)
            # если у дропа достингут лимит то транзакции отклоняются. 
            # Если входящая отклонена, дропу больше не пытаются послать деньги
            if decline: 
                break
            
            # обработка полученного батча
            self.process_batch() 
            
            # сброс кэша после завершения обработки батча
            self.AmtHand.reset_cache(chunk_size=True, batch_txns=True)
            self.BehavHand.reset_cache(all=False)
            
        self.reset_cache() # сброс всего кэша после завершения активности дропа
    