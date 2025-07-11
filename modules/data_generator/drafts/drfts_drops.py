# Наброски

# 1. Управление активностью дропа от ее начала до завершения

# ВОЗМОЖНО НА КАНВАСЕ БОЛЕЕ АКТУАЛЬНАЯ ВЕРСИЯ
class DropLifecycleManager:
    """
    Управление активностью дропа от ее начала до завершения
    -------
    ВОЗМОЖНО НА КАНВАСЕ БОЛЕЕ АКТУАЛЬНАЯ ВЕРСИЯ
    """
    def __init__():
        self.Behav = BehavHand
        self.AccHand = AccHand
        self.AmtHand = AmtHand
        self.CreatTxn = CreateTxn
        self.declined = False # МБ переписать в метод property как ГПТ советовал
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


# Возможный метод для DistBehavior
@property
def to_crypto(self):
    """
    """
    # Если не онлайн, то невозможен перевод в крипту
    if not self.online: 
        return
    
    drop_rate = self.to_drop_rate
    # Возвращаем True или False
    return np.random.uniform(0,1) < drop_rate


def switch_online(self, declined):
    """
    """
    if not declined:
        return
    scen = self.scen
    if scen in ["atm", "atm+transfer"]:
        pass

def switch_online(self, prev_declined):
    """
    Направляет выполнение сценария.
    Записывает True или False в self.online с точки зрения какая 
    должна быть транзакция: онлайн или оффлайн (перевод или снятие).
    ------------
    """
    if not prev_declined:
        return
    
    scen = self.scen
    {"method01->method02":0.6,
     "method01->method01":0.4}
    # В atm+transfer только первая транзакция может быть atm(оффлайн)
    if scen == "atm+transfer" and not self.last_online:
        self.online = True
    elif scen == "atm+transfer"and self.last_online:
        self.online = False
    elif scen == "atm":
        self.online = True
    elif scen in ["split_transfer", "transfer"]:
        self.online = True


min_drops = dist_configs.to_drops["min_drops"]
print(min_drops)
drop_acc_hand1.accounts["is_drop"] = False
drop_acc_hand1.get_account(own=True)

accs_samp = drop_acc_hand1.accounts.query("client_id != @own_id").client_id.sample(n=min_drops - 1)
drop_acc_hand1.accounts.loc[drop_acc_hand1.accounts.client_id.isin(accs_samp), "is_drop"] = True
drop_acc_hand1.accounts.query("client_id != @own_id and is_drop == True").shape[0]
    