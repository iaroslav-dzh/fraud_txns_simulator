# Наброски

class DropBatchProcessor:
    def __init__(self, base, create_txn):
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

        # Работа с батчем закончена. Сбрасываем кэш.
        behav_hand.reset_cache(all=False)
        amt_hand.reset_cache(life_end=False)
            

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

        # Работа с батчем закончена. Сбрасываем кэш, кроме попыток.
        behav_hand.reset_cache()
            





































































# 000000000000. Управление активностью дропа от ее начала до завершения

# ВОЗМОЖНО НА КАНВАСЕ БОЛЕЕ АКТУАЛЬНАЯ ВЕРСИЯ
# class DropLifecycleManager:
#     """
#     Управление активностью дропа от ее начала до завершения
#     -------
#     ВОЗМОЖНО НА КАНВАСЕ БОЛЕЕ АКТУАЛЬНАЯ ВЕРСИЯ
#     """
#     def __init__():
#         self.Behav = BehavHand
#         self.AccHand = AccHand
#         self.AmtHand = AmtHand
#         self.CreatTxn = CreateTxn
#         self.declined = False # МБ переписать в метод property как ГПТ советовал
#         self.alltxns = []
    
#     def process_batch():
#         BehavHand.sample_scen() # выбрать сценарий
#         BehavHand.in_chunks_val() # транзакции по частям или нет
    
#         while AmtHand.balance > 0:
#             if online and np.random.uniform(0, 1) < crypto_rate:
#                 part_out = CreateTxn.purchase(declined=declined)
#             else:
#                 part_out = CreateTxn.trf_or_atm(receive=False,
#                                             declined=declined)
#             all_txns5.append(part_out)
#             Behav.deduct_attempts(declined=declined, receive=False)
            
#     # Решение об остановке после первой отклоненной транзакции
#             if BehavHand.stop_after_decline(declined=declined):
#                 break
#             # Лимит достигнут: следующая транзакция будет отклонена     
#             self.declined = CreateTxn.limit_reached()
    
#     def reset_cache():
#         """
#         Сброс кэшей когда активность дропа закончена совсем
#         """
#         self.declined = False
#         self.BehavHand.reset_cache()
#         self.TimeHand.reset_cache()
#         self.CreatTxn.reset_cache()
#         self.CreatTxn.PartData.reset_cache()
        
#     def run_drop_lifecycle():
#         # создать счет дропа, записать is_drop = True
#         AccHand.get_acc(own) # получить номер счета дропа. Пишется в атрибут AccHand.account
#         AccHand.label_drop() # помечаем клиента как дропа в таблице accounts
    
#         while True:
#             decline = self.decline # статус транзакции. будет ли она отклонена
#             receive_txn = CreateTxn.one_txn(receive=True) # входящая транзакция. Новый батч денег.
#             self.alltxns.append(receive_txn)
#             # если у дропа достингут лимит то транзакции отклоняются. 
#             # Если входящая отклонена, дропу больше не пытаются послать деньги
#             if decline: 
#                 break
            
#             # обработка полученного батча
#             self.process_batch() 
            
#             # сброс кэша после завершения обработки батча
#             self.AmtHand.reset_cache(chunk_size=True, batch_txns=True)
#             self.BehavHand.reset_cache(all=False)
            
#         self.reset_cache() # сброс всего кэша после завершения активности дропа
