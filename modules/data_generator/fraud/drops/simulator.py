# Полный жизненный цикл дропа
import pandas as pd
from data_generator.fraud.drops.build.builder import DropBaseClasses
from data_generator.fraud.drops.txns import CreateDropTxn
from data_generator.fraud.drops.processor import DropBatchHandler
from data_generator.utils import create_progress_bar

class DropLifecycleManager:
    """
    Управление полным жизненный циклом одного дропа.
    ------------------
    drop_type: str. 'distributor' или 'purchaser'
    acc_hand: DropAccountHandler. Генератор номеров счетов входящих/исходящих транзакций.
              Учет использованных счетов.
    amt_hand: DropAmountHandler. Генератор сумм входящих/исходящих транзакций, сумм снятий.
              Управление балансом текущего дропа.
    time_hand: DropTimeHandler.
               Управление временем транзакций дропа.
    behav_hand: DistBehaviorHandler | PurchBehaviorHandler.
                Управление поведением дропа: распределителя или покупателя.
    part_data: DropTxnPartData.
               Генерация части данных о транзакции дропа.
    behav_hand: DistBehaviorHandler | PurchBehaviorHandler.
                Управление поведением дропа: распределителя или покупателя.
    create_txn: CreateDropTxn. Создание транзакций.
    batch_hand: DropBatchHandler. Обработка полученной дропом партии (батча) денег
    drop_txns: list. Созданные транзакции дропа.
    """
    def __init__(self, base: DropBaseClasses, create_txn: CreateDropTxn):
        """
        base: DropBaseClasses. Объекты основных классов для дропов.
        create_txn: CreateDropTxn. Создание транзакций.
        """
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
        self.batch_hand.reset_cache(all=True)
        self.time_hand.reset_cache()
        self.part_data.reset_cache()
        self.create_txn.reset_cache()
        self.drop_txns = []
        

    def run_drop_lifecycle(self):
        # создать счет дропа, записать is_drop = True в таблице acc_hand.accounts
        acc_hand = self.acc_hand
        # получить номер счета дропа. Пишется в атрибут acc_hand.account
        acc_hand.get_account(own=True) 
        acc_hand.label_drop() # помечаем клиента как дропа в таблице acc_hand.accounts
        
        behav_hand = self.behav_hand
        batch_hand = self.batch_hand
        create_txn = self.create_txn

        while True:
            declined = batch_hand.declined # статус транзакции. будет ли она отклонена
            # входящая транзакция. Новый батч денег.
            receive_txn = create_txn.trf_or_atm(declined=declined, \
                                                to_drop=False, receive=True) 
            drop_txns = self.drop_txns
            drop_txns.append(receive_txn)
            # если у дропа достигнут лимит то транзакции отклоняются. 
            # Если входящая отклонена, дропу больше не пытаются послать деньги
            if declined: 
                break

            behav_hand.sample_scenario() # выбрать сценарий
            behav_hand.in_chunks_val() # транзакции по частям или нет

            batch_hand.process_batch() # обработка полученного батча

            txns_fm_batch = batch_hand.txns_fm_batch
            drop_txns.extend(txns_fm_batch)
            # сброс кэша после завершения обработки батча
            batch_hand.reset_cache(all=False)
            
        # Кэш надо сбрасывать вне метода. После его исполнения. Т.к.
        # надо же транзакции записать во внешний список


# 2. Полная симуляция дропов
  
class DropSimulator:
    """
    Генерация активности множества дропов.
    ------------------------
    drop_clients: pd.DataFrame. Клиенты которые будут дропами.
    part_data: DropTxnPartData.
            Генерация части данных о транзакции дропа.
    acc_hand: DropAccountHandler. Генератор номеров счетов входящих/исходящих транзакций.
            Учет использованных счетов.
    life_manager: DropLifecycleManager. Управление полным жизненный циклом одного дропа.
    """
    def __init__(self, base_cfg, configs, base, create_txn):
        """
        base_cfg: dict. Конфиги из base.yaml
        configs: DropDistributorCfg | DropPurchaserCfg.
                 Конфиги и данные для создания дроп транзакций.
        base: Объекты основных классов для дропов. 
        create_txn: CreateDropTxn. Создание транзакций.
        """
        self.base_cfg = base_cfg
        self.drop_type = base.drop_type
        self.drop_clients = configs.clients
        self.part_data = base.part_data
        self.acc_hand = base.acc_hand
        self.life_manager = DropLifecycleManager(base=base, create_txn=create_txn)
        self.all_txns = []
    
    def write_to_file(self, data, category, file_key):
        """
        Запись данных в файл одного из
        """
        path = self.base_cfg["data_paths"][category][file_key]
        file_type = path.split(".")[-1]

        if file_type == "csv":
            return data.to_csv(path, index=False)
        
        if file_type == "gpkg":
            return data.to_file(path, layer="layer_name", driver="GPKG")
        
        if file_type == "parquet":
            return data.to_parquet(path, engine="pyarrow")


    def run(self):
        drop_clients = self.drop_clients
        progress_bar = create_progress_bar(drop_clients)
        part_data = self.part_data
        acc_hand = self.acc_hand
        life_manager = self.life_manager
        all_txns = self.all_txns

        for client in drop_clients.itertuples():
            part_data.client_info = client
            acc_hand.client_id = client.client_id

            life_manager.run_drop_lifecycle()
            drop_txns = life_manager.drop_txns
            all_txns.extend(drop_txns)

            life_manager.reset_all_caches()
            progress_bar.update(1)
        
        accounts = acc_hand.accounts
        self.write_to_file(data=accounts, category="generated_data", \
                           file_key="accounts")
        
        all_txns_df = pd.DataFrame(self.all_txns)
        if self.drop_type == "distributor":
            self.write_to_file(data=all_txns_df, category="generated_data", \
                           file_key="dist_drop_txns")
            
        elif self.drop_type == "purchaser":
            self.write_to_file(data=all_txns_df, category="generated_data", \
                           file_key="purch_drop_txns")
        