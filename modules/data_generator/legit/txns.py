# Функции генерации легальных транзакций
import pandas as pd
import numpy as np

from data_generator.utils import build_transaction, gen_trans_number_norm, create_progress_bar
from data_generator.legit.txndata import get_txn_location_and_merchant
from data_generator.legit.time.time import get_legit_txn_time


# 1.

def generate_one_legit_txn(client_info, client_trans_df, client_device_ids, category, \
                           merchants_df, configs):
    """
    Генерация одной легальной транзакции покупки для клиента.
    ------------------------------------------------
    client_info: namedtuple, полученная в результате итерации с помощью
                 .itertuples() через датафрейм с информацией о клиентах.
    client_trans_df: pd.DataFrame. Транзакции клиента.
    client_device_ids: pd.Series. id девайсов клиента.
    category: pd.DataFrame. Одна запись с категорией и её характеристиками.
    merchants_df: pd.DataFrame. Оффлайн мерчанты заранее отфильтрованные по
                  городу клиента т.к. это легальные транзакции.
    configs: LegitCfg. Конфиги и данные для генерации легальных транзакций.  
    """
    all_time_weights = configs.all_time_weights

    client_id = client_info.client_id
    
    category_name = category["category"].iloc[0]
    round_clock = category["round_clock"].iloc[0]
    online = category["online"].iloc[0]
    # средняя сумма для этой категории
    amt_mean = category["avg_amt"].iloc[0]
    # стандартное отклонение сумм для этой категории
    amt_std = category["amt_std"].iloc[0]
    
    # случайно сгенерированная сумма транзакции, но не менее 1
    amount = max(1, np.random.normal(amt_mean, amt_std))

    # 1. Offline_24h_Legit - круглосуточные оффлайн покупки
    if not online and round_clock:
        weights_key = "Offline_24h_Legit"
        channel = "POS"
        device_id = np.nan
        
    # 2. Online_Legit - Онлайн покупки
    elif online:
        weights_key = "Online_Legit"
        # локация клиента по IP. Т.к. это не фрод. Просто записываем координаты города клиента
        channel = "ecom"
        device_id = client_device_ids.sample(n=1).iloc[0]
        
    # 3. Offline_Day_Legit - Оффлайн покупки. Дневные категории.
    elif not online and not round_clock:
        weights_key = "Offline_Day_Legit"
        channel = "POS"
        device_id = np.nan
        
    # Генерация мерчанта, координат транзакции. И если это онлайн, то IP адреса с которого сделана транзакция
    merchant_id, trans_lat, trans_lon, trans_ip, trans_city = \
                                get_txn_location_and_merchant(online=online, merchants_df=merchants_df, \
                                                              category_name=category_name, client_info=client_info, \
                                                              configs=configs)
    
    time_weights = all_time_weights[weights_key]["weights"]
    
    # Генерация времени транзакции
    txn_time, txn_unix = get_legit_txn_time(trans_df=client_trans_df, time_weights=time_weights, \
                                            configs=configs, round_clock=round_clock, online=online)
    # Статичные значения для данной функции.
    status = "approved"
    txn_type = "purchase"
    is_fraud = False
    is_suspicious = False
    account = np.nan
    rule = "not applicable"
    
    # Возвращаем словарь со всеми данными сгенерированной транзакции
    return build_transaction(client_id=client_id, txn_time=txn_time, txn_unix=txn_unix, amount=amount, txn_type=txn_type, \
                             channel=channel, category_name=category_name, online=online, merchant_id=merchant_id, \
                             trans_city=trans_city, trans_lat=trans_lat, trans_lon=trans_lon, trans_ip=trans_ip, \
                             device_id=device_id, account=account, is_fraud=is_fraud, is_suspicious=is_suspicious, \
                             status=status, rule=rule)


# 2.

def gen_multiple_legit_txns(configs, ignore_index=True):
    """
    Генерирует несколько транзакций для каждого клиента ориентируясь 
    на существующие транзакции если они есть.
    Количество на клиента берется по нормальному распределению с 
    указанными средним и стандартным отклонением.
    Ограничение забито в функцию gen_trans_number_norm: от 1 до 120 транзакций.
    ---------------------------------------------------
    configs: LegitCfg. Конфиги и данные для генерации легальных транзакций.
    ignore_index: bool. Сбросить ли индекс при конкатенации датафреймов
                  в финальный датафрейм с транзакциями всех клиентов
    """
    clients_df = configs.clients
    trans_df = configs.transactions
    client_devices = configs.transactions
    offline_merchants = configs.offline_merchants
    categories = configs.categories
    avg_txn_num = configs.txn_num["avg_txn_num"]
    txn_num_std = configs.txn_num["txn_num_std"]
    low_bound = configs.txn_num["low_bound"]
    up_bound = configs.txn_num["up_bound"]

    # Сюда собираются все созданные датафреймы с транзакциями клиентов для объединения в конце через pd.concat
    all_clients_trans = [trans_df]
    
    for client_info in clients_df.itertuples():
    	
        # случайное кол-во транзакций на клиента взятое из нормального распределения. Но не меньше 1 и не более 120
        trans_number = gen_trans_number_norm(avg_num=avg_txn_num, num_std=txn_num_std, low_bound=low_bound, \
                                             up_bound=up_bound)
        merchants_from_city = offline_merchants[offline_merchants["city"] == client_info.city]
        client_transactions = trans_df.loc[trans_df.client_id == client_info.client_id]
        
        # id девайсов клиента для онлайн транзакций
        client_device_ids = client_devices.loc[client_devices.client_id == client_info.client_id, "device_id"]
        
        # Сюда будем собирать сгенрированные транзакции в виде словарей.
        pos_txns = []

        for _ in range(trans_number):
            # семплирование категории для транзакции
            category = categories.sample(1, replace=True, weights=categories.share)

            # генерация одной транзакции
            one_trans = generate_one_legit_txn(client_info=client_info, client_trans_df=client_transactions, \
                                                 category=category, client_device_ids=client_device_ids, \
                                                 merchants_df=merchants_from_city, configs=configs)
            
            pos_txns.append(one_trans)
            one_trans_df = pd.DataFrame([one_trans])
            # Добавляем созданную транзакцию к транзакциям клиента, т.к. иногда при генерации других транзакций
            # нужно знать уже созданные транзакции
            client_transactions = pd.concat([client_transactions, one_trans_df], ignore_index=True)
            
        client_new_trans = pd.DataFrame(pos_txns)
        all_clients_trans.append(client_new_trans)
        
    trans_df = pd.concat(all_clients_trans, ignore_index=ignore_index)
    return trans_df