# Функции генерации легальных транзакций
import pandas as pd
import numpy as np

from data_generator.utils import build_transaction, gen_trans_number_norm, amt_rounding
from data_generator.legit.txndata import get_txn_location_and_merchant
from data_generator.legit.time.time import get_legit_txn_time


# 1. Генерация одной легальной транзакции покупки для клиента.

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
    amount = max(1, round(np.random.normal(amt_mean, amt_std), 2))
    amount = amt_rounding(amount=amount, rate=0.6) # Случайное целочисленное округление

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


# 2. Функция генерации множества легальных транзакций для нескольких клиентов

def gen_multiple_legit_txns(configs, txn_recorder, ignore_index=True):
    """
    Генерирует несколько транзакций для каждого клиента ориентируясь 
    на существующие транзакции если они есть.
    Количество на клиента берется по нормальному распределению с 
    указанными средним и стандартным отклонением.
    Ограничение забито в функцию gen_trans_number_norm: от 1 до 120 транзакций.
    ---------------------------------------------------
    configs: LegitCfg. Конфиги и данные для генерации легальных транзакций.
    txn_recorder: LegitTxnsRecorder. 
    ignore_index: bool. Сбросить ли индекс при конкатенации датафреймов
                  в финальный датафрейм с транзакциями всех клиентов
    """
    clients_df = configs.clients
    trans_df = configs.transactions
    client_devices = configs.client_devices
    offline_merchants = configs.offline_merchants
    categories = configs.categories
    avg_txn_num = configs.txn_num["avg_txn_num"]
    txn_num_std = configs.txn_num["txn_num_std"]
    low_bound = configs.txn_num["low_bound"]
    up_bound = configs.txn_num["up_bound"]
    
    # Сюда будем собирать сгенрированные транзакции клиента в виде словарей.
    client_txns = txn_recorder.client_txns
    
    for client_info in clients_df.itertuples():
        txn_recorder.clients_counter += 1

        # случайное кол-во транзакций на клиента взятое из нормального распределения с мин. и макс. лимитами
        txns_num = gen_trans_number_norm(avg_num=avg_txn_num, num_std=txn_num_std, low_bound=low_bound, \
                                             up_bound=up_bound)
        merchants_from_city = offline_merchants[offline_merchants["city"] == client_info.city]
        client_transactions = trans_df.loc[trans_df.client_id == client_info.client_id]
        
        # id девайсов клиента для онлайн транзакций
        client_device_ids = client_devices.loc[client_devices.client_id == client_info.client_id, "device_id"]
        
        for _ in range(txns_num):
            # семплирование категории для транзакции
            category = categories.sample(1, replace=True, weights=categories.share)

            # генерация одной транзакции
            one_txn = generate_one_legit_txn(client_info=client_info, client_trans_df=client_transactions, \
                                             category=category, client_device_ids=client_device_ids, \
                                             merchants_df=merchants_from_city, configs=configs)
            # Запись транз-ции в список транз-ций текущего клиента.
            client_txns.append(one_txn)
            txn_recorder.txns_counter += 1 # счетчик всех транз-ций

            # Управление записью транзакций чанками в файлы.
            txn_recorder.record_chunk(txn=one_txn, txns_num=txns_num)
            
            # Добавляем созданную транзакцию к транзакциям клиента, т.к. иногда 
            # при генерации других транзакций нужно знать уже созданные транзакции
            one_txn_df = pd.DataFrame([one_txn])
            client_transactions = pd.concat([client_transactions, one_txn_df], ignore_index=ignore_index)
        
        client_txns.clear() # Конец генерации на клиента. Чистим список для текущего кл-та

    txn_recorder.build_from_chunks()

    # Запись собранного датафрейма в два файла в разные директории: data/generated/lastest/
    # И data/generated/history/<своя_папка_с_датой_временем>
    txn_recorder.write_built_data()