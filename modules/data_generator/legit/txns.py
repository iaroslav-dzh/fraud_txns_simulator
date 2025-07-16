# Функции генерации легальных транзакций
import pandas as pd
import numpy as np

from data_generator.utils import build_transaction
from data_generator.legit.txndata import get_txn_location_and_merchant
from data_generator.legit.time.time import get_legit_txn_time


# 1.

def generate_one_legit_txn(client_info, client_trans_df, client_device_ids, category, merchants_df, \
                           online_merchant_ids, timestamps, timestamps_1st_month, all_time_weights, \
                           legit_cfg):
    """
    Генерация одной легальной транзакции покупки для клиента
    ------------------------------------------------
    client_info: namedtuple, полученная в результате итерации с помощью .itertuples() через датафрейм с информацией о клиентах
    client_trans_df: pd.DataFrame. Транзакции клиента.
    client_device_ids: pd.Series. id девайсов клиента.
    category: pd.DataFrame. Одна запись с категорией и её характеристиками
    merchants_df: pd.DataFrame. Оффлайн мерчанты заранее отфильтрованные по городу клиента т.к. это легальные транзакции
    online_merchant_ids: pd.Series. id для онлайн мерчантов
    timestamps: pd.DataFrame. timestamps для генерации времени.
    timestamps_1st_month: pd.DataFrame. сабсет timestamps отфильтрованный по первому месяцу и, 
                если применимо, году. Чтобы генерировать первые транзакции.
    all_time_weights: dict. веса для часов времени в виде словаря содержащего датафрейм с весами, 
                        название распределения и цветом для графика.
    legit_cfg: dict. Конфиги легальных транзакция из legit.yaml
    """

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
                                                              online_merchant_ids=online_merchant_ids)
    time_weights = all_time_weights[weights_key]["weights"]
    
    # Генерация времени транзакции
    txn_time, txn_unix = get_legit_txn_time(trans_df=client_trans_df, time_weights=time_weights, \
                                            timestamps=timestamps, timestamps_1st_month=timestamps_1st_month, \
                                            legit_cfg=legit_cfg, round_clock=round_clock, online=online)
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