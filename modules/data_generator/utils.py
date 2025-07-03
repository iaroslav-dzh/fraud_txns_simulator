# Модуль с общими вспомогательными функциями
import pandas as pd
import numpy as np
from scipy.stats import truncnorm
from pyproj import Geod
from dataclasses import dataclass

# 1. 

def build_transaction(client_id, txn_time, txn_unix, amount, type, channel, category_name, online, merchant_id, \
                      trans_city, trans_lat, trans_lon, trans_ip, device_id, account, is_fraud, is_suspicious, \
                      status, rule):
    """
    Собирает словарь с данными транзакции
    """

    txn_dict = {
                "client_id": client_id, "txn_time": txn_time, "unix_time":txn_unix, "amount": amount, "type": type,
                "channel": channel, "category": category_name, "online": online, "merchant_id": merchant_id,
                "trans_city":trans_city, "trans_lat": trans_lat, "trans_lon": trans_lon, "trans_ip":trans_ip,
                "device_id": device_id, "account": account, "is_fraud": is_fraud, "is_suspicious":is_suspicious, "status":status,
                "rule":rule
                }

    return txn_dict


# 2. 

def gen_trans_number_norm(avg_num, num_std, low_bound=1, up_bound=120):
    """
    Возвращает целое число по нормальному распределению.
    Число ограничено выставленными лимитами.
    ----------------------------------------
    avg_num - среднее число транзакций
    num_std - стандартное отклонение числа транзакций
    low_bound - минимальное возможное число транзакций
    up_bound - максимальное возможное число транзакций
    size - размер выборки
    """
    
    # Вернет float в виде np.ndarray
    random_float = truncnorm.rvs(a=(low_bound - avg_num) / num_std, b=(up_bound - avg_num) / num_std, \
                                 loc=avg_num, scale=num_std, size=1)

    # Преобразуем float в int и извлекаем из массива
    return random_float.astype(int)[0]


# 3. 

def get_values_from_truncnorm(low_bound, high_bound, mean, std, size=1):
    """
    Сгенерировать массив чисел из обрезанного нормального распределения.
    Можно сгенерировать массив с одним числом
    ------------
    low_bound - float, int. Нижняя граница значений
    high_bound - float, int. Верхняя граница значений 
    mean - float, int. Среднее
    std - float, int. Стандартное отклонение
    size - Количество чисел в возвращаемом массиве
    ------------
    Возвращает np.ndarray
    """
    return truncnorm.rvs((low_bound - mean) / std, (high_bound - mean) / std, loc=mean, scale=std, size=size)

# 4.

def calc_distance(lat_01, lon_01, lat_02, lon_02, km=True):
    """
    Считает растояние между двумя координатами на Земном шаре.
    Между координатами последней по времени транзакции и переданными координатами.
    -----------------------------
    lat_01 - float. Широта первой точки
    lon_01 - float. Долгота первой точки
    lat_02 - float. Широта второй точки
    lon_02 - float. Долгота второй точки
    km - bool. Единицы измерения. Либо километры либо метры. Километры округляет до 2-х знаков, метры до целого.
    """
    
    # Геодезический расчёт по эллипсоиду WGS84
    geod = Geod(ellps="WGS84")
    # [-1] берет последний элемент из кортежа. Это метры
    distance_m = geod.inv(lon_01, lat_01, lon_02, lat_02)[-1]

    if km:
        return round(distance_m / 1000, 2)

    return round(distance_m)


# . Функция sample_category. На данный момент предназначена только для фрода

def sample_category(categories, online=None, is_fraud=None, rule=None):
    """
    categories - pd.DataFrame с категориями и их характеристиками
    online - bool. Онлайн или оффлайн категория нужна
    is_fraud - bool. Фрод или не фрод. От этого зависит вероятность категории.
    """

    if is_fraud and online and rule != "trans_freq_increase":
        online_categories = categories.loc[categories.online == True]
        cat_sample = online_categories.sample(1, weights=online_categories.fraud_share)
        return cat_sample

    elif is_fraud and online and rule == "trans_freq_increase":
        chosen_categories = categories.loc[categories.category.isin(["shopping_net", "misc_net"])]
        cat_sample = chosen_categories.sample(1, weights=chosen_categories.fraud_share)
        return cat_sample

        
    elif is_fraud and not online:
        offline_categories = categories.loc[categories.online == False]
        cat_sample = offline_categories.sample(1, weights=offline_categories.fraud_share)
        return cat_sample


# Функция семплирования антифрод-правила

def sample_rule(rules):
    """
    rules - pd.DataFrame с названиями правил и их весами
    """
    return rules.rule.sample(1, weights=rules.weight).iat[0]


# . Датакласс для конфигов транзакций. Это данные на основе которых будут генерироваться транзакции
# на данный момент этот класс служит только для фрода

@dataclass
class ConfigForTrans:
    """
    Это данные на основе которых будут генерироваться транзакции
    ---------------------
    clients: pd.DataFrame
    timestamps: pd.DataFrame
    transactions: pd.DataFrame
    client_devices: pd.DataFrame
    offline_merchants: pd.DataFrame
    categories: pd.DataFrame
    online_merchant_ids: pd.Series
    time_weights_dict: dict
    rules: pd.DataFrame
    cities: pd.DataFrame
    fraud_devices: pd.DataFrame
    fraud_ips: pd.DataFrame
    fraud_amounts: pd.DataFrame 
    """
    clients: pd.DataFrame
    timestamps: pd.DataFrame
    transactions: pd.DataFrame
    client_devices: pd.DataFrame
    offline_merchants: pd.DataFrame
    categories: pd.DataFrame
    online_merchant_ids: pd.Series
    time_weights_dict: dict
    rules: pd.DataFrame
    cities: pd.DataFrame
    fraud_devices: pd.DataFrame
    fraud_ips: pd.DataFrame
    fraud_amounts: pd.DataFrame


# . Датакласс для конфигов транзакций дропов

@dataclass
class DropConfigs:
    """
    Это данные на основе которых будут генерироваться транзакции
    ---------------------
    clients: pd.DataFrame
    timestamps: pd.DataFrame
    transactions: pd.DataFrame
    accounts: pd.DataFrame. Номера счетов клиентов. 
    outer_accounts: pd.Series. Номера внешних счетов для транзакций вне банка.
    client_devices: pd.DataFrame
    offline_merchants: pd.DataFrame
    categories: pd.DataFrame
    online_merchant_ids: pd.Series
    time_weights_dict: dict
    rules: pd.DataFrame
    cities: pd.DataFrame
    fraud_amounts: pd.DataFrame
    period_in_lim: int. Количество входящих транзакций после которых дроп уходит на паузу.
    period_out_lim: int. Количество исходящих транзакций после которых дроп уходит на паузу.
    lag_interval: int. Минуты. На сколько дроп должен делать паузу после
                       достижения лимита входящих и/или исходящих транзакций.
                       Например 1440 минут(сутки). Отсчет идет от первой транзакции в последнем периоде активности.
    two_way_delta: dict. Минимум и максимум дельты времени. Для случаев когда дельта может быть и положительной и отрицательной.
                         Эта дельта прибавляется к lag_interval, чтобы рандомизировать время возобновления активности,
                         чтобы оно не было ровным. Берется из конфига drops.yaml
    pos_delta: dict. Минимум и максимум случайной дельты времени в минутах. Для случаев когда дельта может быть только положительной.
                          Эта дельта - промежуток между транзакциями дропа в одном периоде. Просто прибавляется ко времени последней транзакции.
    """
    clients: pd.DataFrame
    timestamps: pd.DataFrame
    transactions: pd.DataFrame
    accounts: pd.DataFrame
    outer_accounts: pd.Series
    client_devices: pd.DataFrame
    offline_merchants: pd.DataFrame
    categories: pd.DataFrame
    online_merchant_ids: pd.Series
    time_weights_dict: dict
    rules: pd.DataFrame
    cities: pd.DataFrame
    fraud_amounts: pd.DataFrame
    period_in_lim: int
    period_out_lim: int
    lag_interval: int
    two_way_delta: dict
    pos_delta: dict