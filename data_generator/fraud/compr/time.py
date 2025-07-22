# Функции создания времени для compromised client фрода

import pandas as pd
import numpy as np
import random

from data_generator.general_time import pd_timestamp_to_unix
from data_generator.fraud.time import derive_from_last_time


# 1. Подфункция генерации времени для фрод кейсов `fast_geo_change` и `fast_geo_change_online`

def generate_time_fast_geo_jump(last_txn_unix, geo_distance, threshold=800):
        """
        Генерация времени с коротким интервалом от предыдущей транзакции, для имитации быстрой смены геопозиции
        ---------------------------------------------------------------------
        last_txn_unix - время последней транзакции в unix формате в секундах.
        geo_distance - кратчайшая дистанция между точками координат последней и текущей транзакции - в километрах.
                       Точки между координатами берутся при генерации транзакции. Домашний город клиента и любой другой город, кроме домашнего.
        threshold - порог скорости перемещения между точками в км/ч. Все что быстрее - фрод.
                    Это нужно чтобы генерировать соответствующее время в зависимости от дистанции между точками транзакций.
                    Быстрая скорость - маленькое время между транзакциями в плане возможностей перемещения на расстояние.
        ---------------------------------------------------------------------
        Возвращает pd.Timestamp и unix время в секундах.
        """

        # Случайно сгенерированная фактическая скорость превышающая легитимный порог. Допустим от 801 до 36000 км/ч
        # до 36000 км/ч т.к. грубо говоря транзакция может быть совершена через 20 минут в 9000 км от предыдущей т.е. как-будто бы скорость 36000 км/ч
        # 9000 км взяты как весьма примерное, самое длинное возможное расстояние между городами России при путешествии самолетом.
        # но в зависимости от расстояния мы берем разные границы для распределений, чтобы не было перекоса в очень быстрое время. 
        # Также 20 минут я случайно взял как средний интервал для подобной фрод транзакции.
        # Конечно же "скорость перемещения" может быть и больше в реальной жизни
        
        if geo_distance < 1000:
            fact_speed = np.random.uniform(threshold + 1, 3000)
        elif geo_distance >= 1000 and geo_distance <= 3000:
            fact_speed = np.random.uniform(threshold + 1, 9000)
        elif geo_distance > 3000 and geo_distance <= 6000:
            fact_speed = np.random.uniform(threshold + 1, 18000)
        else:
            fact_speed = np.random.uniform(threshold + 1, 36000)
        
        # Делим полученную скорость на 3.6 для перевода в м/с - для расчета времени в секундах
        # т.к. будет добавлять к unix времени предыдущей транзакции
        fact_speed /= 3.6

        # переводим дистанцию в метры
        geo_distance = geo_distance * 1000
    
        # интервал времени между последней транзакцией и текущей фрод транзакцией в секундах
        time_interval = geo_distance / fact_speed
        
        txn_unix = round(last_txn_unix + time_interval)
        txn_time = pd.to_datetime(txn_unix, unit="s")

        return txn_time, txn_unix


# 2. Подфункция генерации времени транзакции для правила `trans_freq_increase`
# - несколько частых транзакций подряд

def gen_time_for_frequent_trans(last_txn_unix, configs, test=False):
    """
    Функция для имитации времени нескольких частых транзакций подряд.
    -------------------------------------------------
    last_txn_unix - unix время последней транзакции в секундах
    test - True или False. Тестируем мы функцию или нет.
    --------------------------------------------------
    При test == False возвращает pd.Timestamp и unix time в секундах
    При test == True возвращает pd.Timestamp, unix time в секундах и получившуюся разницу времени
    с предыдущей транзакцией в минутах в виде int
    """
    # мин. разрыв между транз-циями, минут
    freq_low = configs.rules_cfg["freq_txn"]["time"]["freq_low"]
    # макс. разрыв между транз-циями, минут
    freq_high = configs.rules_cfg["freq_txn"]["time"]["freq_high"]

    # частота фрод транзакций. от 1 до 5 минут. Выразим в секундах для удобства расчетов
    freq = np.random.randint(freq_low, freq_high + 1) * 60
    txn_unix = last_txn_unix + freq
    txn_time = pd.to_datetime(txn_unix, unit="s")

    if not test:
        return txn_time, txn_unix
    
    return txn_time, txn_unix, freq


# 3. Конечная функция генерации времени

def get_time_fraud_txn(trans_df, configs, online, round_clock, rule=None, \
                       geo_distance=None, lag=None):
    """
    Создать время для генерируемой compromised client fraud транзакции
    ---------------------------------------
    trans_df: pd.DataFrame. Транзакции текущего клиента. Откуда брать 
              информацию по предыдущим транзакциям клиента.
    configs: ComprClientFraudCfg. Конфиги и данные для генерации фрод 
             транзакци в категории compromised client fraud.
    online: bool. Онлайн или оффлайн покупка. True or False
    round_clock: bool. Круглосуточная или дневная категория.
    rule: str. Название антифрод правила
    geo_distance: int. Дистанция между локацией последней и текущей транзакции если фрод 
                  со сменой геолокации - в километрах
    lag: bool. Задержка по времени от предыдущей транзакции. Нужна для моделирования 
         увеличения частоты транзакций. Это задержка именно между последней легитимной 
         транзакцией и серией частых фрод транзакций. Подразумевается что функция
         get_time_fraud_txn будет использована в цикле, и для первой итерации lag будет True.
    ---------------------------------------------
    Возвращает время для генерируемой транзакции в виде pd.Timestamp и в виде unix времени
    """
    # Предел скорости перемещения клиента между транз-циями км/ч, включительно
    threshold = configs.rules_cfg["threshold"]
    all_time_weights = configs.all_time_weights
    timestamps = configs.timestamps
    
    # Время последней транзакции клиента unix, в секундах
    last_txn_unix = trans_df.unix_time.max()

    # 1. Offline_24h_Fraud - круглосуточные оффлайн покупки
    if not online and round_clock:
        weights_key = "Offline_24h_Fraud"
        
    # 2. Online_Fraud - Онлайн покупки
    elif online:
        weights_key = "Online_Fraud"
        
    # 3. Offline_Day_Fraud - Оффлайн покупки. Дневные категории.
    elif not online and not round_clock:
        weights_key = "Offline_Day_Fraud"
    
    time_weights = all_time_weights[weights_key]["weights"]

    # Правила: другая гео за короткое время либо по локации оффлайн мерчанта либо по новому ip адресу
    if rule in ["fast_geo_change", "fast_geo_change_online"]:
        return generate_time_fast_geo_jump(last_txn_unix=last_txn_unix, geo_distance=geo_distance, \
                                           threshold=threshold)
    
    # Если это не первая фрод транзакция в серии частых транзакций для правила trans_freq_increase
    # Для первой транзакции в серии (lag==True) время будет создано прибавлением интервала к последней транзакции клиента
    elif rule == "trans_freq_increase" and not lag:
        return gen_time_for_frequent_trans(last_txn_unix=last_txn_unix, configs=configs)
    
    # Генерация времени на основе времени предыдущей транзакции, но с учетом гео.
    # Обеспечивает НЕпопадание под правила резкой смены гео.
    elif rule in ["new_ip_and_device_high_amount", "new_device_and_high_amount", "trans_freq_increase"]:
        # Случайный lag_interval от 30 до 60 минут для случаев где дистанция 0
        return derive_from_last_time(last_txn_unix=last_txn_unix, lag_interval=0, min=30, max=60, \
                                     random_lag=True, geo_distance=geo_distance, threshold=threshold)
        
    # Время для остальных правил. Просто семплирование времени в соответсвии с весами
    # берем случайный час передав веса часов для соответсвующейго временного паттерна
    txn_hour = time_weights.hours.sample(n=1, weights=time_weights.proportion, replace=True).iloc[0]
    
    # фильтруем по этому часу timestamp-ы и семплируем timestamp уже с равной вероятностью
    # Дальше будем обрабатывать этот timestamp в некоторых случаях
    timestamps_subset = timestamps.loc[timestamps.hour == txn_hour]
    timestamp_sample = timestamps_subset.sample(n=1, replace=True)
    txn_time = timestamp_sample.timestamp.iloc[0]
    
    return txn_time, pd_timestamp_to_unix(txn_time)
