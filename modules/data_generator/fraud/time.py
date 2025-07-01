# Функции создания времени для фрода

import pandas as pd
import numpy as np
import random

from data_generator.general_time import *
from data_generator.utils import get_values_from_truncnorm


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


# 2. Подфункция генерации времени транзакции для правила `trans_freq_increase` - упрощенная
# - несколько частых транзакций подряд

def gen_time_for_frequent_trans(last_txn_time, last_txn_unix, freq_low=1, freq_high=5, test=False):
    """
    Функция для имитации времени нескольких частых транзакций подряд.
    -------------------------------------------------
    last_txn_time - pd.Timestamp последней транзакции
    last_txn_unix - unix время последней транзакции в секундах
    freq_low - int. минимальный разрыв между транзакциями в цепочке транзакций, в минутах
    freq_high - int. максимальный разрыв между транзакциями в цепочке транзакций, в минутах
    test - True или False. Тестируем мы функцию или нет.
    --------------------------------------------------
    При test == False возвращает pd.Timestamp и unix time в секундах
    При test == True возвращает pd.Timestamp, unix time в секундах и получившуюся разницу времени
    с предыдущей транзакцией в минутах в виде int
    """

    
    # частота фрод транзакций. от 1 до 5 минут. Выразим в секундах для удобства расчетов
    freq = random.randint(freq_low, freq_high) * 60
    txn_unix = last_txn_unix + freq
    txn_time = pd.to_datetime(txn_unix, unit="s")

    if not test:
        return txn_time, txn_unix
    
    else:
        return txn_time, txn_unix, freq


# . Подфункция генерации времени c прибавлением к времени последней транзакции derive_from_last_time

def derive_from_last_time(last_txn_unix, lag_interval, min, max, random_lag=False, geo_distance=0, threshold=800):
    """
    Создать время основываясь на времени последней транзакции.
    Либо на основании гео дистанции между транзакциями либо на основании заданного лага по времени.
    Для гео надо ввести geo_distance. Для лага, НЕ вводить geo_distance, ввести lag_interval
    ---------------
    last_txn_unix - int. Unix время последней транзакции в секундах.
    lag_interval - int. Желаемый лаг по времени от последней транзакции в минутах.
    min - int. Минуты. Минимальное значение если нужен случайный лаг по времени. Выставляется когда random_lag - True
    max - int. Минуты. Максимальное значение если нужен случайный лаг по времени. Не включается в возможные выбор, ставить на 1 больше.
               Выставляется когда random_lag - True
    random_lag - bool. Должно ли значение лага быть случайным. Берется по равномерному распределению.
    geo_distance - int. Расстояние между координатами текущей и последней транзакции в километрах.
    threshold - int. Максимальная допустимая скорость перемещения км/ч между совершением транзакций,
                     для случаев когда расстояние больше 500 километров.
    ---------------
    Возвращает unix время в секундах
    """
    # Перевод в секунды для расчетов
    lag_interval = lag_interval * 60

    if random_lag:
        lag_interval = np.random.randint(min, max) * 60

    if geo_distance == 0:
        txn_unix = last_txn_unix + lag_interval
        txn_time = pd.to_datetime(txn_unix, unit="s")
        return txn_time, txn_unix
    
    if geo_distance <= 500:
        mean = 90
        std = 20
        speed = get_values_from_truncnorm(low_bound=50, high_bound=120, mean=mean, std=std).astype("int")[0]
        # Расчет добавления времени и перевод в секунды
        lag_interval = round((geo_distance / speed) * 3600)

    elif geo_distance > 500:
        mean = 300
        std = 200
        speed = get_values_from_truncnorm(low_bound=50, high_bound=threshold, mean=mean, std=std).astype("int")[0]
        # Расчет добавления времени и перевод в секунды
        lag_interval = round((geo_distance / speed) * 3600)

    txn_unix = last_txn_unix + lag_interval
    txn_time = pd.to_datetime(txn_unix, unit="s")
        
    return txn_time, txn_unix