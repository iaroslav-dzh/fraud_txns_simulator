# Функции создания времени для фрода

import pandas as pd
import numpy as np

from data_generator.general_time import *
from data_generator.utils import get_values_from_truncnorm


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