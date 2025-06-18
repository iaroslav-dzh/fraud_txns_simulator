# Функции по проекту antifraud_01. Этап генерации данных. Функции из раздела генерации времени
# Ноутбук 05_time_patterns_generation

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
import re
import geopandas as gpd
import random
from scipy.stats import truncnorm, norm
from collections import defaultdict
import math
from datetime import datetime


# 1. Круглосуточные оффлайн legit транзакции offline_24h_legit_time_dist
# - Распределение времени для легитимных транзакций в круглосуточных оффлайн категориях

def offline_24h_legit_time_dist():
    # Время в минутах с начала суток
    start, end = 0, 1439  # 00:00 до 23:59
    
    # название паттерна. Пригодится если нужно построить график распределения
    title = "Offline. 24h. Legit"
    
    # небольшой пик утром в районе 9. Обрезаный слева по 6-и утра
    mean_morn = 9 * 60
    std_morn = 90
    morn_start = 6 * 60
    
    # Truncated normal - пик утро
    dist_morn = truncnorm((morn_start - mean_morn)/ std_morn, (end - mean_morn) / std_morn, loc=mean_morn, scale=std_morn)
    minutes_morn = dist_morn.rvs(3000).astype(int)
    
    # небольшой пик обед в районе 13. Обрезанный по 16-и часам справа
    mean_noon = 14 * 60
    std_noon= 75
    noon_end = 16.5 * 60
    
    # Truncated normal - пик обед
    dist_noon = truncnorm((start - mean_noon)/ std_noon, (noon_end - mean_noon) / std_noon, loc=mean_noon, scale=std_noon)
    minutes_noon = dist_noon.rvs(5000).astype(int)
    
    
    # Вечерний пик. В районе 19 часов
    mean_evn = 19.7 * 60
    std_evn = 120
    evn_start = 17 * 60
    evn_end = 23.3 * 60
    
    # Truncated normal - пик вечером
    dist_evn = truncnorm((evn_start - mean_evn) / std_evn, (evn_end - mean_evn) / std_evn, loc=mean_evn, scale=std_evn)
    minutes_evn = dist_evn.rvs(9000).astype(int)
    
    # ночная равномерная небольшая активность с 0 до 6:50 утра включительно
    night_hours_add =  np.array([np.random.uniform(0, 710) for _ in range(300)]).astype(int)
    
    # соединяем все созданные массивы в один
    minutes = np.concatenate((minutes_morn, minutes_noon, minutes_evn, night_hours_add), axis=0) #  

    # переводим значения массива в тип pd.Timedelta.
    # Затем через аттрибут .dt.components получаем датафрейм со значениями массива разбитых на колонки hours и minutes
    # берем hours оттуда
    times = pd.Series(pd.to_timedelta(minutes, unit="min")).dt.components
    time_hours = times.hours

    return time_hours, title


# 2. Круглосуточные оффлайн категории - фрод.
# - фрод транзакции в круглосуточных оффлайн категориях

def offline_24h_fraud_time_dist():
    
    # название паттерна. Пригодится если нужно построить график распределения
    title = "Offline. 24h. Fraud"
    
    # равномерная активность с 8 до 23
    day_time =  np.array([np.random.uniform(8, 23.9) for _ in range(500)]).astype(int)
    
    # равномерная сниженная активность с 0 до 8
    night_time = np.array([np.random.uniform(0, 7.9) for _ in range(120)]).astype(int)
    
    # соединяем все созданные массивы в один и делаем серией
    time_hours = pd.Series(np.concatenate((day_time, night_time), axis=0), name="hours")

    return time_hours, title


# 3. Онлайн категории, НЕ фрод online_legit_time_dist

def online_legit_time_dist():
    
    # название паттерна. Пригодится если нужно построить график распределения
    title = "Online. Legit"
    
    # Первый слой. равномерно с утра до вечера. с 8:00 до 16:59
    day_time = np.array([np.random.uniform(8*60, 16.9*60) for _ in range(500)]).astype(int)
    
    
    # небольшой пик обед в районе 13. Обрезанный по 12 и 17 часам
    mean_noon = 14 * 60
    std_noon= 75
    noon_start = 12 * 60
    noon_end = 16.5 * 60
    
    # пик обед. Распределение
    dist_noon = truncnorm((noon_start - mean_noon)/ std_noon, (noon_end - mean_noon) / std_noon, loc=mean_noon, scale=std_noon)
    minutes_noon = dist_noon.rvs(400).astype(int)
    
    
    # Вечерний пик. От 17 до 23 часов.
    mean_evn = 19.7 * 60
    std_evn = 120
    evn_start = 17 * 60
    evn_end = 23.3 * 60
    
    # пик вечером. Распределение
    dist_evn = truncnorm((evn_start - mean_evn) / std_evn, (evn_end - mean_evn) / std_evn, loc=mean_evn, scale=std_evn)
    minutes_evn = dist_evn.rvs(2000).astype(int)
    
    # ночная равномерная низкая активность с 0 до 7:59 утра
    night_hours_add = np.array([np.random.uniform(0, 7.9*60) for _ in range(200)]).astype(int)
    
    # соединяем все созданные массивы в один
    minutes = np.concatenate((day_time, minutes_noon, minutes_evn, night_hours_add), axis=0) #  
    
    times = pd.Series(pd.to_timedelta(minutes, unit="min")).dt.components
    time_hours = times.hours

    return time_hours, title


# 4. Онлайн фрод

def online_fraud_time_dist():
    # Время в минутах с начала суток. В данном случае нужно для ограничения распределения в рамках 24 часов.
    start, end = 0, 1439  # 00:00 до 23:59

    # название паттерна. Пригодится если нужно построить график распределения
    title = "Online. Fraud"
    
    # Ночной пик после 00:00
    mean = 1 # 1-я минута суток
    std = 120
    
    # распределение ночного пика с максимумом примерно в первом часу суток: 00:00-00:59.
    # Обрезка распределения слева по 00:00
    dist = truncnorm((start - mean) / std, (end - mean) / std, loc=mean, scale=std)
    minutes = dist.rvs(2000).astype(int)
    
    # Добавляем вечернюю активность - обрезка справа в 0 часов. Ограничиваем значения 00:00 часами справа
    mean_evn = 23.9*60
    std_evn = 120
    evn_end = 23.9*60
    
    dist_evn = truncnorm((start - mean_evn) / std_evn, (evn_end - mean_evn) / std_evn, loc=mean_evn, scale=std_evn)
    minutes_evn = dist_evn.rvs(2000).astype(int)
    
    # добавим небольшое количество равномерных значений на протяжении дня, с 4 до 20 включительно
    mid_start = 4 * 60
    mid_end = 20.9 * 60
    midday_add =  np.array([np.random.uniform(mid_start, mid_end) for _ in range(4250)]).astype(int)
    
    # соединяем все три массива
    total_minutes = np.concatenate((minutes, midday_add, minutes_evn)) # 
    
    times = pd.Series(pd.to_timedelta(total_minutes, unit="min")).dt.components
    time_hours = times.hours

    return time_hours, title


# 5. Дневной оффлайн, легитимные транзакции.

def offline_day_legit_time_dist():

    # название паттерна. Пригодится если нужно построить график распределения
    title = "Offline. Day-only. Legit"
    
    # равномерное распределение с 8:00 до 17:00
    day_time = np.array([np.random.uniform(8*60, 16.9*60) for _ in range(2000)]).astype(int)
    
    # слабый пик обед в районе 13. Интервал с 12 до 17:00
    mean_noon = 14 * 60
    std_noon= 75
    noon_start = 12 * 60
    noon_end = 16.5 * 60
    
    # пик обед
    dist_noon = truncnorm((noon_start - mean_noon)/ std_noon, (noon_end - mean_noon) / std_noon, loc=mean_noon, scale=std_noon)
    minutes_noon = dist_noon.rvs(1000).astype(int)
    
    
    # Вечерний пик. С 17:00 до 22:00
    mean_evn = 19.7 * 60
    std_evn = 120
    evn_start = 17 * 60
    evn_end = 21.9 * 60
    
    # пик вечером
    dist_evn = truncnorm((evn_start - mean_evn) / std_evn, (evn_end - mean_evn) / std_evn, loc=mean_evn, scale=std_evn)
    minutes_evn = dist_evn.rvs(9000).astype(int)
    
    
    # соединяем все созданные массивы в один
    minutes = np.concatenate((day_time, minutes_noon, minutes_evn), axis=0) #
    
    # переводим значения массива в тип pd.Timedelta.
    # Затем через аттрибут .dt.components получаем датафрейм со значениями массива разбитых на колонки hours и minutes
    # берем hours оттуда
    times = pd.Series(pd.to_timedelta(minutes, unit="min")).dt.components
    time_hours = times.hours

    return time_hours, title


# 6. Дневной оффлайн. Фрод

def offline_day_fraud_time_dist():
    # название паттерна. Пригодится если нужно построить график распределения
    title = "Offline. Day-only. Fraud"

    # равномерная активность с 8:00 до 22:00
    day_time =  np.array([np.random.uniform(8, 21.9) for _ in range(500)]).astype(int)
    
    # переведем массив в серию
    time_hours = pd.Series(day_time, name="hours")

    return time_hours, title


# 7. Функция генерации весов для часов в периоде времени `gen_weights_for_time` - Созданная из подфункций  
# На основании созданных функций распределений времени для соответсвующих временных паттернов, выдает веса(доли) для каждого часа в распределении.
# Которые будут в использоваться в рандомизации времени генерируемой транзакции

def gen_weights_for_time(is_fraud=False, round_clock=False, online=False):
    """
    возвращает датафрейм с часами от 0 до 23 и их весами,
    название паттерна в виде строки и цвет для возможного графика - в зависимости от фрод не фрод
    
    is_fraud - True или False. По умолчанию False
    round_clock - Круглосуточная категория или нет. True или False. По умолчанию False
    online - None, True или False. По умолчанию False
    """
    
    #  Далее в зависимости от условий генерация распределения времени для транзакций

    # 1. Категория - круглосуточные, оффлайн, НЕ фрод 
    if not is_fraud and round_clock and not online:
        time_hours, title = offline_24h_legit_time_dist()
        
    # 2. Оффлайн фрод. круглосуточные категории
    elif is_fraud and round_clock and not online:
        time_hours, title = offline_24h_fraud_time_dist()

    # 3. НЕ фрод. Онлайн покупки
    elif not is_fraud and round_clock and online:
        time_hours, title = online_legit_time_dist()
        
    # 4. онлайн фрод
    elif is_fraud and round_clock and online:
        time_hours, title = online_fraud_time_dist()

    # 5. Не круглосуточный. Не фрод. Оффлайн
    elif not is_fraud and not round_clock and not online:
        time_hours, title = offline_day_legit_time_dist()

    # 6. Фрод. Не круглосуточный. Оффлайн. 
    elif is_fraud and not round_clock and not online:
        time_hours, title = offline_day_fraud_time_dist()
        
    
    # посчитаем долю каждого часа. Это и будут веса 
    # т.е. вероятность транзакций в этот час для выбранного временного паттерна
    # переведем индекс в колонку т.к. в индексе у нас часы
    hour_weights = time_hours.value_counts(normalize=True).sort_index().reset_index()

    # если период не круглосуточный. То добавить колонку остальных часов со значениями равными 0, для построения графиков со шкалой от 0 до 23
    if not round_clock:
        all_hours = pd.DataFrame({"hours":np.arange(0,24, step=1)}).astype(int)
        hour_weights = all_hours.merge(hour_weights, how="left", on="hours").fillna(0)
        
    # цвет для графика.
    if not is_fraud:
        color = "steelblue"
    elif is_fraud:
        color = "indianred"
        
    return hour_weights, title, color


# 8. Функция генерации весов для всех паттернов времени и сбора их в словарь
# содержит в себе функцию `gen_weights_for_time()`
# Нужна для генерации всех паттернов один раз, чтобы дальше не вызывать функцию генерации каждого паттерна 
# каждый раз для транзакции либо по отдельности записывать в переменные

def get_all_time_patterns(pattern_args):
    """
    pattern_args - словарь с названием паттерна в ключе и словарем из аргументов для функции gen_weights_for_time,
                   соответствующим паттерну.
    """

    time_weights = defaultdict(dict)
    
    for key in pattern_args.keys():
        weights, title, color = gen_weights_for_time(**pattern_args[key])
        time_weights[key]["weights"] = weights
        time_weights[key]["title"] = title
        time_weights[key]["color"] = color

    return time_weights

# 9. Функция построения одиночного графика распределения

def plot_time_weights(weights, title, color, ax):
    sns.barplot(x=weights.hours, y=weights.proportion, color=color, ax=ax)
    ax.set_xlim(-0.5, 23.5)
    ax.set_ylim(0, 0.4)
    ax.grid(axis="y")
    ax.set_title(title)
    

# 10. # функция построения нескольких графиков time_weights в две колонки

def plot_all_patterns(time_weights):
    """
    Строит графики всех распределений из time_weights
    time_weights - словарь с ключами - названиями паттернов и
                   под каждым ключом еще словарь с весами для паттерна,
                   названием для графика и цветом графика.
                   Его можно сгенерировать функцией get_all_time_patterns
    """
    
    dict_len = len(time_weights)
    rows_number = math.ceil(dict_len / 2)
    
    rows_list = []
    one_row = []
      
    for index, key in enumerate(time_weights.keys(), start=1):
        one_row.append(key)
        if len(one_row) == 2 or index == dict_len:
            rows_list.append(one_row.copy())
            one_row = []
    
    
    fig, axes = plt.subplots(nrows=rows_number, ncols=2, figsize=(10, rows_number*3))
    
    for sub_axes, keys in zip(axes, rows_list):
        for ax, key in zip(sub_axes, keys):
            weights = time_weights[key]["weights"]
            title = time_weights[key]["title"]
            color = time_weights[key]["color"]
            # строим график на его оси
            plot_time_weights(weights, title, color, ax)
    
    plt.tight_layout()
    plt.show()


# Разбивка будущей функции генерации времени транзакции `get_time_for_trans` на подфункции
# - Не все функции в этом блоке будут частью `get_time_for_trans`. 
# Некоторые будут просто связаны с этой функцией, например, создавать данные для аргументов

# 11. Функция перевода pd.Series с datetime64 в unix time в секундах

def datetime_series_to_unix(series):
    """
    Функция перевода pd.Series с datetime64 в unix time в секундах
    ----------------------
    series - pd.Series с типом datetime64
    """
    unix_time_series = (series - pd.Timestamp("1970-01-01")) // pd.Timedelta('1s')

    return unix_time_series


# 12. Функция создания датафрейма с диапазоном timestamp-ов

def create_timestamps_range_df(start, end, format="%Y-%m-%d", freq="min"):
    """
    функция создания датафрейма с диапазоном timestamp-ов.
    Возвращает pd.DataFrame | timestamp | hour | unix_time
    -----------------------------------------------------
    start - str. начало диапазона. Дата или дата и время формата указанного в format. По умолчанию %Y-%m-%d
    end - str. конец диапазона. Дата или дата и время формата указанного в format. По умолчанию %Y-%m-%d
    format - str. Строка формата stftime. Формат передаваемых start и end. 
    freq - str. частота генерации timestamp-ов. Минуты, секунды, дни и т.д. 'min', 's', 'D' etc.
    """
    timestamps = pd.DataFrame(pd.Series(pd.date_range(pd.to_datetime(start, format=format), \
                                                  pd.to_datetime(end, format=format), freq=freq), name="timestamp"))
    timestamps["hour"] = timestamps.timestamp.dt.hour
    timestamps["unix_time"] = datetime_series_to_unix(timestamps.timestamp)

    return timestamps

# 13. Функция перевода pd.Timestamp в unix время

def pd_timestamp_to_unix(timestamp):
    """
    Переводит pandas timestamp в unix время
    timestamp - чистый pandas timestamp. Без индексов. Не серия, не датафрейм.
    """
    unix_time = (timestamp - pd.Timestamp("1970-01-01")) // pd.Timedelta('1s')

    return unix_time


# 14. Подфункция для генерации времени когда нет предыдущих транзакций `sample_time_for_trans`

def sample_time_for_trans(timestamps, time_weights):
    """
    Для понимания аргументов см. docstring для get_time_for_trans.
    Для этого исполни ?get_time_for_trans в ячейке

    Генерирует время для транзакции в виде timestamp и unix времени
    """
    # семплируем час из весов времени, указав веса для семплирования
    txn_hour = time_weights.hours.sample(n=1, weights=time_weights.proportion, replace=True).iloc[0]
    
    # фильтруем основной датафрейм с диапазоном таймстемпов по этому часу
    timestamps_subset = timestamps.loc[timestamps.hour == txn_hour]
    
    # из отфильтрованного датафрейма таймстемпов семплируем один таймстемп с равной вероятностью
    txn_time = timestamps_subset.timestamp.sample(n=1, replace=True).iloc[0]
    txn_unix = pd_timestamp_to_unix(txn_time)

    return txn_time, txn_unix


# 16. Функция логирования для `check_min_interval_from_near_trans`

def log_check_min_time(client_id, txn_time, txn_unix, online, closest_txn_offline, closest_txn_online, last_txn, \
                       close_flag):
    """
    Логирует нужные данные для дебаггинга.
    closest_txn_offline
    closest_txn_online
    """
    if not closest_txn_offline.empty:
        closest_offline_time = closest_txn_offline.txn_time.iloc[0]
        closest_offline_unix = closest_txn_offline.unix_time.iloc[0]
    else:
        closest_offline_time = pd.NaT
        closest_offline_unix = np.nan

    if not closest_txn_online.empty:
        closest_online_time = closest_txn_online.txn_time.iloc[0]
        closest_online_unix = closest_txn_online.unix_time.iloc[0]
    else:
        closest_online_time = pd.NaT
        closest_online_unix = np.nan


    last_txn_time = last_txn.txn_time.iloc[0]
    last_txn_unix = last_txn.unix_time.iloc[0]
    last_online_flag = last_txn.online.iloc[0]
    
    log_df = pd.DataFrame({"client_id":[client_id], "txn_time":[txn_time], "txn_unix":[txn_unix], "online":[online], \
                           "closest_offline_time":[closest_offline_time], "closest_offline_unix":[closest_offline_unix], \
                            "closest_online_time":[closest_online_time], "closest_online_unix":[closest_online_unix], \
                            "last_txn_time":[last_txn_time], "last_txn_unix":[last_txn_unix], \
                           "last_online":[last_online_flag], "condition":[close_flag]})
        
    file_exists = os.path.exists("./data/generated_data/log_check_min_time.csv")
    
    if file_exists:
        log_df.to_csv("./data/generated_data/log_check_min_time.csv", mode="a", header=False)
    else:
        log_df.to_csv("./data/generated_data/log_check_min_time.csv")


# 17. Подфункция `check_min_interval_from_near_trans` - когда есть предыдущие транзакции

def check_min_interval_from_near_trans(client_txns, timestamp_sample, online, round_clock, offline_time_diff=60, \
                                       online_time_diff=6, online_ceil=60, general_diff=30, general_ceil=90, test=False):
    """
    Если для сгенерированного времени есть транзакции, которые по времени ближе заданного минимума, 
    то создать время на основании времени последней транзакции + установленный минимальный интервал.
    Учитывает разницу между типами транзакций: онлайн-онлайн, онлайн-оффлайн, оффлайн-оффлайн.
    Можно поставить свои минимальные интервалы для случаев: онлайн-онлайн, онлайн-оффлайн, оффлайн-оффлайн. 
    Для оффлайн-оффлайн - один фиксированный интервал.
    Для онлайн-онлайн задается минимум и максимум откуда интервал берется с равной вероятностью
    Для оффлайн-онлайн задается минимум и максимум откуда интервал берется с равной вероятностью

    Также, условия в функции сделаны так, что в параметрах функции для минимальных временных разниц
    всегда должно быть такое отношение значений:
    offline_time_diff > general_diff > online_time_diff
    то есть
    оффлайн-оффлайн разница > оффлайн-онлайн разница > онлайн-онлайн разница
    -----------------------------------------------

    client_txns - датафрейм с транзакциями клиента.
    timestamp_sample - случайно выбранная запись из датафрейма с таймстемпами
    online - boolean. Онлайн или оффлайн категория
    round_clock - boolean. Круглосуточная или дневная категория.
    offline_time_diff - int. желаемая минимальная разница от последней оффлайн транзакции в минутах
    online_time_diff - int. желаемая минимальная разница от последней оффлайн транзакции в минутах
    online_ceil - int. желаемая максимальная разница от последней оффлайн транзакции в минутах
    general_diff - int. минимально допустимая разница от последней транзакции противоположной по флагу online.
    general_ceil - int. максимальная разница от последней транзакции противоположной по флагу online.
                   При случае необходимости увеличения разрыва времени.
    test - boolean. True - логировать исполнение функции в csv.
    ------------------------------------------------
    Возвращает pd.Timestamp и int unix время в секундах 
    """

    assert offline_time_diff > general_diff, f"""offline_time_diff must not be lower than general_diff. 
                        {offline_time_diff} vs {general_diff} Check passed arguments"""
    assert offline_time_diff > online_time_diff, f"""offline_time_diff must not be lower than online_time_diff.
                        {offline_time_diff} vs {online_time_diff} Check passed arguments"""
    assert general_diff > online_time_diff, f"""general_diff must not be lower than online_time_diff.
                        {general_diff} vs {online_time_diff} Check passed arguments"""
            

    
    # перевод аргументов в секунды для работы с unix time
    offline_time_diff= offline_time_diff * 60
    online_time_diff= online_time_diff * 60
    online_ceil= online_ceil * 60
    general_diff = general_diff * 60
    general_ceil = general_ceil * 60

    
    timestamp_unix = timestamp_sample.unix_time.iloc[0]
    # Копия, чтобы не внести изменения в исходный датафрейм
    client_txns = client_txns.copy()
    client_txns["abs_time_proximity"] = client_txns.unix_time.sub(timestamp_unix).abs()

    # Оффлайн и онлайн транзакции
    offline_txns = client_txns.loc[client_txns.online == False]
    online_txns = client_txns.loc[client_txns.online == True]
    

    # Записи о ближайших по времени оффлайн и онлайн транзакциях
    closest_txn_offline = offline_txns.loc[offline_txns.abs_time_proximity == offline_txns.abs_time_proximity.min()]
    closest_txn_online = online_txns.loc[online_txns.abs_time_proximity == online_txns.abs_time_proximity.min()]
    
    # Разница семплированного timestamp-а с ближайшей по времени оффлайн транзакцией
    # Если такая есть
    if not closest_txn_offline.empty:
        closest_offline_diff = closest_txn_offline.abs_time_proximity.iloc[0]
        
    # Если нет предыдущийх оффлайн транзакций то назначаем минимальную разницу
    # для оффлайн транзакций, чтобы дальнейшее условие closest_offline_diff < offline_time_diff не исполнилось
    else:
        closest_offline_diff = offline_time_diff

    # Разница семплированного timestamp-а с ближайшей по времени онлайн транзакцией
    # Если такая есть
    if not closest_txn_online.empty:
        closest_online_diff = closest_txn_online.abs_time_proximity.iloc[0]
        
    # Если нет предыдущийх оффлайн транзакций то назначаем минимальную разницу
    # между онлайн и оффлайн транзакциями, чтобы дальнейшее условие closest_offline_diff < general_diff 
    # либо closest_online_diff < online_time_diff не исполнилось
    else:
        closest_online_diff = general_diff
    
    # Запись о последней транзакции
    last_txn = client_txns.loc[client_txns.unix_time == client_txns.unix_time.max()]
    # Онлайн или не онлайн последняя транзакция
    last_online_flag = last_txn.online.iloc[0]
    # unix время
    last_txn_unix = last_txn.unix_time.iloc[0]
    
    # Если, создаваемая транзакция оффлайн
    # И разница с ближайшей оффлайн транзакцией меньше допустимой
    if not online and closest_offline_diff < offline_time_diff:
        close_flag = "offline_to_offline"

    # Если оффлайн транзакция и разница с ближайшей оффлайн допустима, но не допустима с ближайшей онлайн.
    elif not online and closest_online_diff < general_diff:
        close_flag = "offline_to_online"

    # Если онлайн транзакция и разница с ближайшей оффлайн меньше допустимой
    elif online and closest_offline_diff < general_diff:
        close_flag = "online_to_offline"

    # Если онлайн транзакция и разница с ближайшей оффлайн допустима, но с ближайшей онлайн меньше допустимой
    elif online and closest_online_diff < online_time_diff:
        close_flag = "online_to_online"
        

    # Если нет транзакций ближе установленной разницы
    # Просто берем изначальный timestamp
    else:
        close_flag = "No flag"
        txn_unix = timestamp_unix
        txn_time = pd.to_datetime(txn_unix, unit="s")

    # Если транзакция близка по времени к другой, то согласно типам транзакций
    # создаем другое время на основании времени и типа последней и текущей транзакции
    if close_flag in ["offline_to_offline", "offline_to_online"]:
        # Если последняя транзакция Онлайн. То добавляем случайную разницу для онлайн и оффлайн транзакций в установленном диапазоне
        if last_online_flag:
            general_random_diff = random.randint(general_diff, general_ceil)
            txn_unix = last_txn_unix + general_random_diff
            txn_time = pd.to_datetime(txn_unix, unit="s")
            
        # Если последняя транзакция Оффлайн. То добавляем допустимую разницу между оффлайн транзакциями
        elif not last_online_flag:
            txn_unix = last_txn_unix + offline_time_diff
            txn_time = pd.to_datetime(txn_unix, unit="s")

    # Если текущая транзакция онлайн и есть онлайн/оффлайн транзакция с разницей меньше допустимой
    elif close_flag in ["online_to_online", "online_to_offline"]:
        # Если последняя транзакция онлайн. То добавляем случайную разницу для онлайн транзакций в установленном диапазоне
        if last_online_flag:
            online_random_diff = random.randint(online_time_diff, online_ceil)
            txn_unix = last_txn_unix + online_random_diff
            txn_time = pd.to_datetime(txn_unix, unit="s")
            
        # Если последняя транзакция Оффлайн. То добавляем случайную разницу для онлайн и оффлайн транзакций в установленном диапазоне
        elif not last_online_flag:
            general_random_diff = random.randint(general_diff, general_ceil)
            txn_unix = last_txn_unix + general_random_diff
            txn_time = pd.to_datetime(txn_unix, unit="s")

    # Проверка и корректировка времени, на случай если категория дневная, и время выходит за рамки этой категории
    # Если час меньше 8 и больше 21. Т.е. ограничение 08:00-21:59
    if not online and not round_clock and (txn_time.hour < 8 or txn_time.hour > 21):
        txn_time = txn_time + pd.Timedelta(10, unit="h")
        txn_unix = pd_timestamp_to_unix(txn_time)
        
    if not test:
        return txn_time, txn_unix
        
    # В тестовом режиме логируем некоторые данные в csv
    else:
        log_check_min_time(client_id=client_txns.client_id.iloc[0], txn_time=txn_time, txn_unix=txn_unix, online=online, \
                            closest_txn_offline=closest_txn_offline, closest_txn_online=closest_txn_online, last_txn=last_txn, \
                           close_flag=close_flag)
        return txn_time, txn_unix
    

# 18. Подфункция генерации времени для фрод кейсов `fast_geo_change` и `fast_geo_change_online`

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


# 19. Подфункция генерации времени транзакции для правила `trans_freq_increase`
# - несколько частых транзакций подряд

def gen_time_for_frequent_trans(last_txn_time, last_txn_unix, timestamps, time_weights, lag, lag_interval=30, \
                                freq_low=1, freq_high=5, test=False, test_timestamp=None):
    """
    Функция для имитации времени нескольких частых транзакций подряд.
    -------------------------------------------------
    last_txn_time - pd.Timestamp последней транзакции
    last_txn_unix - unix время последней транзакции в секундах
    timestamps - pd.DataFrame с диапазоном pd.Timestamp-ов и их unix аналогом в секундах и колонкой часа timestamp-а
    lag - True или False. Является ли текущая транзакция первой в серии мошеннических/подозрительных учащенных транзакций.
          При True будет добавлен лаг равный lag_interval от предыдущей легальной транзакции
    lag_interval - int. лаг от предыдущей транзакции в минутах
    freq_low - int. минимальный разрыв между транзакциями в цепочке фрод транзакций, в минутах
    freq_high - int. максимальный разрыв между транзакциями в цепочке фрод транзакций, в минутах
    test - True или False. Тестируем мы функцию или нет. Если тестируем, то надо передать test_timestamp
    test_timestamp - str. Дата и время в формате '1899-01-01 00:00:00'. Это замена семплируемуго внутри
                     функции timestamp-а, который проверяется на разницу во времени с последней легальной транзакцией.
    --------------------------------------------------
    При test == False возвращает pd.Timestamp и unix time в секундах
    При test == True возвращает pd.Timestamp, unix time в секундах и получившуюся разницу времени с предыдущей транзакцией в минутах в виде int
    """

    # перевод минут в секунды для удобства подсчета с unix time
    lag_interval = lag_interval * 60
    
    # если транзакция первая в серии фрод транзакций - аргумент lag равен True. И не режим тестирования функции
    if lag and not test:
        
        # Фильтруем timestamp-ы, оставляя те, что после последней легитимной транзакции
        timestamp_after_last_trans = timestamps.loc[timestamps.timestamp > last_txn_time]

        # Семплируем час первой транзакции, передав веса часов.
        # Затем фильтруем timestamp-ы по этому часу
        txn_hour = time_weights.hours.sample(n=1, weights=time_weights.proportion, replace=True).iloc[0]
        timestamps_subset = timestamp_after_last_trans.loc[timestamp_after_last_trans.hour == txn_hour]
        # семплируем таймстемп из таймстемпов по времени не ранее последней транзакции
        timestamp_sample = timestamps_subset.sample(n=1, replace=True)
        # Узнаем разницу между timestamp-ом и последней транзакцией
        trans_time_diff = timestamp_sample.unix_time.sub(last_txn_unix).iloc[0]

    # Если режим тестирования. То timestamp_sample у нас это не семпл, а переданный в аргументе timestamp
    elif lag and test:
        timestamp_sample = pd.DataFrame({"timestamp":pd.Series(dtype="datetime64[s]"), "unix_time":pd.Series(dtype="int64")})
        timestamp_sample.loc[0, "timestamp"] = pd.to_datetime(test_timestamp, format="%Y-%m-%d %H:%M:%S")
        timestamp_sample.loc[0, "unix_time"] = pd_timestamp_to_unix(timestamp_sample.timestamp.iloc[0])
        # Узнаем разницу между timestamp-ом и последней транзакцией
        trans_time_diff = timestamp_sample.unix_time.iloc[0] - last_txn_unix


    # если транзакция первая в серии фрод транзакций
    # и интервал между последней транзакцией менее 30 минут
    # прибавить интервал 30 минут к семплированному времени текущей транзакции
    if lag and trans_time_diff < lag_interval:
        # print("Condition #1")
        time_addition = lag_interval - trans_time_diff
        txn_unix = timestamp_sample.unix_time.iloc[0] + time_addition
        txn_time = pd.to_datetime(txn_unix, unit="s")

    # Если lag равен True, но не надо добавлять интервал
    elif lag:
        # print("Condition #2")
        txn_unix = timestamp_sample.unix_time.iloc[0]
        txn_time = pd.to_datetime(txn_unix, unit="s")

    # для остальных случаев - когда это не первые фрод транзакции в серии
    # просто добавляем 1-5 минут ко времени предыдущей транзакции
    else:
        # частота фрод транзакций. от 1 до 5 минут. Выразим в секундах для удобства расчетов
        freq = random.randint(freq_low, freq_high) * 60
        txn_unix = last_txn_unix + freq
        txn_time = pd.to_datetime(txn_unix, unit="s")

    if not test:
        return txn_time, txn_unix
    
    elif test and lag:
        return txn_time, txn_unix
        
    elif test and not lag:
        return txn_time, txn_unix, freq
    

# Разбитая на подфункции функция `get_time_for_trans`
# Разбивка на `handle_legit_case` и `handle_fraud_case` функции

# 20. Подфункция для работы с временем для легальных транзакций

def handle_legit_case_time(client_txns, timestamp_sample, online, round_clock):
    """
    Проверяет время для легальной транзакции если есть предыдущие транзакции.
    Обеспечивает минимальный интервал от предыдущих транзакций
    --------------------------------------------------
    client_txns - pd.DataFrame. Предыдущие транзакции клиента.
    timestamp_sample - pd.DataFrame. Семплированный timestamp. | timestamp | unix_time | hour |
    online - boolean. Онлайн или оффлайн транзакция
    round_clock - boolean. Круглосуточная или дневная категория.
    --------------------------------------------------
    Возвращает pd.Timestamp и unix время в секундах в виде int 
    """
    
    # Если текущая транзакция - оффлайн.
    if not online:

        # check_min_interval_from_near_trans проверит ближайшие к timestamp_sample по времени транзакции в соответсвии с установленными
        # интервалами и если время до ближайшей транзакции меньше допустимогшо,то создаст другой timestamp
        # Если интервал допустимый, то вернет исходный timestamp
        
        txn_time, txn_unix = check_min_interval_from_near_trans(client_txns=client_txns, timestamp_sample=timestamp_sample, online=online, \
                                                                round_clock=round_clock, offline_time_diff=60,  online_time_diff=6, online_ceil=60, \
                                                                general_diff=30, general_ceil=90)
        return txn_time, txn_unix

    # То же самое, но если текущая транзакция - онлайн
    elif online:

        txn_time, txn_unix = check_min_interval_from_near_trans(client_txns=client_txns, timestamp_sample=timestamp_sample, online=online, \
                                                                round_clock=round_clock, offline_time_diff=60,  online_time_diff=6, online_ceil=60, \
                                                                general_diff=30, general_ceil=90)
        return txn_time, txn_unix
    

# 21. Подфункция для работы с временем для фрод транзакций

def handle_fraud_case_time(last_txn_time, last_txn_unix, rule, timestamps, time_weights, geo_distance):
    """
    Генерирует время, чтобы транзакция попала под антифрод-правило
    --------------------------------------------------------------
    last_txn_time - pd.Timestamp. Время последней транзакции
    last_txn_unix - int. Unix время последней транзакции в секундах
    rule - str. Название антифрод-правила.
    time_weights - pd.DataFrame. Вероятностные веса для паттерна времени.
    geo_distance - int. Кратчайшее расстояние между
    --------------------------------------------------------------
    Возвращает pd.Timestamp и int unix время в секундах
    """
    
    # Правила: другая гео за короткое время либо по локации оффлайн мерчанта либо по новому ip адресу
    if rule in ["fast_geo_change", "fast_geo_change_online"]:
        return generate_time_fast_geo_jump(last_txn_unix, geo_distance, threshold=800)

    
    # Увеличение количества транзакций в единицу времени выше установленного порога в процентах.
    elif rule == "trans_freq_increase":
        return gen_time_for_frequent_trans(last_txn_time, last_txn_unix, timestamps, time_weights, lag, \
                                                         lag_interval=30, freq_low=1, freq_high=5)
    

# 22. Функция `get_time_for_trans`

def get_time_for_trans(trans_df, is_fraud, time_weights, timestamps, timestamps_1st_month, \
                       round_clock, online=None, rule=None, geo_distance=None, lag=None):
    """
    trans_df - датафрейм с транзакциями текущего клиента. Откуда брать информацию по предыдущим транзакциям клиента
    is_fraud - boolean. Фрод или не фрод
    time_weights - датафрейм с весами часов в периоде времени
    timestamps - датафрейм с timestamps
    timestamps_1st_month - сабсет timestamps отфильтрованный по первому месяцу и, если применимо, году. Чтобы генерировать первые транзакции
    round_clock - boolean. Круглосуточная или дневная категория.
    online - boolean. Онлайн или оффлайн покупка. True or False
    rule - str. Название антифрод правила
    geo_distance - int. Дистанция между локацией последней и текущей транзакции если фрод со сменой геолокации - в километрах
    lag - boolean. Задержка по времени от предыдущей транзакции. Нужна для моделирования увеличения частоты транзакций.
          Это задержка именно между последней легитимной транзакцией и серией частых фрод транзакций. Подразумевается что функция
          get_time_for_trans будет использована в цикле, и для первой итерации lag будет True.

    Возвращает время для генерируемой транзакции в виде pd.Timestamp и в виде unix времени
    """
    
    # Время последней транзакции клиента. pd.Timestamp и unix в секундах
    last_txn_time = trans_df.txn_time.max()
    last_txn_unix = trans_df.unix_time.max()
    

    # Если нет никакой предыдущей транзакции т.е. нет последнего времени совсем
    # Подразумевается, что первая транзакция будет легальной.
    if not is_fraud and last_txn_time is pd.NaT:

        # время транзакции в виде timestamp и unix time.
        return sample_time_for_trans(timestamps=timestamps_1st_month, time_weights=time_weights)

    
    # Если есть предыдущая транзакция

    # берем случайный час передав веса часов для соответсвующейго временного паттерна
    txn_hour = time_weights.hours.sample(n=1, weights=time_weights.proportion, replace=True).iloc[0]
    
    # фильтруем по этому часу timestamp-ы и семплируем timestamp уже с равной вероятностью
    # Дальше будем обрабатывать этот timestamp в некоторых случаях
    timestamps_subset = timestamps.loc[timestamps.hour == txn_hour]
    timestamp_sample = timestamps_subset.sample(n=1, replace=True)

    # ЛЕГАЛЬНЫЕ ТРАНЗАКЦИИ. Если есть предыдущие транзакции
    
    if not is_fraud:
        # Время транзакции в виде pd.Timestamp и unix времени в секундах
        return handle_legit_case_time(client_txns=trans_df, timestamp_sample=timestamp_sample, online=online, round_clock=round_clock)

    
    # ФРОД ТРАНЗАКЦИИ 

    # Список правил, требующих специальной обработки времени
    special_time_rules = ["fast_geo_change", "fast_geo_change_online", "trans_freq_increase"]

    # Фрод в рамкам спец правил
    if is_fraud and rule in special_time_rules:
        return handle_fraud_case_time(last_txn_time, last_txn_unix, rule, timestamps, time_weights, geo_distance)

    # Фрод для других правил.
    # Просто возвращаем семплированный timestamp и его unix вариант
    elif is_fraud:
        txn_time = timestamp_sample.timestamp.iloc[0]
        return txn_time, pd_timestamp_to_unix(txn_time)
    

# 23.