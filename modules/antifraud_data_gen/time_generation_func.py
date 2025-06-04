# Функции по проекту antifraud_01. Этап генерации данных. Функции из раздела генерации времени

# 1. Функция генерации весов для часов в периоде времени

def gen_weights_for_time(is_fraud=False, round_clock=False, online=False):
    """
    возвращает датафрейм с часами от 0 до 23 и из весами,
    название паттерна в виде строки и цвет для возможного графика - в зависимости от фрод не фрод
    
    is_fraud - True или False. По умолчанию False
    round_clock - Круглосуточная категория или нет. True или False. По умолчанию False
    online - None, True или False. По умолчанию False
    """
    # Время в минутах с начала суток. Чтобы ограничить распределения
    start, end = 0, 1439  # 00:00 до 23:59

    #  Далее в зависимости от условий генерация распределения времени для транзакций
    
    # 1. Категория - круглосуточные, оффлайн, НЕ фрод 
    if not is_fraud and round_clock and not online:

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

    # 2. оффлайн фрод. круглосуточные категории
    elif is_fraud and round_clock and not online:

        # название паттерна. Пригодится если нужно построить график распределения
        title = "Offline. 24h. Fraud"
        
        # равномерная активность с 8 до 23
        day_time =  np.array([np.random.uniform(8, 23.9) for _ in range(500)]).astype(int)
        
        # равномерная сниженная активность с 0 до 8
        night_time = np.array([np.random.uniform(0, 7.9) for _ in range(120)]).astype(int)
        
        # соединяем все созданные массивы в один и делаем серией
        time_hours = pd.Series(np.concatenate((day_time, night_time), axis=0), name="hours")


    # 3. НЕ фрод. Онлайн покупки
    elif not is_fraud and round_clock and online:

        # название паттерна. Пригодится если нужно построить график распределения
        title = "Online. Legit"
        
        # равномерно с утра до вечера. с 8:00 до 16:59
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
        
    # 4. онлайн фрод
    elif is_fraud and round_clock and online:

        # название паттерна. Пригодится если нужно построить график распределения
        title = "Online. Fraud"
        
        # Ночной пик после 00:00
        mean = 1
        std = 120
        
        # распределение ночного пика
        dist = truncnorm((start - mean) / std, (end - mean) / std, loc=mean, scale=std)
        minutes = dist.rvs(2000).astype(int)
        
        # Добавление веса для вечернего периода (среднее 23:00 - 1140 минут)
        mean_evn = 23.9*60
        std_evn = 120
        evn_end = 23.9*60
        
        # Добавляем вечернюю активность - обрезка справа в 0 часов. Ограничиваем значения 00:00 часами справа
        dist_evn = truncnorm((start - mean_evn) / std_evn, (evn_end - mean_evn) / std_evn, loc=mean_evn, scale=std_evn)
        minutes_evn = dist_evn.rvs(2000).astype(int)
        
        # добавим небольшое количество равномерных значений на протяжении дня, с 4 до 20 включительно
        mid_start = 4 * 60
        mid_end = 20.9 * 60
        midday_add =  np.array([np.random.uniform(mid_start, mid_end) for _ in range(4250)]).astype(int)
        
        # соединяем все три массива
        total_minutes = np.concatenate((minutes, midday_add, minutes_evn))
        
        times = pd.Series(pd.to_timedelta(total_minutes, unit="min")).dt.components
        time_hours = times.hours

    # 5. Не круглосуточный. Не фрод. Оффлайн
    elif not is_fraud and not round_clock and not online:

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

    # 6. Фрод. Не круглосуточный. Оффлайн. 
    elif is_fraud and not round_clock and not online:

        # название паттерна. Пригодится если нужно построить график распределения
        title = "Offline. Day-only. Fraud"

        # равномерная активность с 8:00 до 22:00
        day_time =  np.array([np.random.uniform(8, 21.9) for _ in range(500)]).astype(int)
        
        # переведем массив в серию
        time_hours = pd.Series(day_time, name="hours")
        
    
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


# 2. Функция генерации весов для всех паттернов времени и сбора их в словарь
# содержит в себе функцию gen_weights_for_time()
# Нужна для генерации всех паттернов один раз, чтобы дальше не вызывать функцию генерации каждого паттерна каждый раз для транзакции либо по отдельности записывать в переменные

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

# 3. Функция построения графика распределения весов времени. Для одиночного графика

def plot_time_weights(weights, title, color, ax):
    sns.barplot(x=weights.hours, y=weights.proportion, color=color, ax=ax)
    ax.set_xlim(-0.5, 23.5)
    ax.set_ylim(0, 0.4)
    ax.grid(axis="y")
    ax.set_title(title)

# 4.  Функция построения графиков распределений весов времени. Для нескольких графиков. Принимает словарь на вход.

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
 

 # 5. Функция генерации времени в виде timestamp-а и unix времени. Выдает и то и другое одновременно в виде двух значений

 def get_time_for_trans(trans_df, client_id, is_fraud, time_weights, timestamps, online=None, rule=None, geo_distance=None, lag=None):
    """
    trans_df - датафрейм с транзакциями. Откуда брать информацию по предыдущим транзакциям
    client_id - id клиента, число, клиент чьи транзакции проверяются
    is_fraud - boolean. Фрод или не фрод
    time_weights - датафрейм с весами часов в периоде времени
    timestamps - датафрейм с timestamps
    online - boolean. Онлайн или оффлайн покупка. True or False
    rule - строка. Название антифрод правила
    geo_distance - число. Дистанция между локацией последней и текущей транзакции если фрод со сменой геолокации - в метрах
    lag - boolean. Задержка по времени от предыдущей транзакции. Нужна для моделирования увеличения частоты транзакций.
          Это задержка именно между последней легитимной транзакцией и серией частых транзакций. Подразумевается что функция
          get_time_for_trans будет использована в цикле, и для первой итерации lag будет True.

    Возвращает время транзакции в виде timestamp и в виде целого числа unix времени 
    """
    
    # timestamp последней транзакции клиента - может быть вынести фильтрацию по client_id из функции?
    # Хотя наверное и вне функции все равно надо будет каждый раз обновлять состояние перед генерацией новой транзакции
    
    last_txn_time = trans_df[trans_df.client_id == client_id].time.max()
    last_txn_unix = trans_df[trans_df.client_id == client_id].unix_time.max()
    
    # Если нет предыдущей транзакции т.е. нет последнего времени
    if not is_fraud and last_txn_time is pd.NaT:
        # берем первый год всего времененного периода т.к. это первая транзакция, пусть будет создана в первом году
        timestamps_1st_year = timestamps.loc[timestamps.timestamp.dt.year == timestamps.timestamp.dt.year.min()]

        # семплируем час из весов времени, указав веса для семплирования
        txn_hour = time_weights.hours.sample(n=1, weights=time_weights.proportion, replace=True).iloc[0]
        # фильтруем основной датафрейм с диапазоном таймстемпов по этому часу
        timestamps_subset = timestamps_1st_year.loc[timestamps_1st_year.hour == txn_hour]
        # из отфильтрованного датафрейма таймстемпов семплируем один таймстемп с равной вероятностью
        txn_time = timestamps_subset.timestamp.sample(n=1, replace=True).iloc[0]
        txn_unix = (txn_time - pd.Timestamp("1970-01-01")) // pd.Timedelta('1s')

    # Если есть предыдущая транзакция

    # Не фрод. Но есть предыдущая транзакция
    elif not is_fraud:

        txn_hour = time_weights.hours.sample(n=1, weights=time_weights.proportion, replace=True).iloc[0]
        timestamps_subset = timestamps.loc[timestamps.hour == txn_hour]
        timestamp_sample = timestamps_subset.sample(n=1, replace=True)
        trans_time_diff = timestamp_sample.unix_time - last_txn_unix
        pos_one_hour_diff = 3600
        neg_one_hour_diff = -3600
        
        # если время между текущей и последней транзакцией меньше часа в положительную сторону,
        # то увеличим время чтобы разница была минимум час 
        if trans_time_diff < pos_one_hour_diff and trans_time_diff >= 0:
            time_addition = pos_one_hour_diff - trans_time_diff
            txn_unix = txn_unix + time_addition
            txn_time = pd.to_datetime(txn_unix, unit="s")
        
        # если время между текущей и последней транзакцией меньше часа в отрицательную сторону, 
        # то уменьшим время чтобы разница была минимум час
        elif trans_time_diff > neg_one_hour_diff and trans_time_diff < 0:
            time_subtraction = neg_one_hour_diff - trans_time_diff
            txn_unix = txn_unix + time_subtraction
            txn_time = pd.to_datetime(txn_unix, unit="s")

    # Фрод. Правила: другая гео за короткое время либо по локации оффлайн мерчанта либо по новому ip адресу
    elif is_fraud and rule in ["fast_geo_change", "fast_geo_change_online"]:
        # Выставим порог скорости перемещения между точками транзакций - м/с
        # выше порога - детект как фрода. 
        # Это нужно чтобы генерировать соответствующее время в зависимости от дистанции между точками транзакций
        # пусть будет порог в 800 км/ч. Делим на 3.6 для перевода в м/с
        speed_threshold = 800 / 3.6
        
        # Случайно сгенерированная фактическая скорость превышающая легитимный порог. Допустим от 801 до 36000 км/ч
        # до 36000 км/ч т.к. грубо говоря транзакция может быть совершена через 20 минут в 9000 км от предыдущей т.е. как-будто бы скорость 36000 км/ч
        # 9000 км взяты как весьма примерное, самое длинное возможное расстояние между городами России при путешествии самолетом.
        # но в зависимости от расстояния мы берем разные границы для распределений, чтобы не было перекоса в очень быстрое время. 
        # Также 20 минут я случайно взял как средний интервал для подобной фрод транзакции.
        # Конечно же "скорость перемещения" может быть и больше в реальной жизни
        
        if geo_distance < 1000_000:
            fact_speed = random.randint(speed_threshold + 1, 3000) / 3.6
        elif geo_distance >= 1000_000 and geo_distance <= 3000_000:
            fact_speed = random.randint(speed_threshold + 1, 9000) / 3.6
        elif geo_distance > 3000_000 and geo_distance <= 6000_000:
            fact_speed = random.randint(speed_threshold + 1, 18000) / 3.6
        else:
            fact_speed = random.randint(speed_threshold + 1, 36000) / 3.6

        # интервал времени между последней транзакцией и текущей фрод транзакцией
        time_interval = geo_distance / fact_speed
        
        txn_unix = last_txn_unix + time_interval
        txn_time = pd.to_datetime(txn_unix, unit="s")

    # Фрод. Увеличение количества транзакций в единицу времени выше установленного порога в процентах.
    # генерируем время
    elif is_fraud and rule == "trans_freq_increase":
        # семплируем таймстемп из таймстемпов по времени не ранее последней транзакции
        timestamp_sample = timestamps.loc[timestamps.timestamp > last_txn_time].sample(n=1, replace=True)
        trans_time_diff = timestamp_sample.unix_time - last_txn_unix
        lag_interval = 1800

        # частота фрод транзакций. от 1 до 5 минут. Выразим в секундах для удобства
        freq = random.randint(1, 5) * 60
        
        # если транзакция первая в серии фрод транзакций - аргумент lag=True
        # и интервал между последней транзакцией менее 30 минут
        # прибавить интервал 30 минут к семплированному времени текущей транзакции
        if lag and trans_time_diff < lag_interval:
            txn_unix = timestamp_sample.unix_time + lag_interval
            txn_time = pd.to_datetime(txn_unix, unit="s")

        # Если lag=True, но не надо добавлять интервал
        elif lag:
            txn_unix = timestamp_sample.unix_time
            txn_time = pd.to_datetime(txn_unix, unit="s")

        # для остальных случаев - когда это не первые фрод транзакции в серии
        else:
            txn_unix = last_txn_unix + freq
            txn_time = pd.to_datetime(txn_unix, unit="s")

    # Любой другой фрод
    elif is_fraud:

        txn_hour = time_weights.hours.sample(n=1, weights=time_weights.proportion, replace=True).iloc[0]
        timestamps_subset = timestamps.loc[timestamps.hour == txn_hour]
        timestamp_sample = timestamps_subset.sample(n=1, replace=True)

        txn_unix = timestamp_sample.unix_time
        txn_time = timestamp_sample.time

    
    # время транзакции в виде timestamp и unix time
    return txn_time, txn_unix
    