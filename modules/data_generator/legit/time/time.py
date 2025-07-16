# Генерация времени легальных транзакций
import random
import pandas as pd

from data_generator.legit.time.utils import log_check_min_time
from data_generator.general_time import pd_timestamp_to_unix, sample_time_for_trans

# 1.

def check_min_interval_from_near_txn(client_txns, timestamp_sample, online, round_clock, legit_cfg, test=False):
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
    legit_cfg - dict. Конфиги легальных транзакция из legit.yaml           
    test - boolean. True - логировать исполнение функции в csv.
    ------------------------------------------------
    Возвращает pd.Timestamp и int unix время в секундах 
    """
    min_inter = legit_cfg["time"]["min_intervals"]
    offline_time_diff = min_inter["offline_time_diff"]
    online_time_diff = min_inter["online_time_diff"]
    online_ceil = min_inter["online_ceil"]
    general_diff = min_inter["general_diff"]
    general_ceil = min_inter["general_ceil"]

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
    

# 2.

def get_legit_txn_time(trans_df, time_weights, timestamps, timestamps_1st_month, \
                       legit_cfg, round_clock, online=None):
    """
    Генерация времени для легальной транзакции
    ------------------------------------------
    trans_df: pd.DataFrame. Транзакции текущего клиента. Откуда брать информацию по предыдущим транзакциям клиента
    time_weights: pd.DataFrame. Веса часов в периоде времени
    timestamps: pd.DataFrame. timestamps для генерации времени.
    timestamps_1st_month: pd.DataFrame. сабсет timestamps отфильтрованный по первому месяцу и, 
                          если применимо, году. Чтобы генерировать первые транзакции.
    legit_cfg: dict. Конфиги легальных транзакция из legit.yaml  
    round_clock: bool. Круглосуточная или дневная категория.
    online: bool. Онлайн или оффлайн покупка. True or False
    -------------------------------------------
    Возвращает время для генерируемой транзакции в виде pd.Timestamp и в виде unix времени
    """
    
    # Время последней транзакции клиента. pd.Timestamp
    last_txn_time = trans_df.txn_time.max()
    
    # Если нет никакой предыдущей транзакции т.е. нет последнего времени совсем
    if last_txn_time is pd.NaT:
        # время транзакции в виде timestamp и unix time.
        return sample_time_for_trans(timestamps=timestamps_1st_month, time_weights=time_weights)

    # Если есть предыдущая транзакция

    # берем случайный час передав веса часов для соответсвующейго временного паттерна
    txn_hour = time_weights.hours.sample(n=1, weights=time_weights.proportion, replace=True).iloc[0]
    
    # фильтруем по этому часу timestamp-ы и семплируем timestamp уже с равной вероятностью
    # Дальше будем обрабатывать этот timestamp в некоторых случаях
    timestamps_subset = timestamps.loc[timestamps.hour == txn_hour]
    timestamp_sample = timestamps_subset.sample(n=1, replace=True)

    # Если текущая транзакция - оффлайн.
    if not online:
        # check_min_interval_from_near_txn проверит ближайшие к timestamp_sample по времени транзакции в соответствии с установленными
        # интервалами и если время до ближайшей транзакции меньше допустимогшо, то создаст другой timestamp
        # Если интервал допустимый, то вернет исходный timestamp
        txn_time, txn_unix = check_min_interval_from_near_txn(client_txns=trans_df, timestamp_sample=timestamp_sample, online=online, \
                                                                round_clock=round_clock, legit_cfg=legit_cfg)
        return txn_time, txn_unix

    # То же самое, но если текущая транзакция - онлайн
    elif online:
        txn_time, txn_unix = check_min_interval_from_near_txn(client_txns=trans_df, timestamp_sample=timestamp_sample, online=online, \
                                                                round_clock=round_clock, legit_cfg=legit_cfg)
        return txn_time, txn_unix