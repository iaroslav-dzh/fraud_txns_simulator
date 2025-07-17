import numpy as np
import pandas as pd
import os


def log_check_min_time(client_id, txn_time, txn_unix, online, closest_txn_offline, \
                       closest_txn_online, last_txn, close_flag):
    """
    Логирует нужные данные для дебаггинга
    -------------------------------
    closest_txn_offline - pd.DataFrame. Запись о ближайшей оффлайн транзакции - может быть пустым
    closest_txn_online - pd.DataFrame. Запись о ближайшей онлайн транзакции - может быть пустым
    last_txn - pd.DataFrame. Запись о последней транзакции
    close_flag - bool. online значение ближайшей транзакции.
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


# 2.

def set_close_flag(online, closest_offline_diff, closest_online_diff, min_inter):
    """
    Определение отношения текущей транз-ции с транз-цией в контексте online
    флага если время между ними меньше допустимого. Для функции
    check_min_interval_from_near_txn
    ---------------
    online: bool. Онлайн текущая транзакция или оффлайн
    closest_offline_diff: int. Секунды до ближайшей оффлайн транзакции если такая имеется.
    closest_online_diff: int. Секунды до ближайшей онлайн транзакции если такая имеется.
    min_inter: dict. Лимиты минимального времени между транз-циями в зависимости от типа
    """
    offline_time_diff = min_inter["offline_time_diff"] # Мин. время между оффлайн
    online_time_diff = min_inter["online_time_diff"] # Мин. время между онлайн
    general_diff = min_inter["general_diff"] # Мин. время между оффлайн и онлайн
    
    # Если, создаваемая транзакция оффлайн
    # И разница с ближайшей оффлайн транзакцией меньше допустимой
    if not online and closest_offline_diff < offline_time_diff:
        return "offline_to_offline"

    # Если оффлайн транзакция и разница с ближайшей оффлайн допустима, но не допустима с ближайшей онлайн.
    elif not online and closest_online_diff < general_diff:
        return "offline_to_online"

    # Если онлайн транзакция и разница с ближайшей оффлайн меньше допустимой
    elif online and closest_offline_diff < general_diff:
        return "online_to_offline"

    # Если онлайн транзакция и разница с ближайшей оффлайн допустима, но с ближайшей онлайн меньше допустимой
    elif online and closest_online_diff < online_time_diff:
        return "online_to_online"

    else:
        return "No flag" 
    

