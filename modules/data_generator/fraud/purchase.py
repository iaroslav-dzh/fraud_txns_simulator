# Основные функции генераторы фрода: одиночные транзакции, множественные транзакции
import pandas as pd
import numpy as np

from data_generator.utils import sample_category, ConfigForTrans, build_transaction, calc_distance, sample_rule
from data_generator.fraud.txndata import FraudTransPartialData, TransAmount
from data_generator.fraud.time import get_time_fraud_txn


# Функция генерации одной фрод транзакции с типом "purchase"

def gen_purchase_fraud_txn(rule, client_info, client_trans_df, configs: ConfigForTrans, trans_partial_data: FraudTransPartialData, \
                       fraud_amts: TransAmount, all_time_weights, trans_num=0, lag=False):
    """
    Генерация одной фрод транзакции для клиента
    ------------------------------------------------
    rule - str.
    client_info - namedtuple, полученная в результате итерации с помощью .itertuples() через датафрейм с информацией о клиентах
    client_trans_df - датафрейм с транзакциями клиента.
    configs - ConfigForTrans dataclass. 
    client_device_ids - pd.Series. id девайсов клиента.
    trans_partial_data - FraudTransPartialData class.
    fraud_amts - TransAmount class.
    all_time_weights - dict. Датафреймы с весами времени для всех временных паттернов.
    trans_num - int. Какая по счету транзакция в данном фрод кейсе.
    lag - bool. Нужен ли лаг по времени от последней легальной транзакции. Используется для trans_freq_increase
    -------------------------------------------------
    Возвращает словарь с готовой транзакцией
    """

    is_fraud = True
    
    # Запись о последней транзакции клиента
    last_txn = client_trans_df.loc[client_trans_df.unix_time == client_trans_df.unix_time.max()]
    
    # Записываем данные клиента в переменные
    client_id = client_info.client_id

    # Берем значение online флага для выбранного правила
    online = configs.rules.loc[configs.rules.rule == rule, "online"].iat[0]
    
    # Семплирование категории. У категорий свой вес в разрезе вероятности быть фродом
    category = sample_category(configs.categories, online=online, is_fraud=is_fraud)
    
    category_name = category["category"].iat[0]
    round_clock = category["round_clock"].iat[0]
    
    # Генерация суммы транзакции. 
    # Пока что для всех правил кроме trans_freq_increase генерация через один и тот же метод
    if rule == "trans_freq_increase":
        amount = fraud_amts.freq_trans_amount(low=2000, high=10000, mean=4000, std=1500)
    else:
        amount = fraud_amts.fraud_amount(category_name=category_name)
    

    # Данные о мерчанте, геопозиции, IP, девайсе
    # Правило: быстрая смена гео. Оффлайн/онлайн
    if rule in ["fast_geo_change", "fast_geo_change_online"]:
        merchant_id, trans_lat, trans_lon, trans_ip, trans_city, device_id, channel, type = \
            trans_partial_data.another_city(client_city=client_info.area, online=online, category_name=category_name)

    elif rule in ["new_ip_and_device_high_amount"]:
        merchant_id, trans_lat, trans_lon, trans_ip, trans_city, device_id, channel, type = \
            trans_partial_data.new_device_and_ip(client_city=client_info.area, online=online, \
                                            category_name=category_name, another_city=True)

    elif rule == "new_device_and_high_amount":
        merchant_id, trans_lat, trans_lon, trans_ip, trans_city, device_id, channel, type = \
            trans_partial_data.new_device_and_ip(client_city=client_info.area, online=online, \
                                            category_name=category_name, another_city=False)

    # Если это первая транзакция под правило trans_freq_increase
    # То вызываем метод freq_trans для генерации части данных транзакции
    # Они не изменятся в дальнейших транзакциях из серии. И поэтому в следующих транзакциях
    # Будем брать кэшированный результат записанный в trans_partial_data.last_txn
    elif rule == "trans_freq_increase" and trans_num == 1:
        # В данном случае получаем также и статус транзакции кроме остальных данных.
        # Зависит от того, какая это транзакция по счету из серии частых транзакций
        merchant_id, trans_lat, trans_lon, trans_ip, trans_city, device_id, channel, type = \
            trans_partial_data.freq_trans(client_city=client_info.area, \
                                            category_name=category_name, another_city=True)

    # Транзакция не первая в серии. Берем кэшированные данные созданные для первой транзакции.
    elif rule == "trans_freq_increase":
        merchant_id, trans_lat, trans_lon, trans_ip, trans_city, device_id, channel, type = \
            trans_partial_data.last_txn


    # Физическое расстояние между координатами последней транзакции и координатами текущей.
    # geo_distance = calc_distance(client_trans_df=client_trans_df, trans_lat=trans_lat, trans_lon=trans_lon)
    geo_distance = calc_distance(lat_01=last_txn.trans_lat.iat[0], lon_01=last_txn.trans_lon.iat[0], \
                                 lat_02=trans_lat, lon_02=trans_lon)
    
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
    
    # Генерация времени транзакции
    txn_time, txn_unix = get_time_fraud_txn(trans_df=client_trans_df, time_weights=time_weights, timestamps=configs.timestamps, \
                                            round_clock=round_clock, online=online, rule=rule, geo_distance=geo_distance, lag=lag)
    
    
    # Только для freq_trans статус может отличаться от declined.
    if rule == "trans_freq_increase" and (trans_num > 0 and trans_num < 4):
        status = "approved"
    else:
        status = "declined"
        
    # Статичные значения для данной функции
    is_suspicious = False
    account = pd.NA
    
    # Возвращаем словарь со всеми данными сгенерированной транзакции
    return build_transaction(client_id=client_id, txn_time=txn_time, txn_unix=txn_unix, amount=amount, type=type, channel=channel, \
                             category_name=category_name, online=online, merchant_id=merchant_id, trans_city=trans_city, \
                             trans_lat=trans_lat, trans_lon=trans_lon, trans_ip=trans_ip, device_id=device_id, account=account, \
                             is_fraud=is_fraud, is_suspicious=is_suspicious, status=status, rule=rule)


# . Обертка для gen_purchase_fraud_txn под правило trans_freq_increase

def trans_freq_wrapper(client_info, client_txns_temp, txns_total, configs: ConfigForTrans, \
                       trans_partial_data: FraudTransPartialData, fraud_amts: TransAmount, \
                       all_time_weights):
    """
    Генерирует указанное число частых фрод транзакций под правило trans_freq_increase
    -----------------------------
    Возвращает pd.DataFrame с фрод транзакциями
    -----------------------------
    client_info - namedtuple. Запись из датафрейма с информацией о клиенте, полученная при итерировании через .itertuples()
    client_txns_temp - pd.DataFrame. Запись о последней транзакции клиента.
    txns_total - int. Сколько транзакции должно быть сгенерировано.
    configs - ConfigForTrans. Конфиги для транзакций.
    trans_partial_data: FraudTransPartialData. Генератор части данных транзакций.
    fraud_amts: TransAmount. Генератор сумм транзакций.
    all_time_weights - dict. Датафреймы с весами времени для всех временных паттернов.
    """
    for trans_num in range(1, txns_total + 1):
        # print(f"trans_num: {trans_num}")
        if trans_num == 1:
            lag = True
        else:
            lag = False
            
        one_txn = gen_purchase_fraud_txn(rule="trans_freq_increase", client_info=client_info, client_trans_df=client_txns_temp, \
                                    configs=configs, trans_partial_data=trans_partial_data, \
                                       fraud_amts=fraud_amts, all_time_weights=all_time_weights, \
                                         trans_num=trans_num, lag=lag)
        
        client_txns_temp = pd.concat([client_txns_temp, pd.DataFrame([one_txn])])

    # Исключаем последню легальную транзакцию для добавления сгенеренных фрод транзакций в общий список
    return client_txns_temp.loc[client_txns_temp.unix_time != client_txns_temp.unix_time.min()]


# Функция генерации нескольких фрод транзакций

def gen_multi_fraud_trans(configs: ConfigForTrans, trans_partial_data: FraudTransPartialData, \
                         fraud_amts: TransAmount, all_time_weights):
    """
    clients_subset - pd.DataFrame. Клиенты у которых будут фрод транзакции. Сабсет клиентов для кого нагенерили
                     легальных транзакций ранее.
    configs - ConfigForTrans. Конфиги для транзакций.
    trans_partial_data: FraudTransPartialData. Генератор части данных транзакций.
    fraud_amts: TransAmount. Генератор сумм транзакций.
    all_time_weights - dict. Датафреймы с весами времени для всех временных паттернов. 
    """
    
    all_fraud_txns = []
    
    for client in configs.clients.itertuples():
        # Запись клиента как у того, у кого был фрод. 
        # Чтобы не повторяться если генерация будет разбита на несколько повторов.
        # clients_with_fraud.loc[clients_with_fraud.shape] = client.client_id
    
        rule = sample_rule(configs.rules)
        client_txns = configs.transactions.loc[configs.transactions.client_id == client.client_id]
        # Записываем данные текущего клиента в атрибут client_info класса FraudTransPartialData
        trans_partial_data.client_info = client
        
 
        if rule == "trans_freq_increase":
            client_txns_temp = client_txns.loc[[client_txns.unix_time.idxmax()]]
            txns_total = np.random.randint(4,9)

            # Генерируем txns_total число фрод транзакций. Датафрейм с ними записываем в переменную
            fraud_only = trans_freq_wrapper(client_info=client, client_txns_temp=client_txns_temp, \
                                            txns_total=txns_total, configs=configs, \
                                            trans_partial_data=trans_partial_data, fraud_amts=fraud_amts, \
                                            all_time_weights=all_time_weights)
            
            # Добавляем созданные транзакции в общий список и сразу переводим цикл на следующую итерацию
            all_fraud_txns.append(fraud_only)
            continue

        else:
            one_txn = gen_purchase_fraud_txn(rule=rule, client_info=client, client_trans_df=client_txns, \
                                            configs=configs, trans_partial_data=trans_partial_data, fraud_amts=fraud_amts, \
                                            all_time_weights=all_time_weights)
               
            all_fraud_txns.append(pd.DataFrame([one_txn]))
        
    return pd.concat(all_fraud_txns, ignore_index=True)


