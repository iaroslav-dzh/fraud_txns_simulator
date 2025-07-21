# Основные функции генераторы фрода: одиночные транзакции, множественные транзакции
import pandas as pd
import numpy as np

from data_generator.utils import sample_category, build_transaction, \
                                 calc_distance, sample_rule
from data_generator.configs import ComprClientFraudCfg
from data_generator.fraud.compr.txndata import FraudTxnPartData, TransAmount
from data_generator.fraud.compr.time import get_time_fraud_txn
from data_generator.fraud.recorder import FraudTxnsRecorder


# Функция генерации одной фрод транзакции с типом "purchase"

def gen_purchase_fraud_txn(rule, client_trans_df, configs: ComprClientFraudCfg, \
                           part_data: FraudTxnPartData, fraud_amts: TransAmount, \
                           txn_num=0, lag=False):
    """
    Генерация одной фрод транзакции для клиента
    ------------------------------------------------
    rule - str.
    client_trans_df - датафрейм с транзакциями клиента.
    configs - ComprClientFraudCfg. Конфиги и данные для генерации фрод транзакций.
    client_device_ids - pd.Series. id девайсов клиента.
    part_data - FraudTxnPartData.
    fraud_amts - TransAmount class.
    txn_num - int. Какая по счету транзакция в данном фрод кейсе.
    lag - bool. Нужен ли лаг по времени от последней легальной транзакции.
          Используется для trans_freq_increase
    -------------------------------------------------
    Возвращает словарь с готовой транзакцией
    """
    client_info = part_data.client_info
    rules_cfg = configs.rules_cfg
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
        amount = fraud_amts.freq_trans_amount()
    else:
        amount = fraud_amts.fraud_amount(category_name=category_name)
    
    partial_data = part_data.get_data(rule=rule, online=online, category_name=category_name, \
                                      txn_num=txn_num)
    # Распаковка кортежа в переменные
    merchant_id, trans_lat, trans_lon, trans_ip, trans_city, device_id, channel, txn_type = partial_data
    
    # Физическое расстояние между координатами последней транзакции и координатами текущей.
    # geo_distance = calc_distance(client_trans_df=client_trans_df, trans_lat=trans_lat, trans_lon=trans_lon)
    geo_distance = calc_distance(lat_01=last_txn.trans_lat.iat[0], lon_01=last_txn.trans_lon.iat[0], \
                                 lat_02=trans_lat, lon_02=trans_lon)
    
    txn_time, txn_unix = get_time_fraud_txn(trans_df=client_trans_df, configs=configs, online=online, \
                                            round_clock=round_clock, rule=rule, geo_distance=geo_distance, \
                                            lag=lag)
    
    # Только для freq_trans статус может отличаться от declined.
    # При кол-ве до freq_min - approved. Условно, детект по этому правилу начинается
    # с freq_min транз-ций
    freq_min = rules_cfg["freq_txn"]["txn_num"]["min"]
    if rule == "trans_freq_increase" and (txn_num > 0 and txn_num < freq_min):
        rule_to_txn = "not applicable"
        status = "approved"

    elif rule == "trans_freq_increase":
        rule_to_txn = "trans_freq_increase"
        status = "declined"

    else:
        rule_to_txn = rule
        status = "declined"
        
    # Статичные значения для данной функции
    is_suspicious = False
    account = np.nan
    
    # Возвращаем словарь со всеми данными сгенерированной транзакции
    return build_transaction(client_id=client_id, txn_time=txn_time, txn_unix=txn_unix, amount=amount, \
                             txn_type=txn_type,  channel=channel, category_name=category_name, online=online, \
                             merchant_id=merchant_id, trans_city=trans_city, trans_lat=trans_lat, \
                             trans_lon=trans_lon, trans_ip=trans_ip, device_id=device_id, account=account, \
                             is_fraud=is_fraud, is_suspicious=is_suspicious, status=status, rule=rule_to_txn)


# . Обертка для gen_purchase_fraud_txn под правило trans_freq_increase

def trans_freq_wrapper(client_txns_temp, txns_total, configs: ComprClientFraudCfg, \
                       part_data: FraudTxnPartData, fraud_amts: TransAmount):
    """
    Генерирует указанное число частых фрод транзакций под правило trans_freq_increase
    -----------------------------
    Возвращает pd.DataFrame с фрод транзакциями
    -----------------------------
    client_info - namedtuple. Запись из датафрейма с информацией о клиенте, полученная при итерировании через .itertuples()
    client_txns_temp - pd.DataFrame. Запись о последней транзакции клиента.
    txns_total - int. Сколько транзакции должно быть сгенерировано.
    configs - ComprClientFraudCfg. Конфиги для транзакций.
    part_data: FraudTxnPartData. Генератор части данных транзакций.
    fraud_amts: TransAmount. Генератор сумм транзакций.
    all_time_weights - dict. Датафреймы с весами времени для всех временных паттернов.
    """

    for txn_num in range(1, txns_total + 1):
        if txn_num == 1:
            lag = True
        else:
            lag = False

        one_txn = gen_purchase_fraud_txn(rule="trans_freq_increase", client_trans_df=client_txns_temp, \
                                         configs=configs, part_data=part_data, fraud_amts=fraud_amts, \
                                         txn_num=txn_num, lag=lag)
        
        client_txns_temp = pd.concat([client_txns_temp, pd.DataFrame([one_txn])])

    # Исключаем последню легальную транзакцию для добавления сгенеренных фрод транзакций в общий список
    return client_txns_temp.loc[client_txns_temp.unix_time != client_txns_temp.unix_time.min()]


# Функция генерации нескольких фрод транзакций

def gen_multi_fraud_txns(configs: ComprClientFraudCfg, part_data: FraudTxnPartData, \
                         fraud_amts: TransAmount, txn_recorder: FraudTxnsRecorder):
    """
    clients_subset - pd.DataFrame. Клиенты у которых будут фрод транзакции. Сабсет клиентов для кого нагенерили
                     легальных транзакций ранее.
    configs - ComprClientFraudCfg. Конфиги для транзакций.
    part_data: FraudTxnPartData. Генератор части данных транзакций.
    fraud_amts: TransAmount. Генератор сумм транзакций.
    txn_recorder: FraudTxnsRecorder. 
    """
    all_fraud_txns = []
    # Конфиги кол-ва транз. для правила trans_freq_increase
    freq_cfg = configs.rules_cfg["freq_txn"]["txn_num"]
    
    # Создать директорию под текущую генерацию
    # txn_recorder.make_dir()

    for client in configs.clients.itertuples():
        rule = sample_rule(configs.rules)

        client_txns = configs.transactions.loc[configs.transactions.client_id == client.client_id]
        # Записываем данные текущего клиента в атрибут client_info класса FraudTxnPartData
        part_data.client_info = client
        
        # Это правило отдельно т.к. такой случай имеет несколько транз-ций
        if rule == "trans_freq_increase":
            client_txns_temp = client_txns.loc[[client_txns.unix_time.idxmax()]]
            # Сколько транз. будет создано под это правило
            low = freq_cfg["min"]
            high = freq_cfg["max"]
            txns_total = np.random.randint(low, high + 1) 

            # Генерируем txns_total число фрод транзакций. Датафрейм с ними записываем в переменную
            fraud_only = trans_freq_wrapper(client_txns_temp=client_txns_temp, \
                                            txns_total=txns_total, configs=configs, \
                                            part_data=part_data, fraud_amts=fraud_amts)
            
            # Добавляем созданные транзакции в общий список и сразу переводим цикл на следующую итерацию
            all_fraud_txns.append(fraud_only)
            continue

        # Остальные правила. Генерация одной транз-ции
        else:
            one_txn = gen_purchase_fraud_txn(rule=rule, client_trans_df=client_txns, \
                                             configs=configs, part_data=part_data, \
                                             fraud_amts=fraud_amts)
               
            all_fraud_txns.append(pd.DataFrame([one_txn]))
        
    txn_recorder.all_txns = pd.concat(all_fraud_txns, ignore_index=True)
    txn_recorder.write_to_file()


