# Конфигурационные структуры: датаклассы под конфиги и данные для генерации транзакций.

import pandas as pd
from dataclasses import dataclass


# 1. Датакласс под конфиги фрода в покупках, когда аккаунт или карта клиента скомпрометированы
# Compromised Purchase Fraud
# Это данные на основе которых будут генерироваться транзакции

@dataclass
class CompPurchFraudCfg:
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


# 2. Датакласс для конфигов транзакций дропов-распределителей

@dataclass
class DropDistributorCfg:
    """
    Это данные на основе которых будут генерироваться транзакции дропов-распределителей
    ---------------------
    clients: pd.DataFrame
    timestamps: pd.DataFrame
    accounts: pd.DataFrame. Номера счетов клиентов.
    outer_accounts: pd.Series. Номера внешних счетов для транзакций вне банка.
    client_devices: pd.DataFrame
    online_merchant_ids: pd.Series
    cities: pd.DataFrame
    in_lim: int. лимит входящих транзакций. После его достижения - отклонение всех операций клиента
    out_lim: int. лимит исходящих транзакций. После его достижения - отклонение всех операций клиента
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
    split_rate: float. От 0 до 1. Доля случаев, когда дроп распределяет полученные деньги по частям, а не одной операцией.
    chunks: dict. Характеристики для генератора сумм транзакций по частям. Ключи см. в drops.yaml
    trf_max: int. Максимальная сумма для одного исх. перевода.
    reduce_share: float. Доля уменьшения суммы от первой отклоненной откл. транз.
                  Если после первой откл. транз. дроп будет пытаться еще, то он будет
                  уменьшать след. сумму на: сумму первой откл. транз умн. на reduce_share.
                  Если это не больше чем текущий баланс.
    inbound_amt: dict. Лимиты на перевод. Если баланс больше. То разбиваем на части
    round: int. Округление целой части сумм транзакций. 
           Напр. 500 значит что суммы будут кратны 500 - кончаться на 500 или 000
    attempts: dict. Лимиты попыток операций после первой отклоненной операции.
              Ключи: trf_min, trf_max, atm_min, atm_max.
    to_drops: dict. Параметры переводов другим дропам
    crypto_rate: float. Доля переводов в крипту от переводов дропа
    """
    clients: pd.DataFrame
    timestamps: pd.DataFrame
    accounts: pd.DataFrame
    outer_accounts: pd.Series
    client_devices: pd.DataFrame
    online_merchant_ids: pd.Series
    cities: pd.DataFrame
    in_lim: int
    out_lim: int
    period_in_lim: int
    period_out_lim: int
    inbound_amt: dict
    split_rate: float
    chunks: dict
    trf_max: int
    reduce_share: float
    round: dict
    lag_interval: int
    two_way_delta: dict
    pos_delta: dict
    attempts: dict
    to_drops: dict
    crypto_rate: float


# 3. Датакласс для конфигов транзакций дропов-покупателей 

@dataclass
class DropPurchaserCfg:
    """
    Это данные на основе которых будут генерироваться транзакции дропов-распределителей
    ---------------------
    clients: pd.DataFrame
    timestamps: pd.DataFrame
    accounts: pd.DataFrame. Номера счетов клиентов.
    outer_accounts: pd.Series. Номера внешних счетов для транзакций вне банка.
    client_devices: pd.DataFrame
    online_merchant_ids: pd.Series
    cities: pd.DataFrame
    in_lim: int. лимит входящих транзакций. После его достижения - отклонение всех операций клиента
    out_lim: int. лимит исходящих транзакций. После его достижения - отклонение всех операций клиента
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
    split_rate: float. От 0 до 1. Доля случаев, когда дроп распределяет полученные деньги по частям, а не одной операцией.
    chunks: dict. Характеристики для генератора сумм транзакций по частям. Ключи см. в drops.yaml
    trf_max: int. Максимальная сумма для одного исх. перевода.
    reduce_share: float. Доля уменьшения суммы от первой отклоненной откл. транз.
                  Если после первой откл. транз. дроп будет пытаться еще, то он будет
                  уменьшать след. сумму на: сумму первой откл. транз умн. на reduce_share.
                  Если это не больше чем текущий баланс.
    inbound_amt: dict. Лимиты на перевод. Если баланс больше. То разбиваем на части
    round: int. Округление целой части сумм транзакций. 
           Напр. 500 значит что суммы будут кратны 500 - кончаться на 500 или 000
    attempts: dict. Лимиты попыток операций после первой отклоненной операции.
              Ключи: min, max.
    """
    clients: pd.DataFrame
    timestamps: pd.DataFrame
    accounts: pd.DataFrame
    client_devices: pd.DataFrame
    online_merchant_ids: pd.Series
    cities: pd.DataFrame
    in_lim: int
    out_lim: int
    period_in_lim: int
    period_out_lim: int
    inbound_amt: dict
    split_rate: float
    chunks: dict
    amt_max: int
    reduce_share: float
    round: dict
    lag_interval: int
    two_way_delta: dict
    pos_delta: dict
    attempts: dict



