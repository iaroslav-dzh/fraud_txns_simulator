import pandas as pd
from dataclasses import dataclass

@dataclass
class DropConfigs:
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
    fraud_amounts: pd.DataFrame
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
    fraud_amounts: pd.DataFrame
    period_in_lim: int
    period_out_lim: int
    lag_interval: int
    two_way_delta: dict
    pos_delta: dict
    