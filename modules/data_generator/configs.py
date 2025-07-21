# Конфигурационные структуры: датаклассы под конфиги и данные для генерации транзакций.

import pandas as pd
from dataclasses import dataclass

# 1. Датакласс под конфиги для легальных транзакций.

@dataclass
class LegitCfg:
    """
    Конфиги и данные для генерации легальных транзакций.
    ---------------------
    clients: pd.DataFrame. Выборка клиентов для создания транз-ций.
    timestamps: pd.DataFrame. timestamps для генерации времени.
    timestamps_1st: pd.DataFrame. Сабсет timestamps отфильтрованный по первому месяцу и, 
                    если применимо, году. Чтобы генерировать первые транзакции.
    transactions: pd.DataFrame. Пустой датафрейм под транзакции.
    client_devices: pd.DataFrame. id и информация о девайсах клиентов.
    offline_merchants: pd.DataFrame. Оффлайн мерчанты с их координатами.
    categories: pd.DataFrame. Названия категорий и их полные характеристики.
                    Берутся из cat_stats_full.csv.
    online_merchant_ids: pd.Series. id для онлайн мерчантов
    all_time_weights: dict. Все веса времени. Генерить в LegitCfgBuilder
                      Веса для часов времени в виде словаря содержащего датафрейм с весами, 
                      название распределения и цветом для графика.
    cities: pd.DataFrame. Все имеющиеся уникальные города.
    min_intervals: dict. Мин. интервалы между транз-ми
    txn_num: dict. Конфиги кол-ва транзакций.
    data_paths: dict. Пути к данным из base.yaml
    dir_category: str. Ключ к категории директорий в base.yaml.
                       Ключ это одна из папок в data/
                       Тут будут храниться сгенерированные данные.
    folder_name: str. Название индивидуальной папки внутри папки 
                 текущего запуска генерации. Например 'legit'.
    key_latest: str. Ключ к полному пути записи файла в папке 
                data/generated/latest/ в base.yaml
    key_history: str. Ключ к пути для создания отдельной папки 
                 генерации в в base.yaml
    run_dir: str. Путь к директории под текущую генерацию.
    prefix: str. Префикс для названия файлов с чанками транзакций, например
            'legit_'
    directory: str. Путь к отдельной папке под конкретно текущую генерацию.
    txns_file_name: str. Название файла с транзакциями который будет создан.
    """
    clients: pd.DataFrame
    timestamps: pd.DataFrame
    timestamps_1st: pd.DataFrame
    transactions: pd.DataFrame
    client_devices: pd.DataFrame
    offline_merchants: pd.DataFrame
    categories: pd.DataFrame
    online_merchant_ids: pd.Series
    all_time_weights: dict
    cities: pd.DataFrame
    min_intervals: dict
    txn_num: dict
    data_paths: dict
    dir_category: str
    folder_name: str
    key_latest: str
    key_history: str
    run_dir: str
    prefix: str
    directory: str
    txns_file_name: str


# 2. Датакласс под конфиги фрода в покупках, когда аккаунт или карта клиента скомпрометированы
# Compromised Purchase Fraud
# Это данные на основе которых будут генерироваться транзакции

@dataclass
class ComprClientFraudCfg:
    """
    Конфиги и данные для генерации фрод транзакций в
    категории compromised client fraud.
    ---------------------
    clients: pd.DataFrame
    timestamps: pd.DataFrame
    transactions: pd.DataFrame
    offline_merchants: pd.DataFrame
    categories: pd.DataFrame
    online_merchant_ids: pd.Series
    all_time_weights: dict
    rules: pd.DataFrame
    cities: pd.DataFrame
    fraud_devices: pd.DataFrame
    fraud_ips: pd.DataFrame
    fraud_amounts: pd.DataFrame
    rules_cfg: dict. Конфиги для правил из compr.yaml.
    data_paths: dict. Пути к данным из base.yaml
    dir_category: str. Ключ к категории директорий в base.yaml.
                       Ключ это одна из папок в data/
                       Тут будут храниться сгенерированные данные.
    folder_name: str. Название индивидуальной папки внутри папки 
                 текущего запуска генерации. Например 'compormised'.
    key_latest: str. Ключ к полному пути записи файла в папке 
                data/generated/latest/ в base.yaml
    key_history: str. Ключ к директории для создания отдельной папки 
                 генерации в в base.yaml
    run_dir: str. Путь к директории под текущую генерацию.
    directory: str. Путь к отдельной папке под конкретно текущую генерацию.
    txns_file_name: str. Название файла с транзакциями который будет создан.
    """
    clients: pd.DataFrame
    timestamps: pd.DataFrame
    transactions: pd.DataFrame
    offline_merchants: pd.DataFrame
    categories: pd.DataFrame
    online_merchant_ids: pd.Series
    all_time_weights: dict
    rules: pd.DataFrame
    cities: pd.DataFrame
    fraud_devices: pd.DataFrame
    fraud_ips: pd.DataFrame
    fraud_amounts: pd.DataFrame
    rules_cfg: dict
    data_paths: dict
    dir_category: str
    folder_name: str
    key_latest: str
    key_history: str
    run_dir: str
    directory: str
    txns_file_name: str


# 3. Датакласс для конфигов транзакций дропов-распределителей

@dataclass
class DropDistributorCfg:
    """
    Это данные на основе которых будут генерироваться транзакции дропов-распределителей
    ---------------------
    clients: pd.DataFrame
    timestamps: pd.DataFrame
    transactions: pd.DataFrame. Пустой датафрейм под транзакции.
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
    crypto_rate: float. Доля переводов в крипту от переводов дропа.
    data_paths: dict. Пути к данным из base.yaml
    dir_category: str. Ключ к категории директорий в base.yaml.
                       Ключ это одна из папок в data/
                       Тут будут храниться сгенерированные данные.
    folder_name: str. Название индивидуальной папки внутри папки текущего запуска генерации.
                 Например 'dist_drops'.
    key_latest: str. Ключ к полному пути записи файла в папке 
                data/generated/latest/ в base.yaml
    key_history: str. Ключ к директории для создания отдельной папки 
                 генерации в в base.yaml
    run_dir: str. Название общей папки для хранениявсех файлов текущего
             запуска генерации: легальных, compromised фрода, дроп фрода.
    txns_file_name: str. Название файла с транзакциями который будет создан.
    """
    clients: pd.DataFrame
    timestamps: pd.DataFrame
    transactions: pd.DataFrame
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
    data_paths: dict
    dir_category: str
    folder_name: str
    key_latest: str
    key_history: str
    run_dir: str
    directory: str
    txns_file_name: str


# 4. Датакласс для конфигов транзакций дропов-покупателей 

@dataclass
class DropPurchaserCfg:
    """
    Это данные на основе которых будут генерироваться транзакции дропов-распределителей
    ---------------------
    clients: pd.DataFrame
    timestamps: pd.DataFrame
    transactions: pd.DataFrame. Пустой датафрейм под транзакции.
    accounts: pd.DataFrame. Номера счетов клиентов.
    outer_accounts: pd.Series. Номера внешних счетов для транзакций вне банка.
    client_devices: pd.DataFrame
    online_merchant_ids: pd.Series
    categories: pd.DataFrame. Категории специально для дропов покупателей.
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
    data_paths: dict. Пути к данным из base.yaml
    dir_category: str. Ключ к категории директорий в base.yaml.
                       Ключ это одна из папок в data/
                       Тут будут храниться сгенерированные данные.
    folder_name: str. Название индивидуальной папки внутри папки текущего запуска генерации.
                 Например 'purch_drops'.
    key_latest: str. Ключ к полному пути записи файла в папке 
                data/generated/latest/ в base.yaml
    key_history: str. Ключ к директории для создания отдельной папки 
                 генерации в в base.yaml
    run_dir: str. Название общей папки для хранениявсех файлов текущего
             запуска генерации: легальных, compromised фрода, дроп фрода.
    txns_file_name: str. Название файла с транзакциями который будет создан.
    """
    clients: pd.DataFrame
    timestamps: pd.DataFrame
    transactions: pd.DataFrame
    accounts: pd.DataFrame
    client_devices: pd.DataFrame
    online_merchant_ids: pd.Series
    categories: pd.DataFrame
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
    data_paths: dict
    dir_category: str
    folder_name: str
    key_latest: str
    key_history: str
    run_dir: str
    directory: str
    txns_file_name: str



