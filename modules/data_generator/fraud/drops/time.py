# Время для дропов

import pandas as pd
import numpy as np
from typing import Union

from data_generator.fraud.time import derive_from_last_time
from data_generator.configs import DropDistributorCfg, DropPurchaserCfg


class DropTimeHandler:
    """
    Управление временем транзакций дропа
    --------------
    Атрибуты:
    --------
    configs - DropDistributorCfg | DropPurchaserCfg. Датакласс с конфигами и данными для транзакций.
    timestamps - pd.DataFrame. Диапазон timestamp-ов с колонками: | timestamp | unix_time | hour |
    start_unix - int. Во сколько была первая транзакция в периоде. Нужная для отсчета следующего периода
                      активности. Unix время в секундах. По умолчанию 0.
    last_unix - int. Время последней транзакции. Unix время в секундах. По умолчанию 0.
    in_lim - int. Количество входящих транзакций после которых дроп уходит на паузу
    out_lim - int. Количество исходящих транзакций после которых дроп уходит на паузу
    in_txns - int. Количество входящих транзакций в периоде активности. По умолчанию 0.
    out_txns - int. Количество исходящих транзакций в периоде активности. По умолчанию 0.
    """
    def __init__(self, configs: Union[DropDistributorCfg, DropPurchaserCfg]):
        """ 
        configs - DropDistributorCfg | DropPurchaserCfg. Датакласс с конфигами и данными для транзакций.
        in_lim - int. Количество входящих транзакций после которых дроп уходит на паузу
        out_lim - int. Количество исходящих транзакций после которых дроп уходит на паузу
        start_unix - int. Во сколько была первая транзакция в периоде. Нужная для отсчета следующего периода
                          активности. Unix время в секундах. 
        last_unix - int. Время последней транзакции. Unix время в секундах.
        """
        self.configs = configs
        self.timestamps = configs.timestamps
        self.start_unix = 0
        self.last_unix = 0
        self.in_lim = configs.period_in_lim
        self.out_lim = configs.period_out_lim
        self.in_txns = 0
        self.out_txns = 0


    def get_time_delta(self, two_way, minutes=True):
        """
        Получение случайного интервала времени в секундах или минутах из равномерного распределения
        ---------------------
        two_way - bool. Дельта может быть <= 0 и > 0. Если False то только > 0
        minutes - bool. Минуты или секунды
        """
        if two_way:
            two_way_min = self.configs.two_way_delta["min"]
            two_way_max = self.configs.two_way_delta["max"]
            delta = np.random.uniform(two_way_min, two_way_max)
        else:
            pos_min = self.configs.pos_delta["min"]
            pos_max = self.configs.pos_delta["max"]
            delta = np.random.uniform(pos_min, pos_max)

        if minutes:
            return round(delta)
            
        return round(delta * 60)
    

    def txns_count(self, receive=False, reset=False):
        """
        Внутренний счетчик входящих и исходящих транзакций за период активности.
        Имеет 4 варианта действий для self.in_txns и self.out_txns: 
        1. сброс счетчиков на in=1, out=0; 
        2. Сброс на in=0, out=1;
        3. in + 1;
        4. out + 1;
        ---------------------
        receive: bool. Входящая транзакция или исходящая.
        reset: bool. Начать отчет заново или нет.
        """
        # Если транзакция входящая. Период начинается заново и с входящей транзакции
        # Эта транзакция будет первой в периоде
        if receive and reset:
            self.in_txns = 1
            self.out_txns = 0
            return
        # Исходящая транзакция и период начинается заново. Эта транзакция будет первой входящей в периоде
        if reset:
            self.in_txns = 0
            self.out_txns = 1
            return
        # Входящая транзакция и период продолжается. Прибавлем счетчик входящих.
        if receive:
            self.in_txns += 1
            return
        # Исходящая транзакция и период продолжается. Прибавлем счетчик исходящих.
        self.out_txns += 1


    def get_txn_time(self, receive, in_txns):
        """
        Генерация времени транзакции
        ------------------
        receive: bool. Текущая транзакция будет входящей или исходящей.
        in_txns: int. Абсолютное количество входящих транзакций на текущий момент, не считая генерируемую.
        lag_interval: int. Желаемый лаг по времени от последней транзакции в минутах.
                            Используется для перерывов в активности дропа. По умолчанию 1440 минут т.е. 24 часа
        """

        # Если это самая первая транзакция. Т.к. активность дропа начинается с входящей транзакции
        if receive and in_txns == 0:
            time_sample = self.timestamps.sample(1)
            txn_time = time_sample.timestamp.iat[0]
            self.last_unix = time_sample.unix_time.iat[0]
            self.start_unix = self.last_unix
            self.in_txns += 1
            return txn_time, self.last_unix

        # Условия для не первых транзакций

        # Если достигнут лимит входящих транзакций для периода активности
        if self.in_txns == self.in_lim:
            # Генерация дельты, чтобы время выглядело не ровным, а случайным.
            # Слагаем её с lag_interval
            time_delta = self.get_time_delta(two_way=True)
            lag_interval = self.configs.lag_interval + time_delta
            # print(time_delta, lag_interval)
            # Сбрасываем счетчик входящих и исходящих транзакций. Отсчет заново для нового периода
            # Входящие = 1, исходящие = 0
            self.txns_count(receive=receive, reset=True)
            # Создаем время и записываем его как время последней транзакции в целом
            # и как время первой транзакции в новом периоде
            last_time, self.last_unix = derive_from_last_time(last_txn_unix=self.start_unix, lag_interval=lag_interval)
            self.start_unix = self.last_unix
            return last_time, self.last_unix

        # Если достигнут лимит исходящих транзакций для периода активности
        elif self.out_txns == self.out_lim:
            time_delta = self.get_time_delta(two_way=True)
            lag_interval = self.configs.lag_interval + time_delta
            # Входящие = 0, исходящие = 1
            self.txns_count(receive=receive, reset=True)
            # Создаем время и записываем его как время последней транзакции в целом
            # и как время первой транзакции в новом периоде
            last_time, self.last_unix = derive_from_last_time(last_txn_unix=self.start_unix, lag_interval=lag_interval)
            self.start_unix = self.last_unix
            return last_time, self.last_unix

        # Тоже дельта, но не может быть <= 0 т.к. тут мы ее используем как lag_interval
        # Это для случаев когда транзакция совершается в тот же период активности что и последняя
        time_delta = self.get_time_delta(two_way=False)
        # print(time_delta)
        last_time, self.last_unix = derive_from_last_time(last_txn_unix=self.last_unix, lag_interval=time_delta)
        # +1 к счетчику соответствующего типа
        self.txns_count(receive=receive, reset=False)

        return last_time, self.last_unix
    

    def reset_cache(self):
        """
        Очистка кэшированных данных и счетчиков
        ------------
        """
        self.start_unix = 0
        self.last_unix = 0
        self.in_txns = 0
        self.out_txns = 0
        