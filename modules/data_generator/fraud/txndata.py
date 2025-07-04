# Функции и классы относящиеся к генерации данных фрод транзакций, кроме времени.

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
from shapely.ops import transform
from pyproj import Geod
import pyarrow
from datetime import datetime
import sys
import importlib


from data_generator.utils import get_values_from_truncnorm
    

#

class FraudTransPartialData:
    """
    Класс для генерации данных о транзакции, мерчанте, геопозиции, IP адресе.
    Пока сделан только для фрода.
    """
    def __init__(self, merchants_df, client_info, online_merchant_ids, fraud_ips, used_ips, fraud_devices, \
                used_devices, client_devices, last_txn=()):
        """
        merchants_df - pd.DataFrame. Оффлайн мерчанты
        client_info - pd.DataFrame или namedtuple. Запись с информацией о клиенте
        online_merchant_ids- pd.Series. id онлайн мерчантов
        fraud_ips - pd.DataFrame. ip для фрода с гео информацией
        used_ips - pd.Series. Сюда записывать ip адреса использованные для фрода.
        fraud_devices - pd.DataFrame. девайсы для фрода: платформа, id устройства.
        used_devices - pd.Series. Сюда записывать id девайсов использованные для фрода.
        client_devices - pd.DataFrame. Девайсы клиентов.
        last_txn - предыдущая транзакция. Записывается при использовании некоторых методов (пока только для freq_trans)
        """
        # self.online = online
        self.merchants_df = merchants_df
        self.client_info = client_info
        self.online_merchant_ids = online_merchant_ids
        self.fraud_ips = fraud_ips
        self.used_ips = used_ips
        self.fraud_devices = fraud_devices
        self.used_devices = used_devices
        self.client_devices = client_devices
        self.last_txn = last_txn

    
    def another_city(self, client_city, online, category_name):
        """
        Генерация merchant_id, координат транзакции, названия города, IP адреса и device_id (если онлайн)
        C городом отличным от города клиента. Для онлайн транзакций также другой device_id и IP адрес в другом городе.
        Нужен для правил: fast_geo_change, fast_geo_change_online, new_ip_and_device_high_amount.
        -----------------------------------------------
        Работает для онлайн и оффлайн транзакций
        client_city - str.
        online - bool.
        category_name - str.
        """
        
        if online:
            merchant_id = self.online_merchant_ids.sample(n=1).iat[0]
            
            # Семпл IP которого нет в used_ips и который IP другого города
            fraud_ips = self.fraud_ips.loc[~self.fraud_ips.fraud_ip.isin(self.used_ips)]
            fraud_ip = fraud_ips.loc[fraud_ips["area"] != client_city].sample(1)
            # Координаты города и название по IP адресу
            trans_lat = fraud_ip.lat.iloc[0]
            trans_lon = fraud_ip.lon.iloc[0]
            trans_ip = fraud_ip.fraud_ip.iloc[0]
            trans_city = fraud_ip["area"].iloc[0]
            channel = "ecom"
            
            # Семпл девайса которого нет в used_devices
            devices = self.fraud_devices.loc[~self.fraud_devices.device_id.isin(self.used_devices), "device_id"]
            device_id = devices.sample(1).iloc[0]
            
            # Записываем IP и device_id как использованные
            self.used_ips.loc[self.used_ips.shape[0]] = trans_ip
            self.used_devices.loc[self.used_devices.shape[0]] = device_id

        else:
            # Семплируется мерчант не из города клиента
            merchants = self.merchants_df.loc[self.merchants_df["area"] != client_city]
            merchant = merchants.loc[merchants.category == category_name].sample(1)
            # Берется его id, и координаты, как координаты транзакции
            merchant_id = merchant["merchant_id"].iloc[0]
            trans_lat = merchant["merchant_lat"].iloc[0]
            trans_lon = merchant["merchant_lon"].iloc[0]
            trans_ip = "not applicable"
            trans_city = merchant["area"].iloc[0]
            device_id = np.nan
            channel = "POS"

        
        txn_type = "purchase"
            
        return merchant_id, trans_lat, trans_lon, trans_ip, trans_city, device_id, channel, txn_type


    def new_device_and_ip(self, client_city, category_name, online=True, another_city=True):
        """
        Генерация merchant_id, координат транзакции, названия города, IP адреса(если применимо) и device_id
        Для правил: new_ip_and_device_high_amount, new_device_and_high_amount, freq_trans.
        ---------------------------------------------------------------------
        another_city - bool. Должен ли IP быть отличного от клиентского города.
        """
        merchant_id = self.online_merchant_ids.sample(n=1).iat[0]

        # IP адрес другого города и остальная информация
        if another_city:
            return self.another_city(client_city=client_city, online=online, category_name=category_name)
            
        # Другой IP адрес, но город клиента - для new_device_and_high_amount
        fraud_ips = self.fraud_ips.loc[~self.fraud_ips.fraud_ip.isin(self.used_ips)]
        fraud_ip = fraud_ips.loc[fraud_ips["area"] == client_city].sample(1)
        
        # Координаты города и название по IP адресу
        trans_lat = fraud_ip.lat.iloc[0]
        trans_lon = fraud_ip.lon.iloc[0]
        trans_ip = fraud_ip.fraud_ip.iloc[0]
        trans_city = fraud_ip["area"].iloc[0]

        # Записываем IP как использованный
        self.used_ips.loc[self.used_ips.shape[0]] = trans_ip
        
        # Семпл девайса которого нет в used_devices
        devices = self.fraud_devices.loc[~self.fraud_devices.device_id.isin(self.used_devices), "device_id"]
        device_id = devices.sample(1).iloc[0]
        
        # Записываем device_id как использованный
        self.used_devices.loc[self.used_devices.shape[0]] = device_id

        channel = "ecom"
        txn_type = "purchase"
        
        return merchant_id, trans_lat, trans_lon, trans_ip, trans_city, device_id, channel, txn_type


    def freq_trans(self, client_city, category_name, another_city):
        """
        another_city - bool. Должен ли IP быть отличного от клиентского города.
        """
        self.last_txn = self.new_device_and_ip(client_city, category_name, online=True, another_city=another_city)
        
        return self.last_txn


    def original_purchase(self, online=True):
        """
        Оригинальные данные клиента для операций покупок.
        На данный момент это для дропов.
        Для операций на криптобирже и для покупки товаров дропами
        """
        if online:
            merchant_id = self.online_merchant_ids.sample(n=1).iat[0]
            # Координаты города и название
            trans_lat = self.client_info.lat
            trans_lon = self.client_info.lon
            trans_ip = self.client_info.home_ip
            trans_city = self.client_info.area        
            # Семпл девайса клиента
            devices = self.client_devices.loc[self.client_devices.client_id == self.client_info.client_id]
            device_id = devices.device_id.sample(1).iloc[0]
            txn_type = "purchase"

        # Не генерируем channel. Он должен быть определен вовне
        return merchant_id, trans_lat, trans_lon, trans_ip, trans_city, device_id, txn_type

        
    def original_data(self, online, receive=None):
        """
        Пока этот метод для клиентов дропов и, возможно, для переводов мошенникам
        ------------------------------------
        client_id - int.
        online - bool.
        received - bool.
        """
        # Входящий перевод
        if online and receive:
            trans_ip = "not applicable"
            device_id = pd.NA
            channel = "transfer"
            txn_type = "inbound"
            merchant_id = np.nan
            trans_lat = np.nan
            trans_lon = np.nan
            trans_city = "not applicable"
            
            return merchant_id, trans_lat, trans_lon, trans_ip, trans_city, device_id, channel, txn_type
        
        # Исходящий перевод
        elif online:
            # Для онлайна просто берется home_ip и device_id из данных клиента.
            trans_ip = self.client_info.home_ip
            devices = self.client_devices.loc[self.client_devices.client_id == self.client_info.client_id]
            device_id = devices.device_id.sample(1).iloc[0]
            channel = "transfer"
            txn_type = "outbound"  

        # Оффлайн
        else:
            trans_ip = "not applicable"
            device_id = pd.NA
            channel = "ATM"
            txn_type = "withdrawal"
            
        merchant_id = np.nan
        # Локация транзакции просто записываем координаты и название города клиента
        trans_lat = self.client_info.lat
        trans_lon = self.client_info.lon
        trans_city = self.client_info.area

        return merchant_id, trans_lat, trans_lon, trans_ip, trans_city, device_id, channel, txn_type


    def reset_used(self, used_ips=False, used_devices=False):
        """
        Сброс кэша использованных ip и/или девайсов
        Можно выбрать одно или оба сразу.
        ----------
        used_ips - bool
        used_devices - bool
        """

        if used_ips:
            self.used_ips = pd.Series()
            
        if used_devices:
            self.used_devices = pd.Series()


# .

class TransAmount: 
    """
    Генерация суммы транзакции
    """

    def __init__(self, categories_stats):
        """
        categories_stats - pd.DataFrame с категориями и характеристиками их сумм
        """
        self.categories = categories_stats
        

    def fraud_amount(self, category_name):
        """
        Фрод транзакции. Генерация суммы с выставленными минимумом, максимумом, средним и отклонением
        """
        
        category = self.categories[self.categories.category == category_name]
        low = category.fraud_low
        high = category.fraud_high
        mean = category.fraud_mean
        std = category.fraud_std

        # Генерация числа и округление до десятков
        return get_values_from_truncnorm(low_bound=low, high_bound=high, mean=mean, std=std)[0] // 10 * 10

    def freq_trans_amount(self, low=2000, high=10000, mean=4000, std=1500):
        """
        Генерация суммы специально для правила freq_trans_amount
        """
        
        # Генерация числа и округление до десятков
        return get_values_from_truncnorm(low_bound=low, high_bound=high, mean=mean, std=std)[0] // 10 * 10
    

# .

def sample_category(categories, online=None, is_fraud=None, rule=None):
    """
    categories - pd.DataFrame с категориями и их характеристиками
    online - bool. Онлайн или оффлайн категория нужна
    is_fraud - bool. Фрод или не фрод. От этого зависит вероятность категории.
    """

    if is_fraud and online and rule != "trans_freq_increase":
        online_categories = categories.loc[categories.online == True]
        cat_sample = online_categories.sample(1, weights=online_categories.fraud_share)
        return cat_sample

    elif is_fraud and online and rule == "trans_freq_increase":
        chosen_categories = categories.loc[categories.category.isin(["shopping_net", "misc_net"])]
        cat_sample = chosen_categories.sample(1, weights=chosen_categories.fraud_share)
        return cat_sample

        
    elif is_fraud and not online:
        offline_categories = categories.loc[categories.online == False]
        cat_sample = offline_categories.sample(1, weights=offline_categories.fraud_share)
        return cat_sample
