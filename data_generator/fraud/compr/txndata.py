# Функции и классы относящиеся к генерации данных фрод транзакций, кроме времени.

import pandas as pd
import numpy as np

from data_generator.utils import get_values_from_truncnorm, amt_rounding
from data_generator.configs import ComprClientFraudCfg
    

# .

class FraudTxnPartData:
    """
    Класс для генерации данных о транзакции для фрода в покупках
    когда данные/аккаунт клиента скомпрометированы: 
    канал, тип операции, мерчант, геопозиция, город, IP адрес, иногда статус.
    ------------------
    Атрибуты:
    --------
    merchants_df - pd.DataFrame. Оффлайн мерчанты
    client_info - pd.DataFrame или namedtuple. Запись с информацией о клиенте
    online_merchant_ids- pd.Series. id онлайн мерчантов
    fraud_ips - pd.DataFrame. ip для фрода с гео информацией
    used_ips - pd.Series. Сюда записывать ip адреса использованные для фрода.
    fraud_devices - pd.DataFrame. девайсы для фрода: платформа, id устройства.
    used_devices - pd.Series. Сюда записывать id девайсов использованные для фрода.
    last_txn - tuple. Предыдущая транзакция. Записывается при использовании некоторых 
               методов (пока только для freq_trans)
    """
    def __init__(self, configs: ComprClientFraudCfg):
        """
        configs: ComprClientFraudCfg. Содержит параметры и конфиги
                 для генерации транз-ций.
        """
        self.merchants_df = configs.offline_merchants
        self.client_info = None
        self.online_merchant_ids = configs.online_merchant_ids
        self.fraud_ips = configs.fraud_ips
        self.used_ips = pd.Series(name="ip_address")
        self.fraud_devices = configs.fraud_devices
        self.used_devices = pd.Series(name="device_id")
        self.last_txn = None
    
    
    def another_city(self, online, category_name):
        """
        Генерация merchant_id, координат транзакции, названия города, IP адреса и device_id (если онлайн)
        C городом отличным от города клиента. Для онлайн транзакций также другой device_id и IP адрес в другом городе.
        Нужен для правил: fast_geo_change, fast_geo_change_online, new_ip_and_device_high_amount.
        -----------------------------------------------
        Работает для онлайн и оффлайн транзакций
        online - bool.
        category_name - str.
        """
        client_city = self.client_info.city

        if online:
            merchant_id = self.online_merchant_ids.sample(n=1).iat[0]
            
            # Семпл IP которого нет в used_ips и который IP другого города
            fraud_ips = self.fraud_ips.loc[~self.fraud_ips.fraud_ip.isin(self.used_ips)]
            fraud_ip = fraud_ips.loc[fraud_ips["city"] != client_city].sample(1)
            # Координаты города и название по IP адресу
            trans_lat = fraud_ip.lat.iloc[0]
            trans_lon = fraud_ip.lon.iloc[0]
            trans_ip = fraud_ip.fraud_ip.iloc[0]
            trans_city = fraud_ip["city"].iloc[0]
            channel = "ecom"
            
            # Семпл девайса которого нет в used_devices
            devices = self.fraud_devices.loc[~self.fraud_devices.device_id.isin(self.used_devices), \
                                             "device_id"]
            device_id = devices.sample(1).iloc[0]
            
            # Записываем IP и device_id как использованные
            self.used_ips.loc[self.used_ips.shape[0]] = trans_ip
            self.used_devices.loc[self.used_devices.shape[0]] = device_id

        else:
            # Семплируется мерчант не из города клиента
            merchants = self.merchants_df.loc[self.merchants_df["city"] != client_city]
            merchant = merchants.loc[merchants.category == category_name].sample(1)
            # Берется его id, и координаты, как координаты транзакции
            merchant_id = merchant["merchant_id"].iloc[0]
            trans_lat = merchant["merchant_lat"].iloc[0]
            trans_lon = merchant["merchant_lon"].iloc[0]
            trans_ip = "not applicable"
            trans_city = merchant["city"].iloc[0]
            device_id = np.nan
            channel = "POS"

        
        txn_type = "purchase"
            
        return merchant_id, trans_lat, trans_lon, trans_ip, trans_city, device_id, channel, txn_type


    def new_device_and_ip(self, category_name, online=True, another_city=True):
        """
        Генерация merchant_id, координат транзакции, названия города, IP адреса(если применимо) и device_id
        Для правил: new_ip_and_device_high_amount, new_device_and_high_amount, freq_trans.
        ---------------------------------------------------------------------
        another_city - bool. Должен ли IP быть отличного от клиентского города.
        """
        client_city = self.client_info.city

        merchant_id = self.online_merchant_ids.sample(n=1).iat[0]

        # IP адрес другого города и остальная информация
        if another_city:
            return self.another_city(online=online, category_name=category_name)
            
        # Другой IP адрес, но город клиента - для new_device_and_high_amount
        fraud_ips = self.fraud_ips.loc[~self.fraud_ips.fraud_ip.isin(self.used_ips)]
        fraud_ip = fraud_ips.loc[fraud_ips["city"] == client_city].sample(1)
        
        # Координаты города и название по IP адресу
        trans_lat = fraud_ip.lat.iloc[0]
        trans_lon = fraud_ip.lon.iloc[0]
        trans_ip = fraud_ip.fraud_ip.iloc[0]
        trans_city = fraud_ip["city"].iloc[0]

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


    def freq_trans(self, category_name, another_city):
        """
        another_city - bool. Должен ли IP быть отличного от клиентского города.
        """

        self.last_txn = self.new_device_and_ip(category_name, online=True, another_city=another_city)
        
        return self.last_txn


    def reset_used(self, used_ips=False, used_devices=False):
        """
        Сброс кэша использованных ip и/или девайсов
        Можно выбрать одно или оба сразу.
        ----------
        used_ips - bool
        used_devices - bool
        """
        if used_ips:
            self.used_ips = pd.Series(name="ip_address")
        if used_devices:
            self.used_devices = pd.Series(name="device_id")


    def get_data(self, rule, online, category_name, txn_num):
        """
        Получить часть данных о транз-ции:
        merchant_id, trans_lat, trans_lon, trans_ip, trans_city,
        device_id, channel, txn_type
        -------------
        rule: str. Антифрод правило.
        online: bool. Онлайн флаг транз-ции.
        category_name: str. Название категории покупки.
        txn_num: int. Какая это фрод транз-ция по счету для текущего
                клиента. 
        """

        # Данные о мерчанте, геопозиции, IP, девайсе
        # Правило: быстрая смена гео. Оффлайн/онлайн
        if rule in ["fast_geo_change", "fast_geo_change_online"]:
            return self.another_city(online=online, category_name=category_name)

        elif rule in ["new_ip_and_device_high_amount"]:
            return self.new_device_and_ip(online=online, category_name=category_name, \
                                               another_city=True)

        elif rule == "new_device_and_high_amount":
            return self.new_device_and_ip(online=online, category_name=category_name, \
                                               another_city=False)

        # Если это первая транзакция под правило trans_freq_increase
        # То вызываем метод freq_trans для генерации части данных транзакции
        # Они не изменятся в дальнейших транзакциях из серии. И поэтому в следующих транзакциях
        # Будем брать кэшированный результат записанный в self.last_txn
        elif rule == "trans_freq_increase" and txn_num == 1:
            # В данном случае получаем также и статус транзакции кроме остальных данных.
            # Зависит от того, какая это транзакция по счету из серии частых транзакций
            return self.freq_trans(category_name=category_name, another_city=True)

        # Транзакция не первая в серии. Берем кэшированные данные созданные для первой транзакции.
        elif rule == "trans_freq_increase":
            return self.last_txn


# .

class TransAmount: 
    """
    Генерация суммы транзакции для compromised client
    фрода.
    ------------------
    categories: pd.DataFrame с категориями и характеристиками их сумм.
    """

    def __init__(self, configs):
        """
        configs: ComprClientFraudCfg. Конфиги и данные для генерации 
                 фрод транзакци в категории compromised client fraud.
        """
        self.categories = configs.fraud_amounts
        self.freq_txn = configs.rules_cfg["freq_txn"]
        

    def fraud_amount(self, category_name):
        """
        Фрод транзакции. Генерация суммы с выставленными минимумом, максимумом, средним и отклонением
        """
        
        category = self.categories[self.categories.category == category_name]
        low = category.fraud_low
        high = category.fraud_high
        mean = category.fraud_mean
        std = category.fraud_std

        # Генерация числа и случайное округление
        amount =  round(get_values_from_truncnorm(low_bound=low, high_bound=high, \
                                         mean=mean, std=std)[0], 2)
        return amt_rounding(amount, rate=0.5)

    def freq_trans_amount(self):
        """
        Генерация суммы специально для правила freq_trans_amount
        """
        low = self.freq_txn["amount"]["min"]
        high = self.freq_txn["amount"]["max"]
        mean = self.freq_txn["amount"]["mean"]
        std = self.freq_txn["amount"]["std"]
        
        # Генерация числа и случайное округление
        amount =  round(get_values_from_truncnorm(low_bound=low, high_bound=high, \
                                         mean=mean, std=std)[0], 2)
        return amt_rounding(amount, rate=0.4)
    

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
