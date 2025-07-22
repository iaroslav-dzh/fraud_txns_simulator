# . Создание части данных транзакции для дропов.

import numpy as np
from typing import Union

from data_generator.configs import DropDistributorCfg, DropPurchaserCfg


class DropTxnPartData:
    """
    Класс для генерации части данных о транзакции дропа:
    канал, тип операции, мерчант, геопозиция, город, IP адрес, иногда статус.
    ------------------
    Атрибуты:
    --------
    client_info - pd.DataFrame или namedtuple. Запись с информацией о клиенте
    online_merchant_ids- pd.Series. id онлайн мерчантов
    client_devices - pd.DataFrame. Девайсы клиентов.
    last_txn - tuple. Для кэширования данных любой последней транзакции.
    """
    def __init__(self, configs: Union[DropDistributorCfg, DropPurchaserCfg]):
        """
        configs: Один из датаклассов — DropDistributorCfg или DropPurchaserCfg.
                 Содержит параметры и конфиги для генерации фрода.
        """
        self.client_info = None
        self.online_merchant_ids = configs.online_merchant_ids
        self.client_devices = configs.client_devices
        self.last_txn = None


    def assert_client_info(self):
        """
        Проверка что self.client_info не пустое
        """
        assert self.client_info is not None, \
            f"self.client_info is {type(self.client_info)}"


    def original_purchase(self, online=True, get_cached=False):
        """
        Оригинальные данные клиента для операций покупок.
        На данный момент это для дропов.
        Для операций на криптобирже и для покупки товаров дропами
        -------
        online: bool.
        get_cached: bool. Пробовать ли вернуть последние кэшированные данные
                    вместо генерации новых.
        """
        self.assert_client_info()

        if get_cached and self.last_txn is not None:
            return self.last_txn

        if online:
            merchant_id = self.online_merchant_ids.sample(n=1).iat[0]
            # Координаты города и название
            trans_lat = self.client_info.lat
            trans_lon = self.client_info.lon
            trans_ip = self.client_info.home_ip
            trans_city = self.client_info.city        
            # Семпл девайса клиента
            devices = self.client_devices.loc[self.client_devices.client_id == self.client_info.client_id]
            device_id = devices.device_id.sample(1).iloc[0]
            txn_type = "purchase"
            # Не генерируем channel. Он должен быть определен вовне
            channel = None

            self.last_txn = merchant_id, trans_lat, trans_lon, trans_ip, trans_city, \
                            device_id, channel, txn_type
            self.last_txn = self.last_txn
            return self.last_txn

        
    def original_data(self, online, receive=None):
        """
        Получение оригинальных данных клиента для транзакции.
        Пока этот метод для клиентов дропов и, возможно, для переводов мошенникам
        ------------------------------------
        online - bool.
        received - bool.
        """
        self.assert_client_info()

        # Входящий перевод
        if online and receive:
            trans_ip = "not applicable"
            device_id = np.nan
            channel = "transfer"
            txn_type = "inbound"
            merchant_id = np.nan
            trans_lat = np.nan
            trans_lon = np.nan
            trans_city = "not applicable"

            self.last_txn = merchant_id, trans_lat, trans_lon, trans_ip, trans_city, \
                            device_id, channel, txn_type
            return self.last_txn
        
        client_info = self.client_info
        client_devices = self.client_devices
        # Исходящий перевод
        if online:
            # Для онлайна просто берется home_ip и device_id из данных клиента.
            trans_ip = client_info.home_ip
            devices = client_devices.loc[client_devices.client_id == client_info.client_id]
            device_id = devices.device_id.sample(1).iloc[0]
            channel = "transfer"
            txn_type = "outbound"  

        # Оффлайн. Снятие в банкомате
        elif not online:
            trans_ip = "not applicable"
            device_id = np.nan
            channel = "ATM"
            txn_type = "withdrawal"
            
        merchant_id = np.nan
        # Локация транзакции просто записываем координаты и название города клиента
        trans_lat = client_info.lat
        trans_lon = client_info.lon
        trans_city = client_info.city

        self.last_txn = merchant_id, trans_lat, trans_lon, trans_ip, trans_city, \
                        device_id, channel, txn_type
        return self.last_txn
    
    
    def check_previous(self, dist, last_full):
        """
        Решить можно ли вернуть данные последней транзакции
        из кэша. True может вернуть только для дропа распределителя.
        ------
        dist: bool. Дроп распределитель или нет.
        last_full: dict. Полные данные последней транзакции.
                   Нужны, чтобы узнать channel.
        """
        if not dist:
            return False
        if last_full is None:
            return False
        if last_full["channel"] == "crypto_exchange":
            return True
        
        return False

    
    def reset_cache(self):
        """
        Сброс кэша
        """
        self.last_txn = None