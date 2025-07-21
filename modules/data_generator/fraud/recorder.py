# 
import os
import pandas as pd
from datetime import datetime

class FraudTxnsRecorder:
    """
    Запись фрод транзакций в файл.
    Пишет в 2 директории.
    ------------
    Атрибуты:
    ------------
    category: str. Ключ к категории директорий в base.yaml.
                       Ключ это одна из папок в data/
                       Тут будут храниться сгенерированные данные.
    key_latest: str. Ключ в base.yaml для пути к файлу в директории
                data/generated/latest/. Это для записи созданных
                транз-ций как последних сгенерированных.
    key_history: str. Ключ в base.yaml для пути к директории.
                    с историей генерации данных data/generated/history/
    folder_name: str. Название индивидуальной папки внутри папки 
                 текущего запуска генерации.
    run_dir: str. Название общей папки для всех файлов этой попытки/запуска
             генерации: легальных, compromised фрода, дроп фрода.
    data_paths: dict. Конфиги путей из base.yaml.
    prefix: str. Для названия индивидуальной папки внутри dir_category
                Например 'legit_'
    directory: str. Путь к директории в папке data/generated/history
               куда записывать чанки и собранный из них файл.
    all_txns: pd.DataFrame. Для записи генерируемых транз-ций.
              Получает готовый датафрейм с транзакциями для записии.
              По умолчанию None.
    """
    def __init__(self, configs):
        """
        configs: ComprClientFraudCfg | DropDistributorCfg | DropPurchaserCfg. 
        Конфиги и данные для генерации  транзакций. 
        """
        self.category = configs.dir_category
        self.key_latest = configs.key_latest
        self.key_history = configs.key_history
        self.data_paths = configs.data_paths
        self.directory = configs.directory
        self.txns_file_name = configs.txns_file_name
        self.all_txns = []


    def write_to_file(self):
        """
        Запись в файл.
        Пишем в две директории:
        history/<папка_текущей_генерации>/<своя_папка>/ и latest/ 
        """
        # Создаем полный путь для записи в папку текущей генерации
        file_name = self.txns_file_name
        path_history = os.path.join(self.directory, f"{file_name}")
        all_txns = self.all_txns
        all_txns.to_parquet(path_history, engine="pyarrow")
        
        # Берем полный путь из категории "generated" и по ключу latest
        # Это путь для последних созданных транзакций
        category = self.category
        key_latest = self.key_latest
        data_paths = self.data_paths
        path_latest = data_paths[category][key_latest]
        all_txns.to_parquet(path_latest, engine="pyarrow")