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
    key_history: str. Ключ в base.yaml для пути к директории
    data_paths: dict.
                    с историей генерации данных data/generated/history/
    prefix: str. Для названия индивидуальной папки внутри dir_category
                Например 'legit_'
    directory: str. Путь к директории в папке data/generated/history
               куда записывать чанки и собранный из них файл.
    all_txns: list. Для записи генерируемых транз-ций.
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
        self.prefix = configs.dir_prefix
        self.directory = None
        self.all_txns = []


    def make_dir(self):
        """
        Создать индивидуальную директорию под текущую генерацию
        транзакций.
        """
        category = self.category
        data_paths = self.data_paths
        key_history = self.key_history
        directory = data_paths[category][key_history]
        prefix = self.prefix
        datetime_suffix = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        folder_name = prefix + datetime_suffix
        path = os.path.join(directory, folder_name)

        if os.path.exists(path):
            return
        
        os.mkdir(path)
        self.directory = path

    def write_to_file(self):
        """
        Запись в файл.
        Пишем в две директории: history/<своя_папка> и latest/ 
        """
        # Создаем полный путь для записи в папку текущей генерации
        file_name = self.key_latest
        path_history = os.path.join(self.directory, f"{file_name}.parquet")
        all_txns = self.all_txns
        all_txns.to_parquet(path_history, engine="pyarrow")

        # Берем полный путь из категории "generated" и по ключу latest
        # Это путь для последних созданных легальных транзакций
        category = self.category
        key_latest = self.key_latest
        data_paths = self.data_paths
        path_latest = data_paths[category][key_latest]
        all_txns.to_parquet(path_latest, engine="pyarrow")