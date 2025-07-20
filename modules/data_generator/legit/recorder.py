import os
import pandas as pd
from datetime import datetime

class LegitTxnsRecorder:
    """
    Запись легальных транзакций в файл.
    Предусмотрена запись частями в несколько файлов и затем
    чтение и запись в единый файл в двух копиях.
    ------------
    Атрибуты:
    ------------
    chunk_size: int. Кол-во части транз-ций для записи в файл.
    total_clients: int. Общее кол-во клиентов отобранных для генерации легальных
                   транзакций.
    category: str. Ключ к категории директорий в base.yaml.
                       Ключ это одна из папок в data/
                       Тут будут храниться сгенерированные данные.
    key_latest: str. Ключ в base.yaml для пути к файлу в директории
                data/generated/latest/. Это для записи созданных
                транз-ций как последних сгенерированных.
    key_history: str. Ключ в base.yaml для пути к директории
                    с историей генерации данных data/generated/history/
    folder_name: str. Название индивидуальной папки внутри папки 
                 текущего запуска генерации.
    run_dir: str. Название общей папки для всех файлов этой попытки/запуска
             генерации: легальных, compromised фрода, дроп фрода.
    prefix: str. Префикс для названия файлов с чанками транзакций, например
            'legit_'
    data_paths: dict. Конфиги путей из base.yaml.
    directory: str. Путь к директории в папке data/generated/history
               куда записывать чанки и собранный из них файл.
    all_txns: pd.DataFrame. Все сгенерированные транзакции. По умолчанию None.
    client_txns: list. Транз-ции текущего клиента для которого идет генерация.
    txns_chunk: list. Транз-ции текущего чанка для последующей записи в файл.
    txns_counter: int. Общее кол-во всех транзакций сгенерированных на данный
                  момент.
    clients_counter: int. Общее кол-во клиентов которые были обработаны включая
                     клиента для которого еще происходит генерация.
    chunks_counter: int. Общее кол-во чанков.
    """
    def __init__(self, configs):
        """
        configs: LegitCfg. Конфиги и данные для генерации легальных транзакций. 
        """
        self.chunk_size = configs.txn_num["chunk_size"]
        self.total_clients = configs.clients.shape[0]
        self.category = configs.dir_category
        self.key_latest = configs.key_latest
        self.key_history = configs.key_history
        self.data_paths = configs.data_paths
        self.folder_name = configs.folder_name
        self.run_dir = configs.run_dir
        self.prefix = configs.prefix
        self.directory = None
        self.all_txns = None
        self.client_txns = []
        self.txns_chunk = []
        self.txns_counter = 0
        self.clients_counter = 0
        self.chunks_counter = 0


    def make_dir(self):
        """
        Создать индивидуальную директорию под текущую генерацию
        транзакций.
        """
        category = self.category
        data_paths = self.data_paths
        key_history = self.key_history
        directory = data_paths[category][key_history]
        run_dir = self.run_dir
        # prefix = self.prefix
        # datetime_suffix = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        folder_name = self.folder_name
        path = os.path.join(directory, run_dir, folder_name)

        if os.path.exists(path):
            return
        
        os.mkdir(path)
        self.directory = path


    def name_the_chunk(self):
        """
        Создать название для файла под
        запись текущего чанка.
        """
        self.chunks_counter += 1
        prefix = self.prefix
        chunks_counter = self.chunks_counter
        return f"{prefix}{chunks_counter:03d}.parquet"


    def to_chunk(self, txns_num):
        """
        txns_num: int. Кол-во созданных транз-ций текущего клиента.
        """
        chunk_size = self.chunk_size
        txns_counter = self.txns_counter
        clients_counter = self.clients_counter
        total_clients = self.total_clients
        client_txns = self.client_txns 

        # если кол-во всех транзакций кратно размеру чанка
        if txns_counter % chunk_size == 0:
            return True 
        # Условия ниже для проверки является ли транзакция последней
        # в случае если генерация закончена, но чанк меньше выставленного размера
        if clients_counter != total_clients:
            return False
        if txns_num != len(client_txns):
            return False
        # Если чанк не по размеру, но генерация транзакций закончена
        # т.е. кол-во обработанных клиентов равно общему кол-ву и
        # кол-во созданных транз. клиента равно заданному кол-ву для него
        # То возвразаем True
        return True


    def record_chunk(self, txn, txns_num):
        """
        Запись транзакций в parquet в папку где хранятся созданные транзакции
        от всех запусков генератора.
        --------
        txn: dict. Созданная транзакция.
        txns_num: int. Кол-во созданных транз-ций текущего клиента.
        """
        directory = self.directory

        self.txns_chunk.append(txn)

        # Проверка по кол-ву. Если кол-во транз-ций равно размеру чанка
        # то пишем чанк в файл
        if self.to_chunk(txns_num=txns_num):
            chunk = pd.DataFrame(self.txns_chunk)
            chunk_name = self.name_the_chunk()
            full_path = os.path.join(directory, chunk_name)

            chunk.to_parquet(full_path, engine="pyarrow")
            self.txns_chunk.clear() # сброс чанка записанного в файл


    def build_from_chunks(self):
        """
        Сборка цельного датафрейма из чанков записанных в
        файлы. Датафрейм сохраняется в атрибут all_txns.
        """
        directory = self.directory
        chunks = os.listdir(directory)

        all_chunks = []

        for file in chunks:
            path_to_chunk = os.path.join(directory, file)
            chunks_df = pd.read_parquet(path_to_chunk, engine="pyarrow")
            all_chunks.append(chunks_df)

        self.all_txns = pd.concat(all_chunks, ignore_index=True).sort_values("unix_time") \
                                .reset_index(drop=True)


    def write_built_data(self):
        """
        Запись собранного из чанков датафрейма в целый файл.
        Пишем в две директории: history/<своя_папка> и latest/ 
        """
        # Создаем полный путь для записи в папку текущей генерации
        path_history = os.path.join(self.directory, "legit_txns.parquet")
        all_txns = self.all_txns
        all_txns.to_parquet(path_history, engine="pyarrow")

        # Берем полный путь из категории "generated" и по ключу latest
        # Это путь для последних созданных легальных транзакций
        category = self.category
        key_latest = self.key_latest
        data_paths = self.data_paths
        path_latest = data_paths[category][key_latest]
        all_txns.to_parquet(path_latest, engine="pyarrow")


    def reset_counters(self):
        """
        Сброс счетчиков: txns_counter, clients_counter,
        chunks_counter
        """
        self.txns_counter = 0
        self.clients_counter = 0
        self.chunks_counter = 0
        
        