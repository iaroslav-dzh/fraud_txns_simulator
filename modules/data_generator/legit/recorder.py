import os
import pandas as pd
from datetime import datetime

class LegitTxnsRecorder:
    """
    category: str. Ключ к категории директорий в base.yaml.
                       Ключ это одна из папок в data/
                       Тут будут храниться сгенерированные данные.
    key_latest: str. Ключ в base.yaml для пути к файлу в директории
                data/generated/latest/. Это для записи созданных
                транз-ций как последних сгенерированных.
    key_history: str. Ключ в base.yaml для пути к директории
                    с историей генерации данных data/generated/history/
    prefix: str. Для названия индивидуальной папки внутри dir_category
                Например 'legit_'
    directory: str. Путь к директории в папке data/generated/history
               куда записывать чанки и собранный из них файл.
    all_txns: pd.DataFrame. Все сгенерированные транзакции. По умолчанию None.
    """
    def __init__(self, configs):
        """
        configs:
        """
        self.in_chunks_gen = configs.in_chunks_gen
        self.total_clients = configs.clients.shape[0]
        self.category = configs.dir_category
        self.key_latest = configs.key_latest
        self.key_history = configs.key_history
        self.data_paths = configs.data_paths
        self.prefix = configs.dir_prefix
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
        prefix = self.prefix
        datetime_suffix = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        folder_name = prefix + datetime_suffix
        path = os.path.join(directory, folder_name)

        if os.path.exists(path):
            return
        
        os.mkdir(path)
        self.directory = path


    def name_the_chunk(self):
        """
        """
        self.chunks_counter += 1
        prefix = self.prefix
        chunks_counter = self.chunks_counter
        return f"{prefix}{chunks_counter:03d}.parquet"


    @property
    def to_chunk(self):
        """
        """
        chunk_size = self.in_chunks_gen["chunk_size"]
        txns_counter = self.txns_counter

        if txns_counter % chunk_size == 0:
            return True
        
        return False


    def record_chunk(self, txn, txns_num):
        """
        Запись транзакций в parquet в папку где хранятся созданные транзакции
        от всех запусков генератора.
        --------
        txn: dict. Созданная транзакция.
        category: str. Ключ к категории директорий в base.yaml. Например 'generated'.
        txns_num: int. Кол-во созданных транз-ций текущего клиента.
        """
        # data_paths = self.data_paths
        directory = self.directory
        clients_counter = self.clients_counter
        total_clients = self.total_clients
        client_txns = self.client_txns 

        self.txns_chunk.append(txn)

        # Проверка по кол-ву. Если кол-во транз-ций равно размеру чанка
        # то пишем чанк в файл
        if self.to_chunk:
            chunk = pd.DataFrame(self.txns_chunk)
            chunk_name = self.name_the_chunk()
            full_path = os.path.join(directory, chunk_name)

            chunk.to_parquet(full_path, engine="pyarrow")
            self.txns_chunk.clear() # сброс чанка записанного в файл
            return
        # print(f"""clients_counter: {clients_counter}
        # total_clients: {total_clients}
        # txns_num: {txns_num}
        # client_txns: {len(client_txns)}
        # """)
        # Эти два if-а проверяют закончилась ли генерация транз-ций
        # Т.е. последний ли это клиент и последняя ли это транз-ция для него
        # Для случаев когда генерация закончена, но чанк не набран до конца.
        if clients_counter != total_clients:
            return
        if txns_num != len(client_txns):
            return
        # print("record_chunk #2")
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
        """
        self.txns_counter = 0
        self.clients_counter = 0
        self.chunks_counter = 0



    # def handle_client_txns(self, txn, mode):
    #     """
    #     txn: list.
    #     """
    #     if mode == "append":
    #         self.client_txns.append(txn)
    #     elif mode == "clear":
    #         self.client_txns.clear()


    # def record_all(self, category, key_latest, key_history, txns_num):
    #     """
    #     Запись всех созданных транзакций в parquet.
    #     """
    #     data_paths = self.data_paths
    #     path_latest = data_paths[category][key_latest]
    #     path_history = data_paths[category][key_history]
    #     clients_counter = self.clients_counter
    #     total_clients = self.total_clients
    #     client_txns = self.client_txns 


    #     # Эти два if-а проверяют закончилась ли генерация транз-ций
    #     # Т.е. последний ли это клиент и последняя ли это транз-ция для него
    #     if clients_counter != total_clients:
    #         return
    #     if txns_num != len(client_txns):
    #         return
        
    #     all_txns = pd.DataFrame(self.all_txns)
    #     all_txns.to_parquet(path_latest, engine="pyarrow")
    #     all_txns.to_parquet(path_history, engine="pyarrow")


    # def record_txns(self, category, txns_num):
    #     """
    #     """
    #     key_latest = self.key_latest
    #     key_history = self.key_history

    #     self.record_all(category=category, key_latest=key_latest,\
    #                    key_history=key_history, txns_num=txns_num)
        
        