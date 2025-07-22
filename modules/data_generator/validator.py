from scipy.stats import truncnorm, norm
import numpy as np
import pandas as pd

class ConfigsValidator:
    """
    Валидация некоторых значений в yaml конфиг файлах.
    ---------------
    Атрибуты:
    ---------
    base_cfg: dict. Конфиги из base.yaml
    legit_cfg: dict. Конфиги из legit.yaml
    fraud_cfg: dict. Конфиги из fraud.yaml
    drop_cfg: dict. Конфиги из drops.yaml
    total_clients: int. Общее кол-во имеющихся клиентов.
                   По умолчанию None.
    """
    def __init__(self, base_cfg, legit_cfg, fraud_cfg, drop_cfg):
        """
        base_cfg: dict. Конфиги из base.yaml
        legit_cfg: dict. Конфиги из legit.yaml
        fraud_cfg: dict. Конфиги из fraud.yaml
        drop_cfg: dict. Конфиги из drops.yaml
        """
        self.legit_cfg = legit_cfg
        self.base_cfg = base_cfg
        self.fraud_cfg = fraud_cfg
        self.drop_cfg = drop_cfg
        self.total_clients = None


    def get_total_clients(self):
        """
        Получить кол-во всех имеющихся клиентов.
        """
        if self.total_clients is not None:
            return self.total_clients
        
        data_paths = self.base_cfg["data_paths"]
        path = data_paths["clients"]["clients"] # путь к файлу со всеми клиентами
        clients = pd.read_parquet(path)
        self.total_clients = clients.shape[0]
        return self.total_clients


    def estimate_legit_clients(self):
        """
        Рассчитать кол-во клиентов нужное для генерации легальных
        транзакций.
        """
        leg_txn_num = self.legit_cfg["txn_num"]
        total_txns = leg_txn_num["total_txns"]
        avg_num_per_client = leg_txn_num["avg_txn_num"]

        # Делим кол-во лег. транз-ций на среднее кол-во на клиента
        # Все взято из legit.yaml конфига
        return int(total_txns / avg_num_per_client)


    def model_legit_txns_num_dist(self):
        """
        Смоделировать распределение общего кол-ва легальных
        транз-ций на основании обрезанного нормального распределения
        кол-ва транзакций на одного клиента.
        """
        leg_txn_num = self.legit_cfg["txn_num"]
        avg_num_per_client = leg_txn_num["avg_txn_num"]
        txn_num_std = leg_txn_num["txn_num_std"]
        low_bound = leg_txn_num["low_bound"]
        up_bound = leg_txn_num["up_bound"]

        est_clients = self.estimate_legit_clients()

        # параметры отдельной транзакции на клиента
        a = (low_bound - avg_num_per_client) / txn_num_std
        b = (up_bound - avg_num_per_client) / txn_num_std
        per_client_dist = truncnorm(a=a, b=b, loc=avg_num_per_client, \
                                    scale=txn_num_std)

        mean_sum = est_clients * per_client_dist.mean()
        std_sum = np.sqrt(est_clients) * per_client_dist.std()

        return norm(loc=mean_sum, scale=std_sum)
    

    def estimate_legit_txns_max(self):
        """
        Расчет теоритического максимального кол-ва легальных транз.
        которое может быть сгенерировано учитывая конфиги в legit.yaml
        и от выставленного процента допустимой ошибки.
        """
        overflow_prob = self.base_cfg["validator"]["rates"]["overflow_prob"]

        upper_lim = 1 - overflow_prob
        legit_num_dist = self.model_legit_txns_num_dist()
        return round(legit_num_dist.ppf(upper_lim))


    def estimate_compr_clients(self):
        """
        Расчет необходимого кол-ва клиентов для compromised client
        фрода. Исходя из rate-а и теоритически макс. кол-ва легальных 
        транз-ций
        """
        fraud_rate = self.fraud_cfg["fraud_rates"]["total"]
        compr_share = self.fraud_cfg["fraud_rates"]["compr_client"]
        legit_max = self.estimate_legit_txns_max()

        # подсчет количества транзакций равных 1% от всех транзакций
        # т.к. число фрод транзакций зависит от легального, то считаем основываясь возможном
        # количестве легальных транзакций и fraud rate
        one_perc = round(legit_max / ((1 - fraud_rate) * 100))
        # Абсолютное кол-во всего фрода
        fraud_abs = one_perc * fraud_rate * 100

        # Абсолютное кол-во фрод транзакций умножаем на долю транзакций compromised фрода
        return round(fraud_abs * compr_share)
    

    def validate_legit_txn_num(self):
        """
        Валидация желаемого кол-ва легальных транзакций.
        """
        leg_txn_num = self.legit_cfg["txn_num"]
        total_txns = leg_txn_num["total_txns"]
        
        total_clients = self.get_total_clients()
        legit_clients = self.estimate_legit_clients()

        if legit_clients > total_clients:
            raise ValueError(f"""Desired legit txns number is too large: {total_txns}.
            Clients number needed for generation: {legit_clients}.
            Available clients: {total_clients}.
            Either reduce legit txns number or increase avg
            txn number per client  or both.""")

        print("Legit txns number config is OK")


    def validate_comp_rate(self):
        """
        Валидировать значение compr_client из fraud.yaml.
        Процент compromised client фрода от всего фрода.
        """
        est_clients = self.estimate_legit_clients()

        clients_count = self.estimate_compr_clients()

        if clients_count > est_clients:
            raise ValueError(f"""Estimated maximum possible number of clients that might be needed to 
            generate 'compromised client' fraud transactions: {clients_count}.
            Estimated number of clients required for generating legitimate transactions: {est_clients}.

            The number of compromised fraud clients cannot be higher than the number required
            for legitimate transaction generation.

            Please either:
            1. Decrease the 'compr_client' rate and/or the overall fraud rate in configs/fraud.yaml
            2. Reduce the average number of legitimate transactions per client in legit.yaml
            """)
        
        print("Compromised fraud rate config is OK")


    def validate_drops_rate(self):
        """
        """
        drop_cfg = self.drop_cfg
        fraud_rate = self.fraud_cfg["fraud_rates"]["total"]
        drop_rates = self.fraud_cfg["fraud_rates"]["drops"]
        dist_rate = drop_rates["distributor"]
        purch_rate = drop_rates["purchaser"]

        out_lim_dist = drop_cfg["distributor"]["out_lim"]
        out_lim_purch = drop_cfg["purchaser"]["out_lim"]

        legit_max = self.estimate_legit_txns_max()

        one_perc = round(legit_max / ((1 - fraud_rate) * 100))
        fraud_abs = one_perc * fraud_rate * 100

        dist_drops = round(fraud_abs * dist_rate / out_lim_dist)
        purch_drops = round(fraud_abs * purch_rate / out_lim_purch)
        total_drops = dist_drops + purch_drops

        legit_clients = self.estimate_legit_clients()
        compr_clients = self.estimate_compr_clients()
        total_clients = self.get_total_clients()

        remainder = max(0, total_clients - (legit_clients + compr_clients))

        if remainder < total_drops:
            raise ValueError(f"""Total clients number needed for drop fraud generation
            exceeds the available clients number.
            Clients number needed for drops: {total_drops}
            Available clients: {remainder}
            Legit txns clients: {legit_clients}
            Compr fraud clients: {compr_clients}
            Please do one or both:
            1. Reduce legit txns number.
            2. Reduce total fraud rate.""")
        
        print("Drop fraud rate config is OK")


    def assert_legit_time_limits(self):
        """
        Проверка минимальных лимитов времени между транз-циями в 
        min_intervals из legit.yaml
        """
        min_inter = self.legit_cfg["time"]["min_intervals"]
        offline_time_diff = min_inter["offline_time_diff"]
        online_time_diff = min_inter["online_time_diff"]
        general_diff = min_inter["general_diff"]

        assert offline_time_diff > general_diff, \
            f"""offline_time_diff must not be lower than general_diff. 
            {offline_time_diff} vs {general_diff} Check configs in legit.yaml"""
        
        assert offline_time_diff > online_time_diff, \
            f"""offline_time_diff must not be lower than online_time_diff.
            {offline_time_diff} vs {online_time_diff} Check configs in legit.yaml"""
        
        assert general_diff > online_time_diff, \
            f"""general_diff must not be lower than online_time_diff.
            {general_diff} vs {online_time_diff} Check configs in legit.yaml"""    

        print("Legit min time intervals config is OK")


    def validate_all(self):
        """
        Валидация всех конфигов, которые могут быть валидированы
        в этом классе.
        """
        print(f"Main configs validation started")
        self.validate_legit_txn_num()
        self.validate_comp_rate()
        self.validate_drops_rate()
        self.assert_legit_time_limits()









        






