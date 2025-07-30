# Запуск генерации транзакций дропов

from data_generator.fraud.drops.build.config import DropConfigBuilder
from data_generator.fraud.drops.build.builder import DropBaseClasses
from data_generator.fraud.recorder import FraudTxnsRecorder
from data_generator.fraud.drops.txns import CreateDropTxn
from data_generator.fraud.drops.simulator import DropSimulator
from data_generator.runner.utils import spinner_decorator


class DropsRunner:
    """
    Запуск генерации транзакций дропов указанного типа.
    ---------
    Атрибуты:
    ---------
    base_cfg: dict. Конфиги из base.yaml
    cfg_builder: DropConfigBuilder.
    drop_type: str. Тип дропа: 'distributor' или 'purchaser'.
    text: str. Текст для вставки в спиннер.
    configs: DropDistributorCfg | DropPurchaserCfg. 
             Конфиги и данные для генерации дроп транзакций.
             По умолчанию None. Создается при вызове self.build_sim().
    base: DropBaseClasses. Создатель основных классов для генерации
          дроп фрода. По умолчанию None. Создается при вызове self.build_sim().
    txn_recorder: FraudTxnsRecorder. Запись транзакций в файл.
                  По умолчанию None. Создается при вызове self.build_sim().
    drop_sim: DropSimulator. Генератор дроп фрода. По умолчанию None. 
              Создается при вызове self.build_sim().
    """
    def __init__(self, base_cfg, legit_cfg, time_cfg, fraud_cfg, drops_cfg, run_dir, drop_type):
        """
        base_cfg: dict. Конфиги из base.yaml
        legit_cfg: dict. Конфиги из legit.yaml
        time_cfg: dict. Конфиги из time.yaml
        fraud_cfg: dict. Общие конфиги фрода из fraud.yaml
        drops_cfg: dict. Конфиги для дроп фрода из drops.yaml
        run_dir: str. Название директории для хранения сгенерированных
                 данных текущей генерации.
        """
        self.base_cfg = base_cfg
        self.cfg_builder = DropConfigBuilder(base_cfg=base_cfg, legit_cfg=legit_cfg, time_cfg=time_cfg, \
                                             fraud_cfg=fraud_cfg, drop_cfg=drops_cfg, run_dir=run_dir)
        self.drop_type = drop_type
        self.text = f"{drop_type.capitalize()} drops generation"
        self.configs = None
        self.base = None
        self.txn_recorder = None
        self.drop_sim = None
        

    def build_sim(self):
        """
        Создать объект DropSimulator.
        """
        drop_type = self.drop_type
        base_cfg = self.base_cfg

        if drop_type == "distributor":
            self.configs = self.cfg_builder.build_dist_cfg()
        elif drop_type == "purchaser":
            self.configs = self.cfg_builder.build_purch_cfg()

        self.base = DropBaseClasses(drop_type=drop_type, configs=self.configs)
        self.base.build_all() # Создать объекты основных дроп классов
        self.txn_recorder = FraudTxnsRecorder(configs=self.configs)
        create_txn = CreateDropTxn(configs=self.configs, base=self.base)

        self.drop_sim = DropSimulator(base_cfg=base_cfg, configs=self.configs, base=self.base, \
                                      create_txn=create_txn, txn_recorder=self.txn_recorder)
        
        
    @spinner_decorator
    def run(self):
        """
        Создать объект DropSimulator и запустить процесс генерации дроп фрода
        для дропов указанного типа.
        """
        self.build_sim()
        self.drop_sim.run()

