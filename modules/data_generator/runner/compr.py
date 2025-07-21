# Запуск генерации транзакций compromised фрода

from data_generator.fraud.compr.build.config import ComprConfigBuilder
from data_generator.fraud.recorder import FraudTxnsRecorder
from data_generator.fraud.compr.txndata import FraudTxnPartData, TransAmount
from data_generator.fraud.compr.txns import gen_multi_fraud_txns
from data_generator.runner.utils import notifier

class ComprRunner:
    """
    Запуск генерации транзакций compromised фрода.
    ---------
    Атрибуты:
    ---------
    cfg_builder: LegitConfigBuilder.
    configs: LegitCfg. Конфиги и данные для генерации легальных транзакций.
    part_data: FraudTxnPartData. Генерация части данных транзакции:
               мерчант, геопозиция, город, IP адрес и др.
    fraud_amts: TransAmount. Генерация суммы транзакций.
    txn_recorder: LegitTxnsRecorder. Запись легальных транзакций в файл.
    """
    def __init__(self, base_cfg, legit_cfg, time_cfg, fraud_cfg, compr_cfg, run_dir):
        """
        base_cfg: dict. Конфиги из base.yaml
        legit_cfg: dict. Конфиги из legit.yaml
        time_cfg: dict. Конфиги из time.yaml
        fraud_cfg: dict. Общие конфиги фрода из fraud.yaml
        compr_cfg: dict. Конфиги для compromised фрода из compr.yaml
        run_dir: str. Название директории для хранения сгенерированных
                 данных текущей генерации.
        """
        self.cfg_builder = ComprConfigBuilder(base_cfg=base_cfg, legit_cfg=legit_cfg, \
                                              time_cfg=time_cfg, fraud_cfg=fraud_cfg, \
                                              compr_cfg=compr_cfg, run_dir=run_dir)
        self.configs = self.cfg_builder.build_cfg()
        self.part_data = FraudTxnPartData(configs=self.configs)
        self.fraud_amts = TransAmount(configs=self.configs)
        self.txn_recorder = FraudTxnsRecorder(configs=self.configs)


    @notifier(text="Compromised client fraud generation")
    def run(self):
        """
        Запуск генератора.
        """
        configs = self.configs
        part_data = self.part_data
        fraud_amts = self.fraud_amts
        txn_recorder = self.txn_recorder


        gen_multi_fraud_txns(configs=configs, part_data=part_data, fraud_amts=fraud_amts, \
                             txn_recorder=txn_recorder)