# Запуск генерации легальных транзакций

from data_generator.legit.txns import gen_multiple_legit_txns
from data_generator.legit.build.config import LegitConfigBuilder
from data_generator.legit.recorder import LegitTxnsRecorder
from data_generator.runner.utils import notifier


class LegitRunner:
    """
    Запуск генератора легальных транзакций.
    ----------
    Атрибуты:
    ----------
    cfg_builder: LegitConfigBuilder.
    configs: LegitCfg. Конфиги и данные для генерации легальных транзакций.
    txn_recorder: LegitTxnsRecorder. Запись легальных транзакций в файл.
    """
    def __init__(self, base_cfg, legit_cfg, time_cfg, run_dir):
        """
        base_cfg: dict. Конфиги из base.yaml
        legit_cfg: dict. Конфиги из legit.yaml
        time_cfg: dict. Конфиги из time.yaml
        run_dir: str. Название директории для хранения сгенерированных
                 данных текущей генерации.
        """
        self.cfg_builder = LegitConfigBuilder(base_cfg=base_cfg, legit_cfg=legit_cfg, \
                                              time_cfg=time_cfg, run_dir=run_dir)
        self.configs = self.cfg_builder.build_cfg()
        self.txn_recorder = LegitTxnsRecorder(configs=self.configs)


    @notifier
    def run(self):
        """
        Запуск генератора.
        """
        configs = self.configs
        txn_recorder = self.txn_recorder
        # self.cfg_builder.make_dir() # legit папка под текущую генерацию

        gen_multiple_legit_txns(configs=configs, txn_recorder=txn_recorder)






