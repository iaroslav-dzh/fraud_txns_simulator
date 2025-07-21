import os

from data_generator.utils import load_configs
from data_generator.validator import ConfigsValidator 
from data_generator.runner.utils import make_dir_for_run
from data_generator.runner.legit import LegitRunner
from data_generator.runner.compr import ComprRunner


# Общие настройки
base_cfg = load_configs("./config/base.yaml")
# Настройки легальных транзакций
legit_cfg = load_configs("./config/legit.yaml")
# Общие настройки фрода
fraud_cfg = load_configs("./config/fraud.yaml")
# Настройки compromised client фрода
compr_cfg = load_configs("./config/compr.yaml")
# Настройки времени
time_cfg = load_configs("./config/time.yaml")
# Настройки дроп фрода
drop_cfg = load_configs("./config/drops.yaml")

cfg_validator = ConfigsValidator(base_cfg=base_cfg, legit_cfg=legit_cfg, \
                                 fraud_cfg=fraud_cfg, drop_cfg=drop_cfg)

# Создаем папку под файлы текущей генерации
run_dir = make_dir_for_run(base_cfg=base_cfg)

# Генерация легальных транзакций
legit_runner = LegitRunner(base_cfg=base_cfg, legit_cfg=legit_cfg, \
                           time_cfg=time_cfg, run_dir=run_dir)

legit_runner.run()

# Генерация compromised client fraud транзакций

compr_runner = ComprRunner(base_cfg=base_cfg, legit_cfg=legit_cfg, \
                           time_cfg=time_cfg, fraud_cfg=fraud_cfg, \
                           compr_cfg=compr_cfg, run_dir=run_dir)
compr_runner.run()



