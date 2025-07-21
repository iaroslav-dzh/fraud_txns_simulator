import os

from data_generator.utils import load_configs
from data_generator.validator import ConfigsValidator 
from data_generator.runner.utils import make_dir_for_run
from data_generator.runner.legit import LegitRunner


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


cfg_validator = ConfigsValidator() # <--------------------------- configure
run_dir = make_dir_for_run(base_cfg=base_cfg)

legit_runner = LegitRunner(base_cfg=base_cfg, legit_cfg=legit_cfg, \
                           time_cfg=time_cfg, run_dir=run_dir)
legit_runner.run()



