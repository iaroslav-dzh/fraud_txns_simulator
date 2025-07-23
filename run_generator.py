# Запуск генерации транзакций всех типов: легальные, compromised fraud, дроп фрод
from pathlib import Path

from data_generator.utils import load_configs
from data_generator.validator import ConfigsValidator 
from data_generator.runner.utils import make_dir_for_run
from data_generator.runner.legit import LegitRunner
from data_generator.runner.compr import ComprRunner
from data_generator.runner.drops import DropsRunner
from data_generator.recorder import AllTxnsRecorder

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


# Валидация основных конфигов перед началом генерации
cfg_validator = ConfigsValidator(base_cfg=base_cfg, legit_cfg=legit_cfg, \
                                 fraud_cfg=fraud_cfg, drop_cfg=drop_cfg)
cfg_validator.validate_all()

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


# Генерация фрода дропов распределителей 
dist_drops_runner = DropsRunner(base_cfg=base_cfg, legit_cfg=legit_cfg, \
                                time_cfg=time_cfg, fraud_cfg=fraud_cfg, \
                                drops_cfg=drop_cfg, run_dir=run_dir, \
                                drop_type="distributor")
dist_drops_runner.run()


# Генерация фрода дропов покупателей 
purch_drops_runner = DropsRunner(base_cfg=base_cfg, legit_cfg=legit_cfg, \
                                time_cfg=time_cfg, fraud_cfg=fraud_cfg, \
                                drops_cfg=drop_cfg, run_dir=run_dir, \
                                drop_type="purchaser")
purch_drops_runner.run()


# Сборка созданных транзакций в один датафрейм и запись в один файл в двух директориях
recorder = AllTxnsRecorder(base_cfg=base_cfg, legit_cfg=legit_cfg, compr_cfg=compr_cfg, \
                           drops_cfg=drop_cfg, run_dir=run_dir)

recorder.build_and_write()

latest_path = Path(base_cfg["data_paths"]["generated"]["latest"])
print(f"""\n
Generated files are located in {run_dir} - individual folder for this run.
And in {latest_path} - contains files of the last run only
\n""")
input("\nPress Enter to exit...")





