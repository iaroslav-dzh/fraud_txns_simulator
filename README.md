
# Симулятор фрод транзакций

## Указатель

- [О чем этот проект](#о-чем-этот-проект)
- [Установка зависимостей проекта](#установка-зависимостей-проекта)
	- [Клонирование репозитория](#клонирование-репозитория)
	- [Установка poetry](#установка-poetry)
		- [Windows (Powershell)](#windows-powershell)
		- [Linux, macOS, Windows (WSL)](#linux-macos-windows-wsl)
	- [Создание виртуальной среды](#создание-виртуальной-среды)
	- [Активация виртуальной среды](#активация-виртуальной-среды)
		- [Windows (powershell)](#windows-powershell-1)
		- [Линукс](#линукс)
- [Возможные ошибки при настройке окружения](#возможные-ошибки-при-настройке-окружения)
	- [Ошибка  `returned non-zero exit status 9009`](#ошибка--returned-non-zero-exit-status-9009)
	- [`UnauthorizedAccess Error` в powershell](#unauthorizedaccess-error-в-powershell)
- [Другие инструкции](#другие-инструкции)
	- [Подключение poetry env как kernel в Jupyter](#подключение-poetry-env-как-kernel-в-jupyter)
- [Запуск генератора](#запуск-генератора)


## О чем этот проект

1. Генерация синтетических банковских транзакций: легальных и фрода. 
2. Веб-приложение для просмотра сгенерированных транзакций. Симуляция интерфейса антифрод системы. Писать планирую либо на фреймворке Streamlit либо на Dash. - **в разработке**  

Более развернутое описание проекта **[по ссылке](https://iaroslav-dzh.github.io/fraud_txns_simulator/01_%D0%93%D0%BB%D0%B0%D0%B2%D0%BD%D0%B0%D1%8F_%D1%81%D1%82%D1%80%D0%B0%D0%BD%D0%B8%D1%86%D0%B0_%D0%BF%D1%80%D0%BE%D0%B5%D0%BA%D1%82%D0%B0_af01.html)** - это сайт проекта. Там более подробное описание в целом и подробное описание генератора транзакций по разделам.  

**Генератор транзакций уже готов** и может быть запущен локально после установки зависимостей проекта, об этом далее.  

**Фрода два типа:** 
1. *compromised client fraud* - банковский аккаунт или карта клиента скомпрометированы.
2. дроп фрод - клиент является дропом, дропов два типа: которые просто посредники в переправке потока денег (*распределители*) и которые занимаются отмыванием через покупку товаров на присланные им деньги (*покупатели*)    

Фрод генерируется в соответствии с антифрод правилами. Т.е. транзакции генерируются с такими характеристиками, чтобы попадать под конкретные антифрод правила. В случае с *compromised client fraud* правило выбирается случайно и под него генерируется транзакция/и.  
<br><br>

## Установка зависимостей проекта

**Все зависимости**, включая модули проекта прописаны в `pyproject.toml` файле.
Предлагаю установить их при помощи менеджера зависимостей **[poetry](https://python-poetry.org/)**.  

### Клонирование репозитория

```bash
git clone https://github.com/iaroslav-dzh/fraud_txns_simulator.git
```  
<br>

### Установка poetry
<br>

#### Windows (Powershell)
```powershell
(Invoke-WebRequest -Uri https://install.python-poetry.org -UseBasicParsing).Content | py -
```  

При установке для **Windows** через powershell добавить путь ниже в `path` **для пользователя**
```
C:\Users\<ИМЯ ПОЛЬЗОВАТЕЛЯ>\AppData\Roaming\Python\Scripts
```
- [Как добавить путь в переменную среды PATH в Windows](https://remontka.pro/add-to-path-variable-windows/)  

После установки и добавления пути в `path` перезапустить терминал, затем проверить:
```
poetry --version
```  
<br>

#### Linux, macOS, Windows (WSL)
```bash
curl -sSL https://install.python-poetry.org | python3 -
```

При установке для **Линукс** или **macOS** добавить путь ниже в `PATH`
```bash
$HOME/.local/bin
```
- [Как добавить путь в PATH на **Линукс**](https://iaroslav-dzh.github.io/fraud_txns_simulator/installation/how_to_add_poetry_to_path_linux.html)  

После установки и добавления пути в `PATH` перезапустить терминал, затем проверить:
```
poetry --version
```    
<br><br>

### Создание виртуальной среды

- Создание среды, установка всех зависимостей, включая собственные модули согласно  файлу `pyproject.toml`
<br>

Перейти в директорию проекта(клонированного репозитория) и исполнить команду
```
poetry install
```    

Будет создана виртуальная среда и установлены все зависимости    
<br><br>

### Активация виртуальной среды

Чтобы войти в виртуальную среду где установлены все зависимости.  
Документация Poetry: [Activating the environment](https://python-poetry.org/docs/managing-environments/#activating-the-environment)  
<br>

#### Windows (powershell)

Находясь в директории проекта в терминале исполнить команду
```powershell
Invoke-Expression (poetry env activate)
```  

**ЛИБО**<br>

Исполнить
```
poetry env activate
```  

И затем скопировать вывод команды. Если это путь к скрипту, например:
```
"C:\Users\<ИМЯ_ПОЛЬЗОВАТЕЛЯ>\AppData\Local\pypoetry\Cache\virtualenvs\<НАЗВАНИЕ_СРЕДЫ>\Scripts\activate.ps1"
```  

И вставляем вывод в терминал, нажимаем Enter
```
C:\Users\<ИМЯ_ПОЛЬЗОВАТЕЛЯ>\AppData\Local\pypoetry\Cache\virtualenvs\<НАЗВАНИЕ_СРЕДЫ>\Scripts\activate.ps1
```  
<br>
#### Линукс

Находясь в директории проекта в терминале исполнить команду
```bash
eval $(poetry env activate)
```  

**ЛИБО**<br>

Исполнить
```
poetry env activate
```  

И затем скопировать вывод команды. Это может быть команда и путь, например:
```bash
source home/<ИМЯ_ПОЛЬЗОВАТЕЛЯ>/.cache/pypoetry/virtualenvs/<ИМЯ_СРЕДЫ>/activate
```  

Затем вставить вывод в терминал, нажимаем Enter 
<br><br>

## Возможные ошибки при настройке окружения
<br>
### Ошибка  `returned non-zero exit status 9009`

Poetry не может найти python.  

Если в консоли будет что-то типа:
```
Command '['C:\\Users\\<USER>\\AppData\\Local\\Microsoft\\WindowsApps\\python.EXE', '-EsSc', 'import sys; print(sys.executable)']' returned non-zero exit status 9009.
```  

Это исправляется командой:
```
poetry config virtualenvs.use-poetry-python true
```  

Теперь poetry использует python который использовался при ее установке.    
<br><br>

### `UnauthorizedAccess Error` в powershell 

Исполнить команду ниже. Для разрешения исполнения скриптов в текущей сессии powershell
```
Set-ExecutionPolicy RemoteSigned -Scope Process
```  

Справка:
- https://learn.microsoft.com/en-us/powershell/module/microsoft.powershell.security/set-executionpolicy?view=powershell-7.5  
<br><br>

## Другие инструкции

### Подключение poetry env как kernel в Jupyter

1. Зайти в виртуальную среду
2. Установить ipykernel `poetry add ipykernel`
3. Зарегистрировать poetry среду как ядро в Jupyter

```
python -m ipykernel install --user --name=poetry-env --display-name "Python (poetry-env)"
```

4. Запустить Jupyter Notebook (можно из Anaconda или терминала), и в списке ядер (kernels) выберfnm `Python (poetry-env)`.   
<br><br>

## Запуск генератора

Чтобы запустить генератор транзакций:
1. Войти в виртуальную среду проекта
2. Исполнить команду  

```
python run_generator.py
```  

У генератора транзакций могут быть заданы конфигурации через `yaml` файлы в папке `config/`. Подробнее об этом в [Основные конфигурации и запуск генератора](https://iaroslav-dzh.github.io/fraud_txns_simulator/07_generator_configs_and_start.html)



