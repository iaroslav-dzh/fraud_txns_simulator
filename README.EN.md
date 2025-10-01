# Fraud Transactions Simulator

## Table of Contents

- [About this project](#about-this-project)
- [Project dependencies installation](#project-dependencies-installation)
	- [Cloning the repository](#cloning-the-repository)
	- [Installing poetry](#installing-poetry)
		- [Windows (Powershell)](#windows-powershell)
		- [Linux, macOS, Windows (WSL)](#linux-macos-windows-wsl)
	- [Creating a virtual environment](#creating-a-virtual-environment)
	- [Activating the virtual environment](#activating-the-virtual-environment)
		- [Windows (powershell)](#windows-powershell-1)
		- [Linux](#linux)
- [Possible errors when setting up the environment](#possible-errors-when-setting-up-the-environment)
	- [Error `returned non-zero exit status 9009`](#error-returned-non-zero-exit-status-9009)
	- [`UnauthorizedAccess Error` in powershell](#unauthorizedaccess-error-in-powershell)
- [Other instructions](#other-instructions)
	- [Connecting poetry env as a kernel in Jupyter](#connecting-poetry-env-as-a-kernel-in-jupyter)
- [Running the generator](#running-the-generator)


## About this project

I've implemented the generation of synthetic banking transactions in Python: legal and fraudulent, taking into account customer behavior and realistic scenarios.  
Transaction modeling. The resulting datasets can be used for testing fraud detection system rules.

**Stack:** Python (`pandas`, `numpy`, `geopandas`, `matplotlib`, `OOP`), `Poetry`, `Power BI` (data model, `DAX`)  

----------------------

1. Cleaned and transformed data for generation using `pandas`, `numpy`, `geopandas`.
2. Built a modular transaction generator using functions and **OOP**. Different parts of the generator logic are in separate files, functions, and classes, but there is a single entry point. Here’s a scheme:  
**[link](https://miro.com/app/board/uXjVJXMulTI=/?focusWidget=3458764636425951315)**
3. Generator configurations are stored in `YAML` files so they can be changed without editing code.
4. Documented the process in Jupyter notebooks — data preprocessing and demonstration of the main functions and classes of the generator. Also created a website describing the project, with a page for each section.
5. Loaded data into `Power BI`, built a data model and a dashboard with transaction information. Created visualizations, applied conditional formatting, wrote `DAX` measures. Video demo (2.5 min): **[link](https://rutube.ru/video/private/28d85101f267427e568ed850d8db0878/?p=vjJbbxQMAL7NUtrge6UC2g)**  
6. Project dependencies are defined in a `toml` file.

**Project results:**

-------------------

- The generated datasets can be used to test fraud detection rules.
- Improved skills in data processing with `pandas`, `numpy`, `geopandas`.
- Gained experience writing systematic and structured code. Components are separate, testable, yet integrated into a system. This is very useful for working with `ETL` and data processing.
- Learned to work with configuration files (`YAML`).
- Gained experience completing a large-scale project end-to-end: 14 Jupyter notebooks documenting the process, 2+ months of work, ~4000 lines of code in the generator module.

More detailed project description **[here](https://iaroslav-dzh.github.io/fraud_txns_simulator/index.html)** — the project website. It provides detailed explanations of the transaction generator by section.

**Two types of fraud:**

--------------------

1. *Compromised client fraud* — the client’s bank account or card is compromised.  
2. *Drop fraud* — the client acts as a drop. Drops can be of two types:  
   - *distributors* (intermediaries for money flow)  
   - *buyers* (launder money through purchases with transferred funds)  

Fraud is generated according to anti-fraud rules. Transactions are created with characteristics that match specific rules. For *compromised client fraud*, a rule is selected randomly and a transaction(s) is generated accordingly.  
<br><br>

## Project dependencies installation

-------------

**All dependencies**, including project modules, are specified in the `pyproject.toml` file.  
It is recommended to install them using the dependency manager **[poetry](https://python-poetry.org/docs/)**.  

### Cloning the repository

```bash
git clone https://github.com/iaroslav-dzh/fraud_txns_simulator.git
````

### Installing poetry

---

#### Windows (Powershell)

```powershell
(Invoke-WebRequest -Uri https://install.python-poetry.org -UseBasicParsing).Content | py -
```

When installing on **Windows** via Powershell, add the path below to the user `PATH`:

```
C:\Users\<USERNAME>\AppData\Roaming\Python\Scripts
```

* [How to add a path to the PATH environment variable in Windows](https://remontka.pro/add-to-path-variable-windows/)

After installation and adding the path to `PATH`, restart the terminal and check:

```
poetry --version
```

<br>

#### Linux, macOS, Windows (WSL)

```bash
curl -sSL https://install.python-poetry.org | python3 -
```

When installing on **Linux** or **macOS**, add the path below to `PATH`:

```bash
$HOME/.local/bin
```

* [How to add a path to PATH on **Linux**](https://iaroslav-dzh.github.io/fraud_txns_simulator/installation/how_to_add_poetry_to_path_linux.html)

After installation and adding the path to `PATH`, restart the terminal and check:

```
poetry --version
```

<br><br>

### Creating a virtual environment

* Creates the environment and installs all dependencies, including custom modules, from `pyproject.toml`.

  <br>

Go to the project directory (cloned repo) and run:

```
poetry install
```

This will create a virtual environment and install all dependencies. <br><br>

### Activating the virtual environment

To enter the virtual environment where dependencies are installed:
Poetry docs: [Activating the environment](https://python-poetry.org/docs/managing-environments/#activating-the-environment) <br>

#### Windows (powershell)

In the project directory, run:

```powershell
Invoke-Expression (poetry env activate)
```

**OR**<br>

Run:

```
poetry env activate
```

Then copy the command output. If it’s a script path, e.g.:

```
"C:\Users\<USERNAME>\AppData\Local\pypoetry\Cache\virtualenvs\<ENV_NAME>\Scripts\activate.ps1"
```

Paste the output in the terminal and press Enter:

```
C:\Users\<USERNAME>\AppData\Local\pypoetry\Cache\virtualenvs\<ENV_NAME>\Scripts\activate.ps1
```

<br>

#### Linux

In the project directory, run:

```bash
eval $(poetry env activate)
```

**OR**<br>

Run:

```
poetry env activate
```

Then copy the command output. This may be a command and a path, e.g.:

```bash
source home/<USERNAME>/.cache/pypoetry/virtualenvs/<ENV_NAME>/activate
```

Paste the output in the terminal and press Enter. <br><br>

## Possible errors when setting up the environment

---

### Error  `returned non-zero exit status 9009`

Poetry cannot find Python.

If you see something like:

```
Command '['C:\\Users\\<USER>\\AppData\\Local\\Microsoft\\WindowsApps\\python.EXE', '-EsSc', 'import sys; print(sys.executable)']' returned non-zero exit status 9009.
```

Fix with:

```
poetry config virtualenvs.use-poetry-python true
```

Now poetry will use the Python interpreter it was installed with. <br>

### `UnauthorizedAccess Error` in powershell

Run this command to allow script execution in the current Powershell session:

```
Set-ExecutionPolicy RemoteSigned -Scope Process
```

Docs:

* [https://learn.microsoft.com/en-us/powershell/module/microsoft.powershell.security/set-executionpolicy?view=powershell-7.5](https://learn.microsoft.com/en-us/powershell/module/microsoft.powershell.security/set-executionpolicy?view=powershell-7.5) <br><br>

## Other instructions

---

### Connecting poetry env as a kernel in Jupyter

1. Enter the virtual environment
2. Install ipykernel `poetry add ipykernel`
3. Register poetry env as a kernel in Jupyter:

```
python -m ipykernel install --user --name=poetry-env --display-name "Python (poetry-env)"
```

4. Launch Jupyter Notebook (from Anaconda or terminal) and in the kernel list select `Python (poetry-env)`. <br><br>

## Running the generator

To run the transaction generator:

1. Enter the project’s virtual environment
2. Run:

```
python run_generator.py
```

The generator can be configured using `yaml` files in the `config/` folder. More details in [Main configurations and running the generator](https://iaroslav-dzh.github.io/fraud_txns_simulator/07_generator_configs_and_start.html)

