SHELL := /bin/bash
env_name = venv
env_path = ~/miniconda3/envs/${env_name}/bin

conda-install:
		conda create -n {env_name} python=3.8 -y
		${env_path}/pip install --upgrade pip
		${env_path}/pip install -r requirements.txt
		${env_path}/pre-commit install

install:
		python3 -m venv ./${env_name}
		./${env_name}/bin/python3 -m pip install --upgrade pip
		./${env_name}/bin/pip3 install -r requirements.txt
		./${env_name}/bin/pre-commit install

format:
		black .
		isort .

format-check:
		black . --check
		isort . --check-only

lint:
		flake8 . --select=E9, F63, F7, F82 --show-source
		flake8 . --exit-zero
