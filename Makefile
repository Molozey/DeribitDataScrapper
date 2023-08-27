build-wheel:
	python setup.py bdist_wheel
	twine upload -r pypi dist/* -u __token__ -p "$PYPI_TOKEN"


activate-dev-venv:
	pip install -e .

remove-pip-packages:
	pip uninstall -y -r <(pip freeze)

twine-upload:
	twine upload --repository pypi dist/*
