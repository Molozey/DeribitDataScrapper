build-wheel:
	python setup.py bdist_wheel
	twine upload -r pypi dist/* -u __token__ -p pypi-AgEIcHlwaS5vcmcCJDE4YTQ2OTNhLTI2MDYtNGRiYS1hZmEzLTBmYTViMjk2N2UxOQACKlszLCJiMjEyMTZhMC0yNTYyLTRhYWEtYjRmYi03YTVkYWNjYzdhMWIiXQAABiD6ytC1yKFkuVNe4oMlTrXMqbyNAectL6dfPcPah2difg


activate-dev-venv:
	pip install -e .

remove-pip-packages:
	pip uninstall -y -r <(pip freeze)

twine-upload:
	twine upload --repository pypi dist/*
