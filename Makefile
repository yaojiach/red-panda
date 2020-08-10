build:
	pipenv run python setup.py sdist bdist_wheel
release:
	pipenv run python setup.py sdist bdist_wheel
	pipenv run python -m twine upload dist/*
unit-test:
	tox -e unit
integ-test:
	tox -e integ