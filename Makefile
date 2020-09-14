build:
	pipenv run python setup.py sdist bdist_wheel
release:
	rm -r dist
	pipenv run python setup.py sdist bdist_wheel
	pipenv run python -m twine upload dist/*
unit-test:
	tox -e unit
integ-test:
	tox -e integ
e2e:
	tox -e clean
	tox -e unit
	tox -e integ -- --skip-cdk
	tox -e report