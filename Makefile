release:
	pipenv run python setup.py sdist bdist_wheel
	pipenv run python -m twine upload dist/*