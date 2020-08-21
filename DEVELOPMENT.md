# Development Guide

## CDK Testing Infrastructure

CDK (TypeScript) is used to manage AWS infrastructure for integration tests.

```sh
cd cdk
npm run build
```

## Testing

```sh
# Unit tests
tox -e unit

# Integration tests, this will also create new AWS infrastructure.
tox -e integ

# Integration tests without creating stack (use existing)
tox -e integ -- --skip-cdk

# Run a single test
tox -e unit -- -k test_{name}
```

## Coverage

```sh
# Manually incrementally test coverage
tox -e clean
tox -e unit
tox -e integ -- --skip-cdk
tox -e report
# Running `tox` will clean existing coverage and only report unit test coverage

# Use make file
make e2e
```

## Build Documentation Locally

```sh
cd docs
rm -r build
sphinx-apidoc -f -o source ../red_panda
make html
```
