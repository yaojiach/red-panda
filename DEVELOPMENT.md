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

## Build Documentation Locally

```sh
cd docs
rm -r build
sphinx-apidoc -f -o source ../red_panda
make html
```
