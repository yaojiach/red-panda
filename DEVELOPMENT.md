# Development Guide

## CDK Testing Infrastructure

```sh
cd cdk
npm run build
```

## Testing

```sh
# Unit tests
tox -e unit

# Integration tests
tox -e integ

# Integration tests without creating stack (use existing)
tox -e integ -- --skip-cdk

# Run a single test
tox -e unit -- -k test_groupby_distinct
```

## Documentation

```sh
cd docs
rm -r build
sphinx-apidoc -f -o source ../red_panda
make html
```
