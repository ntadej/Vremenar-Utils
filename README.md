# Vremenar API Utilities

[![Homepage][web-img]][web] [![Latest release][release-img]][release]
[![License][license-img]][license]
[![Continuous Integration][ci-img]][ci]
[![codecov.io][codecov-img]][codecov] [![CodeFactor][codefactor-img]][codefactor]
[![pre-commit][pre-commit-img]][pre-commit]

A collection of utilities for the [Vremenar API](https://github.com/ntadej/Vremenar-API).

## Installation and running

### uv

This project uses [uv](https://github.com/astral-sh/uv) to track dependencies.
For basic development setup run

```shell
uv sync
```

For production setup run

```shell
uv sync --no-dev
```

## Contributing

### pre-commit

This project uses `pre-commit`. To setup, run

```shell
pre-commit install
```

To check all files run

```shell
pre-commit run --all
```

## Copyright info

Copyright (C) 2020-2025 Tadej Novak

This project may be used under the terms of the
GNU Affero General Public License version 3.0 as published by the
Free Software Foundation and appearing in the file [LICENSE](LICENSE).

[web]: https://vremenar.app
[release]: https://github.com/ntadej/Vremenar-Utils/releases/latest
[license]: https://github.com/ntadej/Vremenar-Utils/blob/main/LICENSE
[ci]: https://github.com/ntadej/Vremenar-Utils/actions
[codecov]: https://codecov.io/github/ntadej/Vremenar-Utils?branch=main
[codefactor]: https://www.codefactor.io/repository/github/ntadej/vremenar-utils
[pre-commit]: https://results.pre-commit.ci/latest/github/ntadej/Vremenar-Utils/main
[web-img]: https://img.shields.io/badge/web-vremenar.app-yellow.svg
[release-img]: https://img.shields.io/github/release/ntadej/Vremenar-Utils.svg
[license-img]: https://img.shields.io/github/license/ntadej/Vremenar-Utils.svg
[ci-img]: https://github.com/ntadej/Vremenar-Utils/workflows/Continuous%20Integration/badge.svg
[codecov-img]: https://codecov.io/github/ntadej/Vremenar-Utils/coverage.svg?branch=main
[codefactor-img]: https://www.codefactor.io/repository/github/ntadej/vremenar-utils/badge
[pre-commit-img]: https://results.pre-commit.ci/badge/github/ntadej/Vremenar-Utils/main.svg
