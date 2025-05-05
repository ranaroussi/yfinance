# Contributing

> [!NOTE]
> This is a brief guide to contributing to yfinance.
> For more information See the [Developer Guide](https://ranaroussi.github.io/yfinance/development) for more information.

## Changes

The list of changes can be found in the [Changelog](https://github.com/ranaroussi/yfinance/blob/main/CHANGELOG.rst)

## Running a branch

```bash
pip install git+ranaroussi/yfinance.git@dev # dev branch
```

For more information, see the [Developer Guide](https://ranaroussi.github.io/yfinance/development/running.html).

## Branches

YFinance uses a two-layer branch model:

* **dev**: new features & some bug-fixes merged here, tested together, conflicts fixed, etc.
* **main**: stable branch where PIP releases are created.

> [!NOTE]
> By default, branches target **main**, but most contributions should target **dev**. 
> Direct merges to **main** are allowed if:
> * `yfinance` is massively broken
> * Part of `yfinance` is broken, and the fix is simple and isolated
> * Not updating the code (e.g. docs)

> [!NOTE]
> For more information, see the [Developer Guide](https://ranaroussi.github.io/yfinance/development/branches.html).

### I'm a GitHub newbie, how do I contribute code?

1. Fork this project. If already forked, remember to `Sync fork`

2. Implement your change in your fork, ideally in a specific branch

3. Create a Pull Request, from your fork to this project. If addressing an Issue, link to it

> [!NOTE]
> See the [Developer Guide](https://ranaroussi.github.io/yfinance/development/contributing.html) for more information.

### [How to download & run a GitHub version of yfinance](#Running-a-branch)

## Documentation website

The new docs website [ranaroussi.github.io/yfinance/index.html](https://ranaroussi.github.io/yfinance/index.html) is generated automatically from code. 

> [!NOTE]
> See the [Developer Guide](https://ranaroussi.github.io/yfinance/development/documentation.html) for more information
> Including how to build and run the docs locally.

## Unit tests

Tests have been written using the built-in Python module `unittest`. Examples:

#### Run all tests: `python -m unittest discover -s tests`

> [!NOTE]
>
> See the [Developer Guide](https://ranaroussi.github.io/yfinance/development/testing.html) for more information.

## Git stuff
### commits

To keep the Git commit history and [network graph](https://github.com/ranaroussi/yfinance/network) compact please follow these two rules:

* For long commit messages use this: `git commit -m "short sentence summary" -m "full commit message"`

* `squash` tiny/negligible commits back with meaningful commits, or to combine successive related commits

> [!NOTE]
> See the [Developer Guide](https://ranaroussi.github.io/yfinance/development/contributing.html#GIT-STUFF) for more information.