# Contributing

yfinance relies on the community to investigate bugs and contribute code.

This is a quick short guide, full guide at https://ranaroussi.github.io/yfinance/development/index.html

## Branches

YFinance uses a two-layer branch model:

* **dev**: new features & most bug-fixes merged here, tested together, conflicts fixed, etc.
* **main**: stable branch where PIP releases are created.

## Running a branch

```bash
pip install git+ranaroussi/yfinance.git@dev  # <- dev branch
```

https://ranaroussi.github.io/yfinance/development/running.html

### I'm a GitHub newbie, how do I contribute code?

1. Fork this project. If already forked, remember to `Sync fork`

2. Implement your change in your fork, ideally in a specific branch

3. Create a [Pull Request](https://github.com/ranaroussi/yfinance/pulls), from your fork to this project. If addressing an Issue, link to it

https://ranaroussi.github.io/yfinance/development/code.html

## Documentation website

The new docs website is generated automatically from code. https://ranaroussi.github.io/yfinance/index.html

Remember to updates docs when you change code, and check docs locally.

https://ranaroussi.github.io/yfinance/development/documentation.html

## Git tricks

Help keep the Git commit history and [network graph](https://github.com/ranaroussi/yfinance/network) compact:

* got a long descriptive commit message? `git commit -m "short sentence summary" -m "full commit message"`

* combine multiple commits into 1 with `git squash`

* `git rebase` is your friend: change base branch, or "merge in" updates

https://ranaroussi.github.io/yfinance/development/code.html#git-stuff

## Unit tests

Tests have been written using the built-in Python module `unittest`. Examples:

* Run all tests: `python -m unittest discover -s tests`

https://ranaroussi.github.io/yfinance/development/testing.html

> See the [Developer Guide](https://ranaroussi.github.io/yfinance/development/contributing.html#GIT-STUFF) for more information.
