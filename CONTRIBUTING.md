# Contributing

## Changes

The list of changes can be found in the [Changelog](https://github.com/ranaroussi/yfinance/blob/main/CHANGELOG.rst)

## Branches

To support rapid development without breaking stable versions, this project uses a two-layer branch model: ![image](https://private-user-images.githubusercontent.com/96923577/269063055-5afe5e2b-a43c-4a64-a736-a9e57fb5fe70.png?jwt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3NDYxNjkxODAsIm5iZiI6MTc0NjE2ODg4MCwicGF0aCI6Ii85NjkyMzU3Ny8yNjkwNjMwNTUtNWFmZTVlMmItYTQzYy00YTY0LWE3MzYtYTllNTdmYjVmZTcwLnBuZz9YLUFtei1BbGdvcml0aG09QVdTNC1ITUFDLVNIQTI1NiZYLUFtei1DcmVkZW50aWFsPUFLSUFWQ09EWUxTQTUzUFFLNFpBJTJGMjAyNTA1MDIlMkZ1cy1lYXN0LTElMkZzMyUyRmF3czRfcmVxdWVzdCZYLUFtei1EYXRlPTIwMjUwNTAyVDA2NTQ0MFomWC1BbXotRXhwaXJlcz0zMDAmWC1BbXotU2lnbmF0dXJlPTZkYWJlZDQyNWMwNTBmY2M5NDY4YmQ2OWM4ZmYzYzg2NThiNGEzMzkzN2NlMjNiZWYwMGM0Mzg5NzVlOWQ4YzImWC1BbXotU2lnbmVkSGVhZGVycz1ob3N0In0.ZK4rk7xa0w1U4KCkvv4hUKdWjuxeleuMYvZ3se6V33I) ([inspiration](https://miro.medium.com/max/700/1*2YagIpX6LuauC3ASpwHekg.png))

* **dev** : new features & some bug-fixes merged here, tested together, conflicts fixed, etc. Once stable, can merge to ...

* **main**: stable branch. Where PIP releases are created


Most of the time you want to target the **dev** branch, but default is **main** so remember to switch to **dev**.

**Exception:** can straight merge into **main** if:

* yfinance is massively broken

* or part of yfinance is broken and the fix is simple and clearly doesn't affect anything else

* or not editing part of the code (e.g. editing this file)



### I'm a GitHub newbie, how do I contribute code?

1. Fork this project. If already forked, remember to `Sync fork`

2. Implement your change in your fork, ideally in a specific branch

3. Create a Pull Request, from your fork to this project. If addressing an Issue, link to it


### [How to download & run a GitHub version of yfinance](https://github.com/ranaroussi/yfinance/discussions/1080)

## Documentation website

The new docs website [ranaroussi.github.io/yfinance/index.html](https://ranaroussi.github.io/yfinance/index.html) is generated automatically from code. When you fetch a branch or PR, probably it also has changes to docs - to generate and view this:

```bash
pip install -r requirements.txt
pip install Sphinx==8.0.2 pydata-sphinx-theme==0.15.4 Jinja2==3.1.4 sphinx-copybutton==0.5.2
sphinx-build -b html doc/source doc/_build/html
python -m http.server -d ./doc/_build/html
# open "localhost:8000" in browser
```

## Unit tests

Tests have been written using the built-in Python module `unittest`. Examples:

* Run all price tests: `python -m unittest tests.test_prices`

* Run sub-set of price tests: `python -m unittest tests.test_prices.TestPriceRepair`

* Run a specific test: `python -m unittest tests.test_prices.TestPriceRepair.test_ticker_missing`

* Run all tests: `python -m unittest discover -s tests`

> [!NOTE]
> The tests are currently failing already
>
> Standard result:
>
> **Failures:** 11
>
> **Errors:** 93
>
> **Skipped:** 1

## Git stuff
### commits

To keep the Git commit history and [network graph](https://github.com/ranaroussi/yfinance/network) compact please follow these two rules:

* For long commit messages use this: `git commit -m "short sentence summary" -m "full commit message"`

* `squash` tiny/negligible commits back with meaningful commits, or to combine successive related commits. [Guide](https://docs.gitlab.com/ee/topics/git/git_rebase.html#interactive-rebase) but basically it's:


```bash
git rebase -i HEAD~2
git push --force-with-lease origin <branch-name>
```

### rebase
 
You might be asked to move your branch from `main` to `dev`. Make sure you have pulled **all** relevant branches then run:

```bash
git checkout <your branch>
git rebase --onto dev main <branch-name>
git push --force-with-lease origin <branch-name>
```