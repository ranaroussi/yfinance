********************************
Contributing to yfinance
********************************

`yfinance` relies on the community to investigate bugs and contribute code. Here&apos;s how you can help:

Contributing
------------

1. Fork the repository on GitHub. If already forked, remember to `Sync fork`
2. Clone your forked repository:

   .. code-block:: bash

      git clone https://github.com/{user}/{repo}.git

3. Create a new branch for your feature or bug fix:

   .. code-block:: bash

      git checkout -b {branch}

4. Make your changes, commit them, and push your branch to GitHub. To keep the commit history and `network graph <https://github.com/ranaroussi/yfinance/network>`_ compact:

   Use short summaries for commits

   .. code-block:: bash

      git commit -m "short summary" -m "full commit message"

   **Squash** tiny or negligible commits with meaningful ones.

   .. code-block:: bash

      git rebase -i HEAD~2
      git push --force-with-lease origin {branch}

5. Open a pull request on the `yfinance` `Github <https://github.com/ranaroussi/yfinance/pulls>`_ page.

Git stuff
---------

To keep the Git commit history and [network graph](https://github.com/ranaroussi/yfinance/network) compact please follow these two rules:

- For long commit messages use this: `git commit -m "short sentence summary" -m "full commit message"`

- `squash` tiny/negligible commits back with meaningful commits, or to combine successive related commits. [Guide](https://docs.gitlab.com/ee/topics/git/git_rebase.html#interactive-rebase) but basically it's:

.. code-block:: bash
   git rebase -i HEAD~2
   git push --force-with-lease origin {branch}


### rebase
 
You might be asked to move your branch from `main` to `dev`. Make sure you have pulled **all** relevant branches then run:

.. code-block:: bash
   git checkout {branch}
   git rebase --onto dev main {brach}
   git push --force-with-lease origin {branch}
