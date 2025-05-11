****
Code
****

1. Fork the repository on GitHub. If already forked, remember to `Sync fork`
2. Clone your forked repository:

   .. code-block:: bash

      git clone https://github.com/{user}/{repo}.git

3. Create a new branch for your feature or bug fix, from appropriate base branch:

   .. code-block:: bash

      git checkout {base e.g. dev}
      git pull
      git checkout -b {branch}

4. Make your changes, commit them, and push your branch to GitHub. To keep the commit history and `network graph <https://github.com/ranaroussi/yfinance/network>`_ compact:

   - for long commit messages use this format. Your long message can be multiple lines (tip: copy-paste):

     .. code-block:: bash

        git commit -m "short sentence summary" -m "full commit message"

   - **git squash** tiny or negligible commits with meaningful ones, or to combine successive related commits. `git squash guide <https://docs.gitlab.com/ee/topics/git/git_rebase.html#interactive-rebase>`_

     .. code-block:: bash

        git rebase -i HEAD~2
        git push --force-with-lease origin {branch}

5. If your branch is old and missing important updates in base branch, then instead of merging in, do a git rebase. This keeps your branch history clean. E.g.

   .. code-block:: bash
     
      git checkout {base e.g. dev}
      git pull
      git checkout {branch}
      git rebase {base}
      git push --force-with-lease origin {branch}

6. `Open a pull request on Github <https://github.com/ranaroussi/yfinance/pulls>`_.

More Git stuff
---------

- ``git rebase``. You might be asked to move your branch from ``main`` to ``dev``. Important to update **all** relevant branches.

  .. code-block:: bash

     # update all branches:
     git checkout main
     git pull
     git checkout dev
     git pull
     # rebase:
     git checkout {branch}
     git pull
     git rebase --onto dev main {branch}
     git push --force-with-lease origin {branch}

