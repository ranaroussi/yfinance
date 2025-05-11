********
Branches
********

To support rapid development without breaking stable versions, this project uses a two-layer branch model:

.. image:: assets/branches.png
   :alt: Branching Model

`Inspiration <https://miro.medium.com/max/700/1*2YagIpX6LuauC3ASpwHekg.png>`_

- **dev**: New features and some bug fixes are merged here. This branch allows collective testing, conflict resolution, and further stabilization before merging into the stable branch.
- **main**: Stable branch where PIP releases are created.

By default, branches target **main**, but most contributions should target **dev**. 

**Exceptions**:
Direct merges to **main** are allowed if:

- `yfinance` is massively broken
- Part of `yfinance` is broken, and the fix is simple and isolated
- Not updating the code (e.g. docs)

Rebasing
--------

If asked to move your branch from **main** to **dev**:

1. Ensure all relevant branches are pulled.
2. Run:

   .. code-block:: bash

      git checkout {branch}
      git rebase --onto dev main {brach}
      git push --force-with-lease origin {branch}

Running a branch
----------------

Please see `this page </development/running>`_.