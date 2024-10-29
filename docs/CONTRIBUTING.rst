Contributions
=============

Hi there! We're thrilled that you'd like to contribute to this project. Your help is essential for keeping it great.

Please note that this project is released with a `Contributor Code of Conduct <https://github.com/astronomer/astro-provider-ray/tree/main/docs/CODE_OF_CONDUCT.rst>`_.
By participating in this project you agree to abide by its terms.

Overview
--------

The astro-provider-ray source code is available on this GitHub `page <https://github.com/astronomer/astro-provider-ray>`_

To contribute to the **Ray provider** project:

#. Please create a `GitHub Issue <https://github.com/astronomer/astro-provider-ray/issues>`_ describing your contribution.
#. Open a feature branch off of the ``main`` branch and create a Pull Request into the ``main`` branch from your feature branch.
#. Link your issue to the pull request.
#. Once development is complete on your feature branch, request a review and it will be merged once approved.

Test Changes Locally
--------------------

Pre-requisites
~~~~~~~~~~~~~~

* pytest

.. code-block:: bash

    pip install pytest


Set up RayCluster and  Apache Airflow
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For instructions on setting up RayCluster and Apache Airflow, please see the `Local Development Setup <https://github.com/astronomer/astro-provider-ray/blob/main/docs/getting_started/local_development_setup.rst>`_.

Run tests
~~~~~~~~~

All tests are inside ``./tests`` directory.

- Just run ``pytest filepath+filename`` to run the tests.

Static Code Checks
------------------

We check our code quality via static code checks. The static code checks in astro-provider-ray are used to verify
that the code meets certain quality standards. All the static code checks can be run through pre-commit hooks.

Your code must pass all the static code checks in the CI in order to be eligible for Code Review.
The easiest way to make sure your code is good before pushing is to use pre-commit checks locally.

Pre-Commit
~~~~~~~~~~

We use pre-commit to run a number of checks on the code before committing. To install pre-commit, run the following from
your cloned ``astro-provider-ray`` directory:

.. code-block:: bash

    pip install pre-commit
    pre-commit install

To run the checks manually, run:

.. code-block:: bash

    pre-commit run --all-files

For details on the pre-commit configuration, refer to the `pre-commit config file <https://github.com/astronomer/astro-provider-ray/blob/main/.pre-commit-config.yaml>`_.

For more details on each hook and additional configuration options, refer to the official pre-commit documentation: https://pre-commit.com/hooks.html

Writing Docs
------------

You can run the docs locally by running the following:

.. code-block:: bash

    hatch run docs:serve


This will run the docs server in a virtual environment with the right dependencies. Note that it may take longer on the first run as it sets up the virtual environment, but will be quick on subsequent runs.

Building
--------

We use `hatch <https://hatch.pypa.io/latest/>`_ to build the project. To build the project, run:

.. code-block:: bash

    hatch build

Releasing
---------

We use GitHub actions to create and deploy new releases. To create a new release, first create a new version using:

.. code-block:: bash

    hatch version minor

hatch will automatically update the version for you. Then, create a new release on GitHub with the new version. The release will be automatically deployed to PyPI.

.. note::
    You can update the version in a few different ways. Check out the `hatch docs <https://hatch.pypa.io/latest/version/#updating>`_ to learn more.

To validate a release locally, it is possible to build it using:

.. code-block:: bash

    hatch build

To publish a release to PyPI, use:

.. code-block:: bash

    hatch publish
