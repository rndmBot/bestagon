============
Installation
============

Supported Python versions
-------------------------

The framework is compatible with Python versions >= 3.10

Pip install
-----------

You can use pip to install the library from PyPI:

::

   pip install bestagon

The core Bestagon package contains only required dependencies.
It is a perfect choice for those who want to implement its own adapters from scratch,
but Bestagon also provides ready-to-use adapters, for example implementation of the event store
that uses KurrentDB or AIOSQLite, such adapters require additional modules that can be
installed using optional dependencies.

If you want to use `Kurrent <https://www.kurrent.io/>`_ event store in your projects, then you can install it with the ``kurrentdb`` option:

::

   pip install "bestagon[kurrentdb]"

If you want to use `Neo4j <https://neo4j.com/>`_ in your project, you can install it using the ``neo4j`` option:

::

   pip install "bestagon[neo4j]"

There are adapter implementations with `aiosqlite <https://pypi.org/project/aiosqlite/>`_, you can install them using the ``aiosqlite`` option:

::

   pip install "bestagon[aiosqlite]"

The options can be combined if you need more than one:

::

   pip install "bestagon[kurrentdb,neo4j]"

To install all dependencies use the ``all`` option:

::

   pip install "bestagon[all]"