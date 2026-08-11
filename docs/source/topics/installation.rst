============
Installation
============

Supported Python versions
-------------------------

The framework is compatible with Python of versions >= 3.10

Pip install
-----------

You can use pip to install library from PyPi:

::

   pip install bestagon

The core Bestagon package contains no additional dependencies at all.
It is a perfect option for those who wants to implement it's own adapters from scratch,
but the framework also provides ready to use adapters, for example implementation of event store
using Kurrent database,
such adapters require additional modules that can be installed using optional dependencies.

If you want to use `Kurrent <https://www.kurrent.io/>`_ database in your projects then you can install it with "kurrentdb" option:

::

   pip install "bestagon[kurrentdb]"

If You want to use `Neo4j <https://neo4j.com/>`_ in your project, you can install it using "neo4j" option:

::

   pip install "bestagon[neo4j]"

There are adapters implementations with `aiosqlite <https://pypi.org/project/aiosqlite/>`_, you can install them using "aiosqlite" option:

::

   pip install "bestagon[aiosqlite]"

The options can be combined if you need more than one:

::

   pip install "bestagon[kurrentdb,neo4j]"

To install all dependencies use "all" option:

::

   pip install "bestagon[all]"