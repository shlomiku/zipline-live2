.. image:: ./zipline-live2.small.png
    :target: https://github.com/shlomikushchi/zipline-live2
    :width: 212px
    :align: center
    :alt: zipline-live

zipline-live2
=============

Welcome to zipline-live2, the on-premise trading platform built on top of Quantopian's
`zipline <https://github.com/quantopian/zipline>`_.

zipline-live2 is based on the `zipline-live <http://www.zipline-live.io>`_ project.

zipline-live2 is designed to be an extensible, drop-in replacement for zipline with
multiple brokerage support to enable on premise trading of zipline algorithms.

Installation
============
use a fresh virtual env

.. code-block:: RST

    pip install virtualenv
    virtualenv venv
    activate:
        Mac OS / Linux
            source venv/bin/activate
        Windows
            venv\Scripts\activate

installing the package:
.. code-block:: python

    pip install zipline-live2


for advanced capabilities recommended way to use this package with docker using this command:

.. code-block:: docker

    docker build -t quantopian/zipline .


(if your algo requires more packages, you could extend the dockerfile-dev and install using: docker build -f dockerfile-dev-t quantopian/zipline .)


you could run everything on a local machine with whatever OS you want. but you may experience package installation issues.

this is the best way to ensure that you are using the same version everyone else use.

