.. image:: ./images/zipline-live2.small.png
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

.. code-block:: batch

    pip install virtualenv
    virtualenv venv
    activate:
        Mac OS / Linux
            source venv/bin/activate
        Windows
            venv\Scripts\activate

installing the package:

.. code-block:: batch

    pip install zipline-live2

.. image:: ./images/youtube/installing.png
    :target: https://www.youtube.com/watch?v=Zh9Vs_yanXY
    :width: 212px
    :align: center
    :alt: zipline-live


for advanced capabilities recommended way to use this package with docker using this command:

.. code-block:: docker

    docker build -t quantopian/zipline .


(if your algo requires more packages, you could extend the dockerfile-dev and install using: docker build -f dockerfile-dev-t quantopian/zipline .)


you could run everything on a local machine with whatever OS you want. but you may experience package installation issues.

this is the best way to ensure that you are using the same version everyone else use.


Ingest data
===========
the quantopian-quandl is a free daily bundle.
every day you should execute this when live trading in order to get the most updated data

.. code-block:: batch

 zipline ingest -b quantopian-quandl

Running Backtests
=================
you can run a backtest with this command:

.. code-block:: batch

    zipline run -f zipline_repo/zipline/examples/dual_moving_average.py --start 2015-1-1 --end 2018-1-1 --bundle quantopian-quandl -o out.pickle --capital-base 10000


.. image:: ./images/youtube/command_line_backtest.png
    :target: https://youtu.be/jeuiCpx9k7Q
    :width: 212px
    :align: center
    :alt: zipline-live



Run the cli tool
================

.. code-block:: batch

    zipline run -f ~/zipline-algos/demo.py --state-file ~/zipline-algos/demo.state --realtime-bar-target ~/zipline-algos/realtime-bars/ --broker ib --broker-uri localhost:7496:1232 --bundle quantopian-quandl --data-frequency minute
