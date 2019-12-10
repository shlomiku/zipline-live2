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
recommended way to use this package is with docker.

you should build a docker image using this command:

`docker build -t quantopian/zipline .`

(if your algo requires more packages, you could extend the dockerfile-dev and install using: docker build -f dockerfile-dev-t quantopian/zipline .)


you could run everything on a local machine with whatever OS you want. but you may experience package installation issues.

this is the best way to ensure that you are using the same version everyone else use.

