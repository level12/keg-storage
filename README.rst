KegBase Library
###############


This repository is the starting point for a Keg library. It makes some
assumptions, for example that you want to support Python 2.7, 3.4, and 3.5, you
want your line length to be 100, and a whole host of other things.

The point being, you can start a new library much quicker now.


Usage
=====

Clone the repo down.

.. code::

  $ git clone git@github.com:level12/keg-baselib


Then change some important things...

setup.py:
  - name
  - install_requires
  - url
  - description


tox.ini:
  - change the folder that ``--cov`` points at

lib:
  - change the name of your library from lib to something a little more helpful,
    though you could keep that name if you want to, though you might need to do
    some extra stuff in setup.py


Why not something else
======================

Git cloning is about as easy as it gets. Using cookie cutter tools has proven to
be rather challenging. This gets you 95% of the way there, with very little
effort. If you keep the git history as well, you can even track updates to this
repo and merge them into your project over time.
