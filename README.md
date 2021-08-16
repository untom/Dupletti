Dupletti
========

Dupletti looks for duplicate files on your machine by hashing them and comparing those hashes.

Simple Usage
------------

Use cargo to compile the program. Pass the path you'd like to search through using the ```-p```
flag, or use ```--help`` to see the full options. By default, Dupletti will search the whole
directories for duplicates, and then open up a web-interface on Port 5757, so you can look 
through the results, and remove or rename any duplicate files.


License
-------
Distributed Parameter Search is copyrighted (c) 2021 by Thomas Unterthiner and licensed under the
`General Public License (GPL) Version 2 or higher <http://www.gnu.org/licenses/gpl-2.0.html>`_.
See ``LICENSE.md`` for the full details.
