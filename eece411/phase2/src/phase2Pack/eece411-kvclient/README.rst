EECE 411 - Key Value Client
===========================

.. image:: https://travis-ci.org/squirly/eece411-kvclient.png
    :target: https://travis-ci.org/squirly/eece411-kvclient

Installation
------------

To install, simply:

.. code-block:: bash

    pip install git+git://github.com/squirly/eece411-kvclient.git

Usage
-----

.. code-block:: python

    from kvclient import KeyValueClient, InvalidKeyError

    SERVER = 'squirly.ca:9090'
    KEY = 'my_key'
    VALUE = 'the value to be saved'

    client = KeyValueClient(SERVER)
    client.put(KEY, VALUE)
    print(client.get(KEY))
    client.delete(KEY)
    try:
        client.get(KEY)
    except InvalidKeyError, error:
        print(str(error))

Development
-----------

Getting the code:

.. code-block:: bash

    git clone git://github.com/squirly/eece411-kvclient.git
    cd eece411-kvclient

To run the tests:

.. code-block:: bash

    python setup.py test

To contribute changes, make a pull request on Github.
