Doctrine Adapter for ProophEventStore
=====================================

[![Build Status](https://travis-ci.org/prooph/event-store-doctrine-adapter.svg?branch=master)](https://travis-ci.org/prooph/event-store-doctrine-adapter)
[![Coverage Status](https://coveralls.io/repos/prooph/event-store-doctrine-adapter/badge.png)](https://coveralls.io/r/prooph/event-store-doctrine-adapter)

Use [ProophEventStore](https://github.com/prooph/event-store) with [Doctrine DBAL](https://github.com/doctrine/dbal).

Database Set Up
---------------

The database structure depends on the [stream strategies](https://github.com/prooph/event-store#streamstrategies) you want to use for your aggregate roots.
You can find example SQLs for MySql in the [scripts folder](https://github.com/prooph/event-store-doctrine-adapter/blob/master/scripts/)
and an [example script](https://github.com/prooph/event-store-doctrine-adapter/blob/master/examples/create-schema.php) of how you can use the adapter to generate stream tables.

Requirements
------------
- PHP >= 5.5
- [Doctrine DBAL](https://github.com/doctrine/dbal) ^2.4
- [Prooph Event Store](https://github.com/prooph/event-store) ^5.0

License
-------

Released under the [New BSD License](https://github.com/prooph/event-store-doctrine-adapter/blob/master/LICENSE).
