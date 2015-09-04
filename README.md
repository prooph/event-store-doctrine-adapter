Doctrine Adapter for ProophEventStore
=====================================

[![Build Status](https://travis-ci.org/prooph/event-store-doctrine-adapter.svg?branch=master)](https://travis-ci.org/prooph/event-store-doctrine-adapter)
[![Coverage Status](https://coveralls.io/repos/prooph/event-store-doctrine-adapter/badge.png)](https://coveralls.io/r/prooph/event-store-doctrine-adapter)
[![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/prooph/improoph)

Use [Prooph Event Store](https://github.com/prooph/event-store) with [Doctrine DBAL](https://github.com/doctrine/dbal).

Database Set Up
---------------

The database structure depends on the [stream strategies](https://github.com/prooph/event-store/blob/master/docs/repositories.md#stream-strategies) you want to use for your aggregate roots.
You can find example SQLs for MySql in the [scripts folder](scripts)
and an [EventStoreSchema tool](src/Schema/EventStoreSchema.php) which you can use an a doctrine migrations scirpt.

Requirements
------------
- PHP >= 5.5
- [Doctrine DBAL](https://github.com/doctrine/dbal) ^2.4
- [Prooph Event Store](https://github.com/prooph/event-store) ^5.0

License
-------

Released under the [New BSD License](https://github.com/prooph/event-store-doctrine-adapter/blob/master/LICENSE).
