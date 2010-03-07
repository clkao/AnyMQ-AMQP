use inc::Module::Install;
name 'AnyMQ-AMQP';
all_from 'lib/AnyMQ/AMQP.pm';
readme_from 'lib/AnyMQ/AMQP.pm';
build_requires 'Test::More';

requires 'AnyEvent::RabbitMQ';

auto_include_deps;
author_tests('xt');
auto_set_repository;
WriteAll;
