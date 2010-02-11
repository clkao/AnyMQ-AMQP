use Test::More;
use Time::HiRes 'time';
use strict;
use AnyEvent;
use AnyMQ;

my $bus = AnyMQ->new_with_traits(traits => ['AMQP'],
                                 host   => 'localhost',
                                 port   => 5672,
                                 user   => 'guest',
                                 pass   => 'guest',
                                 vhost  => '/',
                                 exchange => '',
                             );

is($bus->host, 'localhost');

#$cv->recv;
my $test = $bus->topic('test_q');

my $q = AnyEvent->condvar;

my $client = $bus->new_listener; $client->subscribe($test);
my $cnt = 0;
$client->poll( sub {
                   my $timestamp = shift;
                   diag "latency: ".(time() - $timestamp);
                   ++$cnt;
                   $q->send([1]) if $cnt == 10;
               });

# simulate messages coming to the exchange
$bus->_rf_channel->publish( routing_key => 'test_q',
                            body => time(),
                        ) for 1..10;

my $w; $w = AnyEvent->timer( after => 5,
                             cb => sub {
                                 $q->send([0, 'test timeout']);
                             } );
ok(@{ $q->recv });

done_testing;
