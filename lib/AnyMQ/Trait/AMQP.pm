package AnyMQ::Trait::AMQP;
use Moose::Role;

use AnyEvent;
use AnyEvent::RabbitMQ;

has host => (is => "ro", isa => "Str");
has port => (is => "ro", isa => "Int");
has user => (is => "ro", isa => "Str");
has pass => (is => "ro", isa => "Str");
has vhost => (is => "ro", isa => "Str");
has exchange => (is => "ro", isa => "Str");

has bind_mode => (is => "ro", isa => "Str", default => sub { 'exchange' });

has _rf => (is => "rw");
has _rf_channel => (is => "rw");
has _rf_queue => (is => "rw");

sub BUILD {
    my $self = shift;

    my $rf = AnyEvent::RabbitMQ->new({timeout => 1, verbose => 0,});
    $rf->load_xml_spec('fixed_amqp0-8.xml');

    my $cv = AE::cv;
    $rf->connect((map { $_ => $self->$_ }
                       qw(host port user pass vhost)),
                 on_success => sub {
                     $rf->open_channel(
                         on_success => sub {
                             my $channel = shift;
                             $self->_rf_channel($channel);
                             $channel->qos();
                             $channel->declare_queue(
                                 exclusive => 1,
                                 on_success => sub {
                                     my $method = shift;
                                     my $queue = $method->method_frame->queue;
                                     $self->_rf_queue($queue);
                                     $channel->consume(queue => $queue,
                                                       no_ack => 1,
                                                       on_success => sub {
                                                           $cv->send('init');
                                                       },
                                                       on_consume => sub {
                                                           my $frame = shift;
                                                           my $payload = $frame->{body}->payload;
                                                           my $reply_to = $frame->{header}->reply_to;
                                                           next if $reply_to && $reply_to eq $self->_queue;
                                                           my $topic = $frame->{deliver}->method_frame->routing_key;
                                                           $self->topics->{$topic}->publish($payload);

                                                       },
                                                       on_failure => $cv,
                                                   );
                                 },
                                 on_failure => $cv,
                             ),
                         },
                         on_failure => $cv,
                     );
                     },
                 on_failure => $cv,
             );
    $cv->recv;

}

around 'new_topic' => sub {
    my ($next, $self, @args) = @_;
    my $topic = $self->$next(@args);
    warn "topic: ".$topic->name;
    $self->_rf_channel->bind_queue(
        queue       => $self->_rf_queue,
        routing_key => $topic->name,
    );
    return $topic;
};

1;
