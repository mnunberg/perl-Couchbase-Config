package Couchbase::Config::UA;
use strict;
use warnings;
use LWP::UserAgent;
use Couchbase::Config;
use base qw(Couchbase::Config);
use JSON;
use Log::Fu;

use Class::XSAccessor {
    accessors => [qw(lwp bucket_config)]
};

sub new {
    my ($cls,@options) = @_;
    my $self = __PACKAGE__->SUPER::new(@options);
    bless $self, $cls;
    $self->lwp(LWP::UserAgent->new());
    return $self;
}
foreach my $methname (@Couchbase::Config::COMMANDS)
{
    no strict 'refs';
    *{$methname} = sub {
        my $self = shift;
        my $request = $self->${\"SUPER::$methname"}(@_);
        my $response = $self->lwp->request($request);
        $self->update_context($request, $response);
    };
}

1;

__END__

=head1 NAME

Couchbase::Config::UA - synchronous client for L<Couchbase::Config>

=head1 SYNOPSIS

    my $ua = Couchbase::Config::UA->new(
        hostname => 'localhost:8091',
        username => 'Administrator',
        password => 'secret'
    );
    
    my $default_pool = $ua->list_pools();
    my $info = $ua->pool_info($default_pool);
    
    #...
    
    
=head2 DESCRIPTION

This module wraps around the methods and functions of L<Couchbase::Config> in a
synchronous fashion, using L<LWP::UserAgent>.

See L<Couchbase::Config> for a list of commands supported by this module.

