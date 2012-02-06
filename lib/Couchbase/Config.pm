package Couchbase::Config::Request;
use strict;
use warnings;
use base qw(HTTP::Request Exporter);
our $VERSION = '0.01_1';

use Class::XSAccessor {
    accessors => [qw(cbc_ctx cbc_data cbc_reqtype cbc_streaming)]
};

package Couchbase::Config;
use strict;
use warnings;
use Carp qw(cluck);

use URI;
use MIME::Base64 qw(encode_base64);
use JSON::XS;
use Data::Dumper;
use base qw(Exporter);

our @EXPORT;
our @COMMANDS = qw(
    list_pools
    pool_info
    list_buckets
    create_bucket
    delete_bucket
    flush_bucket
    failover_node
    eject_node
    add_node
    rebalance_pool
    join_cluster
);

use Couchbase::Config::Resource;
use Couchbase::Config::Constants;
use base qw(Couchbase::Config::Handlers);
use base qw(Couchbase::Config::Commands);

push @EXPORT, @Couchbase::Config::Constants::EXPORT;

use Log::Fu { level => "debug" };

use Class::XSAccessor {
    constructor => '_real_new',
    accessors => [qw(
        uri_base
        request_base
        username
        password
        host
        port
        parsers
        is_streaming
        pools
    )]
};


sub new {
    my ($cls,$server,%options) = @_;
    if(!$server) {
        die("Must have cluster server");
    }
    
    my ($host,$port) = split(/:/, $server);
    $port ||= 8091;
    
    
    my $self = $cls->_real_new(host => $host, port => $port, %options);
    
    my $base = Couchbase::Config::Request->new();
    
    if($self->username && $self->password) {
        my $authstring = join(":", $self->username, $self->password);
        $authstring = encode_base64($authstring);
        
        $base->header('Authorization', "Basic $authstring");
    }
    $base->header('Accept', 'application/json');
    $base->uri("http://$host:$port/");
    $self->uri_base("http://$host:$port");
    $base->header('Host', $host);
    $base->protocol('HTTP/1.1');
    
    $self->request_base($base);
    $self->pools({});
    
    $self->parsers({});
    return $self;
}

sub _new_get_request {
    my ($self,$path,$reqtype) = @_;
    my $req = $self->request_base->clone();
    if(!defined $path) {
        cluck("Was given a null path $path");
        return;
    }
    if($path !~ m,^/,) {
        $path = "/$path";
    }
    log_debug("Generating new request for $path");
    #$req->uri->path($path);
    $req->uri($self->uri_base . $path);
    $req->method("GET");
    $req->cbc_reqtype($reqtype);
    return $req;
}

sub _new_post_request {
    my ($self,$path,$reqtype,$content) = @_;
    my $request = $self->_new_get_request($path, $reqtype);
    $request->method("POST");
    if(ref $content) {
        my $uri = URI->new("http:");
        $uri->query_form(ref $content eq 'HASH' ? %$content : @$content);
        $content = $uri->query;
    }
    $content ||= "";
    $request->header('Content-Type', 'application/x-www-form-urlencoded');
    $request->content($content);
    $request->header('Content-Length', length($content));
    return $request;
}

sub update_context {
    my ($self,$request,$response) = @_;
    my $reqtype = $request->cbc_reqtype;
    my $handler = $self->get_handler($reqtype);
    
    if(!$handler) {
        die(sprintf("No handler for command %d (%s)",
                    $reqtype, couchconf_reqtype_str($reqtype)));
    }
    my $ret;
    my $hash;
    eval {
        $hash = decode_json($response->decoded_content);
    };
    eval {
        $ret = $handler->($self, $request, $response, $hash);
    }; if ($@) {
        log_err("Handler died: $@");
        $ret = undef;
    }
    
    unless(!defined $ret || $ret == COUCHCONF_ERROR) {
        return $ret;
    }
    
    $ret ||= COUCHCONF_ERROR;
    my $msg = $response->content;
    if($msg =~ /unexpected server error/) {
        return COUCHCONF_ESERVER;
    } elsif ($response->code =~ /40[13]/) {
        return COUCHCONF_EAUTH;
    }
    
    log_err("Found unhandled error response");
    log_err(couchconf_reqtype_str($reqtype));
    print $response->as_string();
    return $ret;
}

1;

__END__

=head1 NAME

Couchbase::Config - Configuration API for Couchbase

=head2 DESCRIPTION

This module contains commands and handlers for managing and monitoring a
C<couchbase> cluster.

=head2 Commands, Return Types, and Objects

This module forms the foundation of the management API. It provides
B<commands> to query and/or manipulate a couchbase cluster, and B<resources>
which represent information and state about couchbase cluster  resources.

At the most basic level, this module returns a C<Couchbase::Config::Request> for
commands requested. This object is a subclass of L<HTTP::Request> with a few
private fields added for internal book-keeping.

The returned request object should then be submitted to the destination server
(which is embedded into the request object) via your favorite HTTP client.

When a response has been received from the server, L</update_context> should be
called with the issued request and the received response.

L</update_context> will return an object indicating the proper response for the
command.

Commands vary and can have various return types (specified for the documentation
for each command).

You are supposed to apply this logic on the return value of L</update_context>
depending on the command issued.

Some commands will return resource objects (see L<Couchbase::Config::Resource>), and
some commands will expect resource objects as their parameters.

Some commands will need another request re-issued to the server, and thus the
return value of L</update_context> may also be another request object itself.


=head2 METHODS

=head3 new($hostname, %options)

This method will construct a new L<Couchbase::Config::Object> which represents a
single connection to a node.
    
    my $conf = Couchbase::Config->new(
        "localhost:8091",
        username => "user",
        password => "secret"
    );

The first argument is a mandatory hostname parameter in the form of C<"host:port">.
If port is ommited, it will default to 8091.

The following hash options are recognized:

=over

=item C<username>, C<password>

Specify the username and password for the server. This will be converted to proper
HTTP authorization for each request.

=item C<is_streaming>

Some commands can be issued either via a traditional request-response mechanism,
where the response is a single message, or it can employ a streaming API which
will send real-time updates for results, as they happen.

This option controls whether the requests generated should be for traditional or
streaming responses.

=back

=head3 update_context($cbconf_request, $http_response)

Tell C<Couchbase::Config> to update its internal state based on the provided
request and response parameters.

Common return objects are

=over

=item Reference Type

This will be either a single resource object or a collection thereof. This always
indicates a successful operation.

=item Request Object

This will be a C<Couchbase::Config::Request> object, and indicates that the command
issued requires another HTTP transaction for a result.

=item Scalar

This is an error constant. The generic forms are the following:

=over 2
    
=item C<COUCHCONF_OK>

=item C<COUCHCONF_ERROR>

=item C<COUCHCONF_EAUTH>

=item C<COUCHCONF_ESERVER>

This represents an obscure '500' server error.

=back

=back

=head1 SEE ALSO

=over

=item L<Couchbase::Config::Commands>

Listings of commands

=item L<Couchbase::Config::Daemon>

Asynchronous wrapper for commands, based on L<POE::Component::Client::HTTP>

=item L<Couchbase::Config::UA>

Synchronous wrapper for commands, based on L<LWP::UserAgent>

=item L<Couchbase::Config::Resource>

Resource objects

=back

=head1 AUTHOR & COPYRIGHT

Copyright (C) 2012 M. Nunberg

You may use and distribute this software under the same terms and license as
Perl itself.
