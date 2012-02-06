package Couchbase::Config::Daemon;
use strict;
use warnings;
use POE;
use POE::Kernel;
use POE::Component::Client::HTTP;
use POE::Component::Client::Keepalive;
use base qw(POE::Sugar::Attributes Couchbase::Config);

use Couchbase::Config;
use UNIVERSAL;
use Log::Fu { level =>"debug"};
use Data::Dumper::Concise;
use JSON::XS;

use Class::XSAccessor {
    accessors => [qw(
        ua_alias
        ua_streaming_alias
        alias
        peers
        pending_list_buckets
        auto_rebalance
    )]
};

my $poe_kernel = 'POE::Kernel';

my @ServerList;
my $DEFAULT_UA_ALIAS = 'couchbase-config-daemon-ua';
my $DEFAULT_UA_STREAMING_ALIAS = 'couchbase-config-daemon-ua-streaming';
my $DEFAULT_ALIAS = 'couchbase-config-daemon';
my $HKEY_JSONPARSE = '_cbc_parser';
my $HKEY_OURDATA = '_cbc_daemon_data';

my $CM;

sub new {
    my ($cls,$hostname,%options) = @_;
    my @own_keys = qw(ua_alias ua_streaming_alias alias);
    my %own_opts;
    @own_opts{@own_keys} = delete @options{@own_keys};
    my $self = __PACKAGE__->SUPER::new($hostname, %options);
    
    bless $self, $cls;
    @{$self}{keys %own_opts} = values %own_opts;
    if(!$self->alias) {
        die("Must have alias");
    }
    return $self;
}

sub spawn {
    my ($cls,$hostname,%options) = @_;
    my $o = $cls->new($hostname, %options);
    POE::Session->create(
        heap => $o,
        inline_states => POE::Sugar::Attributes->inline_states(
            __PACKAGE__, $o->alias));
}


sub _create_cm {
    return $CM if $CM;
    $CM = POE::Component::Client::Keepalive->new(
        max_per_host => 30,
        max_open => 60,
    );
    $CM;
    
}
sub daemon_start :Start {
    my $obj = $_[HEAP];
    
    if(!$obj->ua_alias) {
        $obj->ua_alias($DEFAULT_UA_ALIAS);
    }
    
    my $ua_session = $poe_kernel->alias_resolve($obj->ua_alias);
    if(!$ua_session) {
        POE::Component::Client::HTTP->spawn(
            Timeout => 30,
            Alias => $obj->ua_alias,
            ConnectionManager => _create_cm()
        );
    }
    
    my $streaming_session = $poe_kernel->alias_resolve(
                                    $DEFAULT_UA_STREAMING_ALIAS);
    
    $obj->ua_streaming_alias($DEFAULT_UA_STREAMING_ALIAS);
    if(!$streaming_session) {
        POE::Component::Client::HTTP->spawn(
            Timeout => 180,
            Alias => $DEFAULT_UA_STREAMING_ALIAS,
            ConnectionManager => _create_cm(),
            Streaming => 4096
        );
    }
    $obj->is_streaming(1);
    
    #Start the initial dance, and get a 'list_pools' request:
    $obj->list_pools();
}

sub got_pool_info {
    my ($self,$cbcrv) = @_;
    my $found_changes;
    my $changes = $cbcrv->changelog;
    if(@{$changes->nodes_added}) {
        $found_changes = 1;
        log_info("Nodes added:", join(",",
            map($_->id, @{$changes->nodes_added})) );
    }
    if(@{$changes->nodes_removed}) {
        $found_changes = 1;
        log_warn("Nodes removed:", join(",",
            map($_->id, @{$changes->nodes_removed})) );
    }                
    foreach my $node (@{$cbcrv->nodes}) {
        while (my ($k,$v) = each %{$node->changelog}) {
            $found_changes = 1;
            log_infof("%s: [%s] %s => %s", $node->id,
                      $k, $v || "<undef>", $node->{$k});
        }
    }
    
    if($changes->balance_state) {
        $found_changes = 1;
        log_infof("Balance state: %s => %s",
                 $changes->balance_state, $cbcrv->balance_state);
    }
    
    if(!$found_changes) {
        #use Hash::Diff qw(diff);
        log_warn("Couldn't find any changes in status!");
        #log_info(Dumper(diff($cbcrv->_data, $cbcrv->_old_data)));
    }
    
    #(RE)issue a request to find bucket information
    if($cbcrv->needs_rebalance) {
        if($self->auto_rebalance) {
            log_debug("Issuing rebalance request");
            $self->rebalance_pool($cbcrv);
        } else {
            log_warn("We need a rebalance, but auto_rebalance not enabled");
        }
    }
    $self->list_buckets($cbcrv);
}

sub got_fatal_error {
    my ($self,$request,$cbcrv) = @_;
    if($cbcrv == COUCHCONF_ENOPOOLS) {
        my $peer_list;
        if(! ($peer_list = $request->{$HKEY_OURDATA})) {
            if($self->peers) {
                $peer_list = [ @{$self->peers} ];
                $request->{$HKEY_OURDATA} = $peer_list;
            } else {
                die("No neighboring peers specified and we are not joined");
            }
        }
        my $next_peer = shift @$peer_list;
        if(!$next_peer) {
            die("No more peers left to join, and we are not yet joined");
        }
        log_warn("Sending join request to $next_peer");
        $self->join_cluster($next_peer, $self->username, $self->password);
    }
}

sub response_handler :Event {
    my ($request_packet,$response_packet) = @_[ARG0, ARG1];
    my ($request,$response) = ($request_packet->[0], $response_packet->[0]);
    
    my $reqtype = $request->cbc_reqtype;
    log_infof("Got response for %s", couchconf_reqtype_str($reqtype));
    my $self = $_[HEAP];
    
    my $cbcrv = $self->update_context($request, $response);
    
    if(!ref $cbcrv && $cbcrv != COUCHCONF_OK) {
        if($cbcrv == COUCHCONF_EXECUTING) {
            log_debug("Request is still processing (multi-part?)");
            return;
        }
        log_err("Got error for request.: $cbcrv");
        $self->got_fatal_error($request, $cbcrv);
        return;
    }
    
    if(UNIVERSAL::isa($cbcrv, 'Couchbase::Config::Request')) {
        log_info("Got new request in multi-request operation");
        $self->submit_request($cbcrv);
        return;
    }
    
    #print Dumper($status);
    if($reqtype == COUCHCONF_REQ_LIST_ALL_POOLS) {
        
        #get information about our default pool
        $self->pool_info($cbcrv);
        return;
    }
    
    
    if ($reqtype == COUCHCONF_REQ_POOL_INFO
             || $reqtype == COUCHCONF_REQ_CMD_JOIN_CLUSTER) {
        $self->got_pool_info($cbcrv);
        return;
        
    }
    if ($reqtype == COUCHCONF_REQ_LIST_BUCKETS) {
        my $buckets = $cbcrv;
        my $pool = $request->cbc_data;
        my $changes = $pool->changelog;
        my @l = ( ["buckets_added", [] ], ["buckets_removed", [] ] );
        foreach my $bspec (@l) {
            my ($meth,$arry) = @$bspec;
            my @items = @{$changes->$meth || [] };
            if(@items) {
                log_infof("%s: %s", $meth, join(",", map($_->name, @items)));
            }
        }
        return;
    }
}

sub chunked_response_handler :Event {
    my ($request_packet,$response_packet) = @_[ARG0,ARG1];
    my ($response,$content) = @$response_packet;
    my $self = $_[HEAP];
    #log_info($content);
    #print Dumper($content);
    if(!defined $content) {
        log_err("Something closed the stream...");
        if($response->code !~ /[45]\d\d/) {
            log_infof("Response needs refreshing..(got %d)", $response->code);
            $self->submit_request(@$request_packet);
        } else {
            log_err("Fatal error:");
            print Dumper($response);
            die("We were probably disconnected from primary endpoint");
        }
        return;
    }
    
    my $json = ($response->{$HKEY_JSONPARSE} ||= JSON::XS->new())
        ->incr_parse($content);
        
    if ($json) {
        my $new_response = $response->clone();
        delete $response->{$HKEY_JSONPARSE};
        $new_response->content(encode_json($json));
        $poe_kernel->yield('response_handler',
                           $request_packet, [$new_response]);
    }
}

sub unknown_event :Event(_default) {
    my $evname = $_[ARG0];
    log_warn("Got unknown event $evname");
}


sub submit_request {
    my ($self,$request,@args) = @_;
    if($request->cbc_streaming) {
        $poe_kernel->post($self->ua_streaming_alias, 'request',
                          'chunked_response_handler', $request, @args);
    } else {
        $poe_kernel->post($self->ua_alias, 'request', 'response_handler',
                          $request,@args);
    }
}


foreach my $command (@Couchbase::Config::COMMANDS)
{
    no strict 'refs';
    *{$command} = sub {
        my ($self,@args) = @_;
        my $request = $self->${\"SUPER::$command"}(@args);
        log_debugf("posting request for reqtype %s",
                  couchconf_reqtype_str($request->cbc_reqtype));
        $self->submit_request($request);
        return COUCHCONF_EXECUTING;
    };
}
1;