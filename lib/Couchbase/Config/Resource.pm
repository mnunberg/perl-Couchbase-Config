#This file primarily handles parsing and converting JSON structures into objects.
#we will sometimes convert semantics and such stuff to abstract complexity and
#add perl-like uniformity.

package Couchbase::Config::Resource;
use strict;
use warnings;
use List::Compare;

use Class::XSAccessor {
    accessors => [qw(_data user_data changelog _old_data)]
};

sub create_changelog {
    return {};
}


sub get_old_new {
    my ($self,$old_list,$new_list) = @_;
    my %maphash = map { $_, $_ } (@$old_list,@$new_list);
    my $lcp = List::Compare->new($old_list, $new_list);
    my @old = delete @maphash{$lcp->get_Lonly};
    my @new = delete @maphash{$lcp->get_Ronly};
    return (\@old, \@new);
}


#To hook into, possibly to create a new object
sub locate_existing {
    my ($self,$hash) = @_;
    return;
}

#constructor-like method. Given a hash, give us an object.
sub from_structure {
    my ($o,$hash) = @_;
    my $self;
    if(!ref $o) {
        if( !($self = $o->locate_existing($hash) ) ) {
            #constructor
            $self = { %$hash };
            $self->{changelog} = $o->create_changelog();
            bless $self, $o;
        }
    } else {
        $self = $o;
    }
    
    $self->_old_data($self->_data);
    
    my $data = $self->_data || {};
    %$data = (%$data, %$hash);
    $self->_data($data);
    
    $self->ds_extract($hash);
    return $self;
}


#May be overriden by subclasses to implement more complex conversion schemes.
sub ds_extract {
    
}


#Class representing a node, a host within a cluster.
package Couchbase::Config::Node;
use strict;
use warnings;
use base qw(Couchbase::Config::Resource);
use Log::Fu;

our %Nodes;
use Class::XSAccessor {
    constructor => 'new',
    accessors => [qw(
        base_addr
        cluster_addr
        port_proxy
        port_direct
        hostname
        status
        clusterMembership
        version
        otpNode
        otpCookie
    )]
};

*id = *otpNode;
*cookie = *otpCookie;
*status_in_cluster = *clusterMembership;

sub ByHostname {
    my ($cls,$hostname) = @_;
    if($hostname !~ /:/) {
        $hostname .= ":8091";
    }
    my $existing = $Nodes{$hostname};
    if(!$existing) {
        $existing = __PACKAGE__->new(
            cluster_addr => $hostname,
            changelog => $cls->create_changelog());
        $Nodes{$hostname} = $existing;
        
        $hostname =~ s/:.+$//g;
        $existing->base_addr($hostname);
    }
    return $existing;
}

sub ds_extract {
    my ($self,$data) = @_;
    my $ports = delete $data->{ports};
    
    my $changelog = $self->changelog;
    
    foreach my $k (qw(status clusterMembership)) {
        if (($self->{$k} || "") ne ($data->{$k} || "" )) {
            $changelog->{$k} = $self->{$k};
        } else {
            delete $changelog->{$k};
        }
    }
    
    %$self = ( %$self, %$data );
    $self->port_direct($ports->{direct});
    $self->port_proxy($ports->{proxy});
}

sub is_ok {
    my $self = shift;
    $self->status eq 'healthy';
}


package Couchbase::Config::Pool::Operations;
use strict;
use warnings;
use base qw(Couchbase::Config::Resource);
use Class::XSAccessor {
    accessors => [qw(
        uri_add_node
        uri_eject_node
        uri_readd_node
        uri_rebalance
        uri_failover
        uri_test_workload
        uri_rebalance
    )]
};

our %json_map = (
    reAddNode => 'add_node',
    rebalance => 'rebalance',
    ejectNode => 'eject_node',
    failOver => 'failover',
    addNode => 'add_node',
    testWorkload => 'test_workload',
    rebalance => 'rebalance',
);

sub ds_extract {
    my ($self,$data) = @_;
    while (my ($json_name,$obj_name) = each %json_map) {
        $obj_name = "uri_$obj_name";
        if(exists $data->{$json_name}->{uri}) {
            $self->{$obj_name} = $data->{$json_name}->{uri};
        }
        delete $self->{$json_name} unless $json_name eq $obj_name;
    }
}

package Couchbase::Config::Pool::Diff;
use strict;
use warnings;
use Class::XSAccessor {
    accessors => [qw(
        nodes_added
        nodes_removed
        buckets_added
        buckets_removed
        balance_state
    )]
};

#Generate a diff between a new and old

package Couchbase::Config::Pool;
use strict;
use warnings;
use base qw(Couchbase::Config::Resource);
use Data::Dumper;
use Log::Fu;
use List::Compare;
use Couchbase::Config::Constants;

use Class::XSAccessor {
    accessors => [qw(
        name uri streamingUri
        controllers
        bucket_info_uri
        uri_status
        our_buckets
        nodes
        balanced
        balance_state
    )]
};

sub create_changelog {
    bless {}, 'Couchbase::Config::Pool::Diff';
}

sub add_bucket {
    my ($self,$bucket) = @_;
    if(!(grep($_ == $bucket, @{$self->{our_buckets}||=[]}))) {
        push @{$self->our_buckets}, $bucket;
        $bucket->pool($self);
    }
}

sub get_buckets {
    my $self = shift;
    $self->our_buckets || [];
}

sub delete_bucket {
    my ($self,$bucket) = @_;
    $self->{our_buckets} =
        [ grep { $_ != $bucket } @{ $self->our_buckets||[] } ];
    if($bucket->pool == $self) {
        $bucket->pool(undef);
        $bucket->destroy_bucket();
    }
}

sub update_bucket_list {
    my ($self,$new_buckets) = @_;
    my ($old_list,$new_list) = $self->get_old_new(
        $self->get_buckets || [], $new_buckets);
    
    $self->changelog->buckets_added($new_list);
    $self->changelog->buckets_removed($old_list);
    $self->delete_bucket($_) for @$old_list;
    $self->add_bucket($_) for @$new_list;
}

*uri_plain = *uri;
*uri_streaming = *streamingUri;

sub needs_rebalance {
    my $self = shift;
    #already in progress
    if($self->_data->{rebalanceStatus} &&
       $self->_data->{rebalanceStatus} eq 'running') {
        return 0;
    }
        
    #implicitly not balanced
    foreach my $node (@{$self->nodes}) {
        if($node->_data->{clusterMembership} eq 'inactiveAdded') {
            log_err("Found inactiveAdded:");
            #log_err(Dumper($self->_data));
            return 1;
        }
    }
    #explicitly not balanced
    if(!$self->balanced) {
        return 1;
    }

    return 0;
}


sub _mk_balance_state {
    my $self = shift;
    
    my $bstatus = $self->_data->{rebalanceStatus};
    if($bstatus && $bstatus eq 'running') {
        $self->balance_state(COUCHCONF_REBALANCE_RUNNING);
        return;
    }
    
    if(defined $self->_data->{balanced} && $self->_data->{balanced} == 0) {
        $self->balance_state(COUCHCONF_REBALANCE_NEEDED);
        return;
    }
    
    $self->balance_state(COUCHCONF_REBALANCE_OK);
}

sub ds_extract {
    my ($self,$hash) = @_;
    
    $self->controllers(
        Couchbase::Config::Pool::Operations->from_structure(
            $hash->{controllers} || {}));
    
    if($hash->{buckets}->{uri}) {
        $self->bucket_info_uri($hash->{buckets}->{uri});
    }
    
    my ($old_list,$new_list);
    my $changelog = $self->changelog;
    my $old_balance_state = $self->balance_state;
    
    $self->_mk_balance_state();
    my $new_balance_state = $self->balance_state;
    
    $old_balance_state ||= -1;
    if($old_balance_state != $new_balance_state) {
        $changelog->balance_state($old_balance_state);
    } else {
        $changelog->balance_state(undef);
    }
    
    #figure out which nodes went missing:
    my @new_nodes = map {
        my $node_data = $_;
        my $node = Couchbase::Config::Node->ByHostname($node_data->{hostname});
        $node->from_structure($node_data);
        $node;
    } (@{$hash->{nodes} });
    my @cur_nodes = @{$self->nodes || []};
    
    ($old_list,$new_list) = $self->get_old_new(\@cur_nodes, \@new_nodes);
    
    $changelog->nodes_removed($old_list);
    $changelog->nodes_added($new_list);
    
    
    
    $self->nodes(\@new_nodes);
    $self->balanced($self->_data->{balanced});
}

package Couchbase::Config::Bucket;
use strict;
use warnings;
my $have_couchbase_vbucket = eval 'use Couchbase::VBucket; 1';
use JSON::XS;
use Data::Dumper;
use Log::Fu;
use base qw(Couchbase::Config::Resource);

use Class::XSAccessor {
    constructor => 'new',
    accessors => [qw(
        name nodes vbconf json pool
        flushCacheUri
        bucketType
        proxyPort
        diff
    )]
};

*uri_flush_cache = *flushCacheUri;
*type = *bucketType;
*port_proxy = *proxyPort;

our %Buckets;

sub ByName {
    my ($cls,$name) = @_;
    my $bucket = $Buckets{$name};
    if(!$bucket) {
        $bucket = __PACKAGE__->new(name => $name);
    }
    return $bucket;
}

sub locate_existing {
    my ($cls,$hash) = @_;
    my $name = $hash->{name};
    if(exists $Buckets{$name}) {
        return $Buckets{$name};
    } else {
        my $bucket = bless {}, __PACKAGE__;
        %$bucket = %$hash;
        $Buckets{$name} = $bucket;
        return $bucket;
    }
}

sub destroy_bucket {
    my ($self,$bucket) = @_;
    $bucket ||= $self;
    delete $Buckets{$bucket->name};
}

sub ds_extract {
    my ($self,$hash) = @_;
    
    if($have_couchbase_vbucket) {
        my $vb = Couchbase::VBucket->parse(encode_json($hash));
        if($vb) {
            $self->vbconf($vb);
        } else {
            log_err("Couldn't parse vbucket config");
        }
    }
    
    my @nodes_array;
    
    foreach my $node (@{$hash->{nodes} }) {
        my $cluster_addr = $node->{hostname};
        if($cluster_addr !~ /:/) {
            next;
        }
        my $nodeobj = Couchbase::Config::Node->ByHostname($cluster_addr);
        $nodeobj->from_structure($node);
        push @nodes_array, $nodeobj;
    }
    $self->nodes(\@nodes_array);
}

sub map_key {
    my ($self,$key) = @_;
    if($self->vbconf) {
        $self->vbconf->map($key);
    } else {
        return;
    }
}

1;