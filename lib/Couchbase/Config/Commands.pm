package Couchbase::Config::Commands;
use strict;
use warnings;
use Couchbase::Config::Constants;

sub _unode2obj {
    my $node = shift;
    if(!ref $node) {
        $node = Couchbase::Config::Node->ByHostname($node);
    }
    if(!$node->id) {
        die("This node is missing its ID");
    }
    return $node;
}

my %AUTHSTR = (
    COUCHCONF_AUTH_SASL, "sasl",
    COUCHCONF_AUTH_NONE, "none",
);

my %BUCKET_TYPE_STR = (
    COUCHCONF_BUCKET_MEMCACHED, "memcached",
    COUCHCONF_BUCKET_COUCHBASE, "couchbase"
);

sub _hash_no_undef(\%) {
    my $h = shift;
    foreach my $k (keys %$h) {
        delete $h->{$k} unless defined $h->{$k};
    }
}

sub list_pools {
    my $self = shift;
    my $req = $self->_new_get_request("/pools", COUCHCONF_REQ_LIST_ALL_POOLS);
}

sub pool_info {
    my ($self,$pool) = @_;
    my $uri;
    if($self->is_streaming) {
        $uri = $pool->uri_streaming;
    } else {
        $uri = $pool->uri;
    }
    my $request = $self->_new_get_request(
        $uri, COUCHCONF_REQ_POOL_INFO);
    $request->cbc_data($pool);
    $request->cbc_streaming($self->is_streaming);
    return $request;
}

sub list_buckets {
    my ($self,$pool) = @_;
    my $request = $self->_new_get_request(
        $pool->bucket_info_uri, COUCHCONF_REQ_LIST_BUCKETS);
    $request->cbc_data($pool);
    return $request;
}

sub create_bucket {
    my ($self,$pool,%params) = @_;
    my %post;
    if(!$params{name}) {
        die("New bucket must have name");
    }
    
    $post{name} = delete $params{name};
        
    my $auth = $params{auth};
    $auth ||= COUCHCONF_AUTH_NONE;
    
    my $authstr = $AUTHSTR{$auth};
    die("Unknown auth $auth") unless defined $authstr;
    $post{authType} = $authstr;
    
    if($auth == COUCHCONF_AUTH_NONE) {
        if(!$params{proxy_port}) {
            die("Proxy port must be specified if no auth");
        }
    }
    
    $post{proxyPort} = $params{proxy_port};
    
    my $btype = delete $params{type};
    if($btype) {
        my $bucketstr = $BUCKET_TYPE_STR{$btype};
        die("Unknown bucket type $btype") unless defined $bucketstr;
        $post{bucketType} = $bucketstr;
    }
    
    my $ram_quota = delete $params{ram_quota};
    if($ram_quota) {
        $post{ramQuotaMB} = $ram_quota;
    }
    
    _hash_no_undef %post;
    
    my $request = $self->_new_post_request(
        $pool->uri . "/buckets", COUCHCONF_REQ_CMD_NEW_BUCKET,
        \%post);
    
    $request->cbc_data($pool);
    $request;
}

sub delete_bucket {
    my ($self,$pool,$bucket_name) = @_;
    if(ref $bucket_name eq 'Couchbase::Config::Bucket') {
        $bucket_name = $bucket_name->name;
    }
    my $request = $self->_new_get_request($pool->uri . "/buckets/" . $bucket_name,
                                          COUCHCONF_REQ_CMD_DEL_BUCKET);
    $request->method("DELETE");
    return $request;
}

sub flush_bucket {
    my ($self,$bucket) = @_;

    my $uri = $bucket->uri_flush_cache;
    my $request = $self->_new_post_request(
        $bucket->uri_flush_cache, COUCHCONF_REQ_CMD_FLUSH_BUCKET, {foo => "bar"});
    $request->cbc_data($bucket);
    return $request;
}

sub failover_node {
    my ($self,$pool,$node) = @_;
    $node = _unode2obj($node);
    my $failover_uri = $pool->controllers->uri_failover;
    my $request = $self->_new_post_request(
        $failover_uri, COUCHCONF_REQ_CMD_FAILOVER_NODE,
        { otpNode => $node->id });
    $request->cbc_data($node);
    return $request;
}

sub eject_node {
    my ($self,$pool,$node) = @_;
    $node = _unode2obj($node);
    
    my $eject_uri = $pool->controllers->uri_eject_node;
    if(!$eject_uri) {
        log_err("Couldn't find ejectNode controller");
        return;
    }
    my $node_id = $node->id;
    my $request = $self->_new_post_request(
        $eject_uri, COUCHCONF_REQ_CMD_EJECT_NODE,
        { otpNode => $node_id });
    $request->cbc_data($node);
    return $request;
}


sub add_node {
    my ($self,$pool,$hostname,$username,$password) = @_;
    my %post = (hostname => $hostname, user => $username, password => $password);
    if(!$hostname) {
        die("Must have hostname for new node!");
    }
    my $add_uri = $pool->controllers->uri_add_node;
    _hash_no_undef %post;
        
    my $request = $self->_new_post_request(
        $add_uri, COUCHCONF_REQ_CMD_ADD_NODE,
        \%post);
    $request->cbc_data($hostname);
    return $request;
}

sub rebalance_pool {
    my ($self,$pool) = @_;
    
    my $uri = $pool->controllers->uri_rebalance;
    
    my %post;
    
    my (@known_nodes,@ejected_nodes,@all_nodes);
    foreach my $node (@{$pool->nodes}) {
        my $status = $node->{clusterMembership};
        my $otp_id = $node->id;
        if($status eq 'active') {
            push @known_nodes, $otp_id
        } else {
            push @ejected_nodes, $otp_id;
        }
    }
    
    $post{ejectedNodes} = join(",", ());
    $post{knownNodes} = join(",", @known_nodes,@ejected_nodes);
    
    my $request = $self->_new_post_request(
        $uri, COUCHCONF_REQ_CMD_REBALANCE, \%post);
    
    return $request;
}

{
    no warnings 'once';
    *rebalance_cluster = *rebalance_pool;
}


sub join_cluster {
    my ($self,$hostname,$username,$password) = @_;
    my $port;
    ($hostname,$port) = split(/:/, $hostname);
    $port ||= 8091;
    die("Must have hostname") unless ($hostname && $port);
    
    my %post = (clusterMemberHostIp => $hostname, clusterMemberPort => $port);
    if($username && $password) {
        $post{user} = $username;
        $post{password} = $password;
    }
    my $request = $self->_new_post_request(
        '/node/controller/doJoinCluster',
        COUCHCONF_REQ_CMD_JOIN_CLUSTER, \%post);
    return $request;
}

1;

__END__

=head1 NAME

Couchbase::Config::Commands - Commands for a couchbase cluster

=head1 Documentation Conventions

Each command will be titled under its logical higher-level name in the world of
couchbase cluster management. Two or more code sections will follow, explaining
the kind of request constant attached to the request object; and explaining
the kind of things which may be returned by C<update_context>

The request type for the request may be discovered by

    $request->cbc_reqtype;
    

Return types indicate those objects which may be returned by the command,
B<in addition> to those returned by C<update_context>
(see L<Couchbase::Config/update_context> for more information).

=head2 Listing Pools

    $r = $config->list_pools();
    $r->cbc_reqtype == COUCHCONF_REQTYPE_LIST_POOLS;
    
Request to connect to a node, and discover node pools available. Currently
only the default pool will ever be found, but is necessary as the first step in
the client bootstrapping process.

    $ret = $config->update_context($request,$response);
    
    $ret->isa('Couchbase::Config::Pool') ||
    $ret == COUCHCONF_ENOPOOLS;
    
C<update_context> will return either a C<Couchbase::Config::Pool> resource object,
indicating success.

If no pools can be found, C<COUCHCONF_ENOPOOLS> will be returned. This is most
likely an indicator that this node is not joined to any cluster.

=head2 Getting Pool Information

    $r = $config->pool_info($pool_resource);
    $r->cbc_reqtype == COUCHCONF_REQ_POOL_INFO;
    
Request more information about the pool, such as balanced status, node memebers,
and member node statuses. This will also retrieve the URIs needed for extra
information such as bucket parameters and node control operations.

    $ret = $config->update_context($request, $response);
    
    $ret->isa('Couchbase::Config::Pool') && $pool_resource = $ret;
    
C<update_context> returns the pool updated (which is the same pool passed to it).

=head2 Getting Bucket Information

    $r = $config->list_buckets($pool_resource);
    $r->cbc_reqtype == COUCHCONF_REQ_LIST_BUCKETS
    $r->cbc_streaming; #may be streaming.
    
Request information about the buckets located within the pool. This will fetch
bucket status, replica copies, and mapping parameters.

    $ret = $config->update_context($request, $response);
    $ret == [ $bucket1, $bucket2, ...];
    
Returns an array reference of C<Couchbase::Config::Bucket> resource objects.

=head2 Creating a New Bucket

    $r = $config->create_bucket($pool_resource,
        name => "new_bucket",
        type => COUCHCONF_BUCKET_*,
        ram_quota => $megabytes,
        auth => COUCHCONF_AUTH_*,
        ..)
    $r->reqtype == COUCHCONF_REQ_CMD_NEW_BUCKET;
    
Create a new bucket. You must supply some parameters for bucket construction
as a hash of options.

TOOD: add more parameters

=over

=item name

Required.

This is the name for the new bucket


=item type

Required.

This should indicate the bucket type. use C<COUCHCONF_BUCKET_MEMCACHED> for a
memcached-style bucket (non-replicated, non-persisting), or
C<COUCHCONF_BUCKET_COUCHBASE> for a couchbase bucket (persisting, optionally
replicating).

This has no default.


=item auth

Specify the authentication type. It may be one of C<COUCHCONF_AUTH_NONE>
in which case, no authentication is used, or C<COUCHCONF_AUTH_SASL> in which
case SASL authentication is used.

If using SASL, then a C<password> parameter should be supplied
If using NONE, then a C<proxy_port> parameter must be specified

If not specified, the default is C<COUCHCONF_AUTH_NONE>.

=item proxy_port

Must be supplied if no authentication is used.

TOOD: i don't know what this actually means :(

=item ram_quota

Specify the amount of memory this bucket will occupy on each node.

If not provided, it will default to the server default (which at the time of
writing happens to be 128MB)

=back

    $r = $config->update_context($request,$response);
    $r == COUCHCONF_OK ||
    $r == COUCHCONF_EREBALANCING;
    
C<update_context> can return an OK status, indicating the request has been
successfully submitted, or C<COUCHCONF_EREBALANCING> which means the bucket
cannot be created because the cluster is in middle of a rebalance operation.

In order to get the resource object for the newly created bucket, it is required
to perform a C<list_buckets> command again, ideally, after a small wait period.

=head2 Flushing a Bucket

    $r = $config->flush_bucket($bucket_resource);
    $r->cbc_reqtype == COUCHCONF_REQ_CMD_FLUSH_BUCKET;
    
Flush a bucket.

Note that the bucket specifier must be a resource object, and not a bucket name.
This is because the command requires extra internal parameters which are stored
within the resource object.

C<update_context> will return a C<COUCHCONF_OK> on success.

=head2 Deleting a Bucket

    $r = $config->delete_bucket($pool_resource, $bucket_name_or_resource);
    $r->cbc_reqtype == COUCHCONF_REQ_CMD_DEL_BUCKET;
    
Deletes a bucket from a pool. The C<$bucket> specified by either be a
C<Couchbase::Config::Bucket> or a bucket name. Unlike C<flush_bucket>, deleting
a bucket requires no parameters from the bucket resource object.

    $ret = $config->update_context;
    $ret == COUCHCONF_OK ||
    $ret == COUCHCONF_ERROR;
    
There are no special return values from C<update_context>

=head2 Removing a Node From the Cluster Pool

Removing a node can involve two steps. First, the node must be failed over, so
that the keys it is hosting can be relocated to another server. Second, it must
be ejected from the pool.

=head2 Fail Over a Node from a Cluster Pool

    $r = $config->failover_node($pool_resource, $node_resource);
    $r->cbc_reqtype == COUCHCONF_REQ_CMD_FAILOVER_NODE;
    
Fail over a node C<$node_resource> located in the pool C<$pool_resource>

The C<$node_resource> is one of the nodes in the array reference returned by
    
    $resource_pool->nodes;


C<update_context> will return the standard OK or ERROR constant depening on the
result of the operation.


=head2 Ejecting a Node From a Cluster Pool

    $r = $config->eject_node($pool_resource,$node_resource);
    $r->cbc_reqtype == COUCHCONF_REQ_CMD_EJECT_NODE;

The node must have first been failed over, or not yet activated.

    $ret = $config->update_context($request,$response);
    $ret == COUCHCONF_ENODEACTIVE;
    
C<update_context> will return a C<COUCHCONF_ENODEACTIVE> if the node requested is
still active. This means you must call C<failover_node> on the node resource, and
then call C<eject_node>.

=head2 Adding a New Node to the Cluster

    $r = $config->add_node($pool, $hostname, $username, $password);
    $r->cbc_reqtype == COUCHCONF_REQ_CMD_ADD_NODE;
    
Adds a new node to the cluster. You must specify a hostname (in the
format of C<host> or C<host:port>), and username/password parameters if needed.

The hostname and auth parameters are for accessing the REST interface of the node
to be added.

    $ret = $config->update_context($request,$response);
    $ret == COUCHCONF_OK ||
    $ret == COUCHCONF_ERROR ||
    $ret == COUCHCONF_ENODEACTIVE;
    
C<update_context> will return the standard OK or ERROR constants depending on the
result. If the node is already part of a cluster, C<COUCHCONF_ENODEACTIVE> will
be returned.

In the event of C<ENODEACTIVE>, refresh the pool information via C<pool_info> to
see if the node is already joined.

Note that you will most likely want a rebalance after adding a node.

=head2 Rebalancing the Cluster

    if($pool_resource->needs_rebalance) {
        $r = $config->rebalance_pool($pool_resource);
        $r = $config->rebalance_cluster($pool_resource); #alias
        $r->cbc_reqtype == COUCHCONF_REQ_CMD_REBALANCE;
    }
    
Rebalance a cluster, taking note of failed and added servers.

It is advised to check if the pool needs rebalancing before issuing the
command (see example above).

    $r = $config->update_context($request, $response);
    $r == COUCHCONF_OK || $r == COUCHCONF_ERROR;
    
C<update_contxt> will return an OK or ERROR context depending on the operation.

Currently error types are ambiguous about whether the pool does not need rebalancing,
or whether a rebalance is in progress, or whether the rebalance has failed for
other reasons. It is therefore recommended to refresh the pool information as well
via C<pool_info>.

=head2 Joining a Cluster

Tell the currently-connected node to join a cluster.

    $r = $config->join_cluster($hostname,$username,$password);
    $r->cbc_reqtype == COUCHCONF_REQ_CMD_JOIN_CLUSTER;
    
Supply the hostname, and optionally username/password parameters for any node
in the I<existing> cluster. Our node will connect to it internally and perform
the necessary steps needed for joining the cluster.

    $r = $config->update_context($request, $response);
    $r->isa('Couchbase::Config::Request') ||
    $r->isa('Couchbase::Config::Pool') ||
    $r == COUCHCONF_EINVAL;
    
C<update_context> will return a C<Couchbase::Resource::Pool> object if successful.

The Couchbase REST API documentation says that a successful join will contain
pool information in the response body, but this does not happen on my installation;
thus, if pool information is not received upon a successful join response,
C<update_context> will return a request object.

If there was a problem joining, typically due to bad parameters, then a
C<COUCHCONF_EINVAL> will be returned.

