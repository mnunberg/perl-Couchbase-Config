package Couchbase::Config::Handlers;
use strict;
use warnings;
use Couchbase::Config::Constants;
use Couchbase::Config::Resource;
use Log::Fu;
use Data::Dumper::Concise;

sub HANDLER_list_pools {
    my ($self,$request,$response,$hash) = @_;
    my @pool_hashes;
    my @ret;
    
    #print Dumper($hash);
    if(ref $hash->{pools} eq 'HASH') {
        push @pool_hashes, $hash->{pools};
    } else {
        push @pool_hashes, @{$hash->{pools}};
    }
    if(!@pool_hashes) {
        return COUCHCONF_ENOPOOLS;
    }
    foreach my $ph (@pool_hashes) {
        my $obj = Couchbase::Config::Pool->from_structure($ph);
        push @ret, $obj;
        $self->pools->{$obj->name} = $obj;
    }
    #print Dumper($self->pools);
    return $self->pools->{default};
}

sub HANDLER_pool_info {
    my ($self,$request,$response,$hash) = @_;
    my $pool = $request->cbc_data;
    $pool->from_structure($hash);
    return $pool;
}

sub HANDLER_list_buckets {
    my ($self,$request,$response,$hash) = @_;
    my $pool = $request->cbc_data;
    my @bucket_specs;
    my @ret;
    
    if(ref $hash eq 'HASH') {
        push @bucket_specs, $hash;
    } else {
        @bucket_specs = @{$hash};
    }
    foreach my $spec (@bucket_specs) {
        my $bucket = Couchbase::Config::Bucket->from_structure($spec);
        push @ret, $bucket;
        $spec->{vBucketServerMap}->{vBucketMap} = 'Deleted for brevity';
    }
    $pool->update_bucket_list(\@ret);
    #print Dumper(@bucket_specs);
    #if(!@bucket_specs) {
    #    die("No bucket specifiers found!");
    #}
    return \@ret;
}

sub HANDLER_create_bucket {
    my ($self,$request,$response,$hash) = @_;
    if($response->code == 202) {
        return COUCHCONF_OK;
    }
    my $msg = $response->content;
    if($msg =~ /during rebalance/) {
        return COUCHCONF_EREBALANCING;
    }
    return COUCHCONF_ERROR;
}
sub HANDLER_del_bucket {
    my ($self,$request,$response,$hash) = @_;
    if($response->is_success) {
        return COUCHCONF_OK;
    } else {
        return COUCHCONF_ERROR;
    }
}

sub HANDLER_flush_bucket {
    my ($self,$request,$response,$hash) = @_;
    if($response->is_success()) {
        return COUCHCONF_OK;
    } else {
        return COUCHCONF_ERROR;
    }
}

sub HANDLER_eject_node {
    my ($self,$request,$response,$hash) = @_;
    #return $response->is_success;
    if($response->is_success) {
        return COUCHCONF_OK;
    } elsif($response->decoded_content =~ /Cannot remove active server/) {
        return COUCHCONF_ENODEACTIVE;
    } else {
        return COUCHCONF_ERROR;
    }   
}

sub HANDLER_failover_node {
    my ($self,$request,$response,$hash) = @_;
    if($response->is_success) {
        return COUCHCONF_OK;
    } else {
        return COUCHCONF_ERROR;
    }
}

sub HANDLER_add_node {
    my ($self,$request,$response) = @_;
    
    my $message = $response->content;
    if($response->is_success) {
        return COUCHCONF_OK;
    }
    if($message =~ /already part of cluster/) {
        return COUCHCONF_ENODEACTIVE;
    }
    return COUCHCONF_ERROR;
}

sub HANDLER_rebalance_cluster {
    my ($self,$request,$response) = @_;
    if($response->is_success) {
        return COUCHCONF_OK;
    } else {
        return COUCHCONF_ERROR;
    }
}

sub HANDLER_join_cluster {
    my ($self,$request,$response,$hash) = @_;
    if($response->code == 200) {
        log_info("Join OK");
        if($hash) {
            return Couchbase::Config::Pool->from_structure($hash);
        } else {
            log_warn("No pool data found in response.");
            print $response->as_string;
            return $self->list_pools();
        }
    }
    if($response->code == 400) {
        if($response->content =~ /node to itself/) {
            log_errf("Tried to join node to itself. URI %s",
                     $request->uri);
        }
        print $response->as_string;
        return COUCHCONF_EINVAL;
    }
    return COUCHCONF_ERROR; 
}

my %HANDLERS = (
    COUCHCONF_REQ_LIST_ALL_POOLS, \&HANDLER_list_pools,
    COUCHCONF_REQ_POOL_INFO, \&HANDLER_pool_info,
    COUCHCONF_REQ_LIST_BUCKETS, \&HANDLER_list_buckets,
    
    COUCHCONF_REQ_CMD_NEW_BUCKET, \&HANDLER_create_bucket,
    COUCHCONF_REQ_CMD_DEL_BUCKET, \&HANDLER_del_bucket,
    COUCHCONF_REQ_CMD_FLUSH_BUCKET, \&HANDLER_flush_bucket,
    COUCHCONF_REQ_CMD_EJECT_NODE, \&HANDLER_eject_node,
    COUCHCONF_REQ_CMD_FAILOVER_NODE, \&HANDLER_failover_node,
    COUCHCONF_REQ_CMD_ADD_NODE, \&HANDLER_add_node,
    COUCHCONF_REQ_CMD_REBALANCE, \&HANDLER_rebalance_cluster,
    COUCHCONF_REQ_CMD_JOIN_CLUSTER, \&HANDLER_join_cluster,
);

sub get_handler {
    my ($self,$reqtype) = @_;
    $HANDLERS{$reqtype};
}

1;