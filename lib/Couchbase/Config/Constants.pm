package Couchbase::Config::Constants;
use base qw(Exporter);
our @EXPORT;

use Constant::Generate [qw(
    LIST_ALL_POOLS
    POOL_INFO
    LIST_BUCKETS
    STAT_BUCKET
    
    CMD_NEW_BUCKET
    CMD_DEL_BUCKET
    CMD_FLUSH_BUCKET
    CMD_EJECT_NODE
    CMD_ADD_NODE
    CMD_REBALANCE
    CMD_FAILOVER_NODE
    CMD_JOIN_CLUSTER
    
)], prefix => 'COUCHCONF_REQ_',
    export => 1,
    mapname => 'couchconf_reqtype_str',
    allsyms => 'couchconf_reqtypes';

use Constant::Generate [qw(
    BUCKET_MEMCACHED
    BUCKET_COUCHBASE
    AUTH_NONE
    AUTH_SASL
)], -export =>1,
    prefix => 'COUCHCONF_',
    -start_at => 1;

use Constant::Generate [qw(
    REBALANCE_RUNNING
    REBALANCE_NEEDED
    REBALANCE_OK
)], export => 1, prefix => "COUCHCONF_", dualvar => 1, start_at => 1;

use Constant::Generate [qw(
    OK
    EXECUTING
    ERROR
    ENODEACTIVE
    EREBALANCING
    ESERVER
    ENOPOOLS
    EAUTH
    EINVAL
)], prefix => "COUCHCONF_", export => 1, dualvar => 1,
    start_at => 100;

1;