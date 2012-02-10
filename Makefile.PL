#!/usr/bin/perl
use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME                => 'Couchbase::Config',
    AUTHOR              => q{M. Nunberg <mnunberg@haskalah.org>},
    VERSION_FROM        => 'lib/Couchbase/Config.pm',
    ABSTRACT_FROM       => 'lib/Couchbase/Config.pm',
    ($ExtUtils::MakeMaker::VERSION >= 6.3002
      ? ('LICENSE'=> 'perl')
      : ()),
    PL_FILES            => {},
    PREREQ_PM => {
        'Test::More' => 0,
        'List::Compare' => 0,
        'LWP::UserAgent' => 6.0,
        'Constant::Generate' => 0.16,
        'JSON::XS'  => 2.3,
        'Data::Dumper::Concise' => 0,
    },
    dist                => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean               => { FILES => 'Couchbase-Config-*' },
);
