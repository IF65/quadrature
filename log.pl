#!/usr/bin/perl -w
use strict;

# by Marco Gnecchi

use DBI;
use DateTime;
use Data::Dumper;
use Log::Log4perl;

# Initialize Logger
my $log_conf = "/script/log4perl.conf";
Log::Log4perl::init($log_conf);
my $logger = Log::Log4perl->get_logger();
# sample logging statement
$logger->info("Ciao");
$logger->info("Ciao1");
$logger->info("Ciao2");
print "LIST\n";
