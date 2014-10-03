#!/usr/bin/perl

use strict;
use warnings;

use Net::SNMP qw(:snmp);
use Data::Dumper;
use DBI qw(:sql_types);

# OIDs
my $targetIP            = "1.3.6.1.4.1.9.9.42.1.2.2.1.2";
my $tag                 = "1.3.6.1.4.1.9.9.42.1.2.1.1.3";
my $tos                 = "1.3.6.1.4.1.9.9.42.1.2.2.1.9";
my $latestRttOper       = "1.3.6.1.4.1.9.9.42.1.2.10.1";
my $sysUpTime           = "1.3.6.1.2.1.1.3.0";

# variables
my $ipslaRouter         = "<ip addr of IP SLA router>";
my $readCommunity       = "<RO community string>";
my $baseOID             = 0;
my $result              = 0;
my $ticks               = 0;
my %table;

# ----------------------------------------------------------------------------------------
# create the SNMP connection
# ----------------------------------------------------------------------------------------
my ($session, $error) = Net::SNMP->session(
                -version        => 'snmpv2c',
                -nonblocking    => 1,
                -timeout        => 30,
                -hostname       => $ipslaRouter,
                -community      => $readCommunity,
                -translate      => [ -timeticks => 0x0, -octetstring => 0x0 ],
);

if (!defined($session)) {
        printf("ERROR: %s.\n", $error);
        exit (-1);
}

# ----------------------------------------------------------------------------------------
# get sysUpTime
# ----------------------------------------------------------------------------------------
$baseOID = $sysUpTime;
my $uptime = 0;
$result = $session->get_request(
                -varbindlist    => [ $baseOID ],
                -callback       => [\&validate_sysUpTime_cb, \$uptime],
);

if (!defined $result) {
        printf "ERROR: %s\n", $session->error();
        $session->close();
        exit 1;
}

snmp_dispatcher();


# ----------------------------------------------------------------------------------------
# get the operation Tag
# ----------------------------------------------------------------------------------------
$baseOID = $tag;
$result = $session->get_bulk_request(
                -varbindlist    => [ $baseOID ],
                -callback       => [ \&table_callback, \%table ],
                -maxrepetitions => 20,
);

if (!defined $result) {
        printf "ERROR: %s\n", $session->error();
        $session->close();
        exit 1;
}

snmp_dispatcher();


# ----------------------------------------------------------------------------------------
# get the ToS value
# ----------------------------------------------------------------------------------------
$baseOID = $tos;
$result = $session->get_bulk_request(
                -varbindlist    => [ $baseOID ],
                -callback       => [ \&table_callback, \%table ],
                -maxrepetitions => 20,
);

if (!defined $result) {
        printf "ERROR: %s\n", $session->error();
        $session->close();
        exit 1;
}

snmp_dispatcher();


# ----------------------------------------------------------------------------------------
# get the rttMonLatestRttOperEntry table
# ----------------------------------------------------------------------------------------
$baseOID = $latestRttOper;
$result = $session->get_bulk_request(
                -varbindlist    => [ $baseOID ],
                -callback       => [ \&table_callback, \%table ],
                -maxrepetitions => 20,
);


if (!defined $result) {
        printf "ERROR: %s\n", $session->error();
        $session->close();
        exit 1;
}

snmp_dispatcher();


# ---------------------------------------------------------------------------------------
# done with the SNMP session, close it
# ---------------------------------------------------------------------------------------
$session->close();


# ---------------------------------------------------------------------------------------
# print the results
# ---------------------------------------------------------------------------------------
#print Dumper(\%table);



# ---------------------------------------------------------------------------------------
# loop on the results hash and add to database
# ---------------------------------------------------------------------------------------
my $dbh = DBI->connect('DBI:mysql:database=$dbname;host=$dbserver', $dbuser, $dbpw, { RaiseError => 1 }) ||
        die "Error opening database: $DBI::errstr\n";

my $sth = $dbh->prepare(q{
        CALL InsertIpSlaResult(?, ?, ?, ?, ?, ?, ?)
}) || die "Couldn't prepare statement: $dbh->errstr\n";


my %month2num = qw(
  jan 1  feb 2  mar 3  apr 4  may 5  jun 6
  jul 7  aug 8  sep 9  oct 10 nov 11 dec 12
);

foreach my $ifIndex (keys %table)
{
        my ( $month, $day, $hour, $min, $sec, $year ) = ( $table{$ifIndex}{OperTime} =~ m{\w{3}\s+(\w{3})\s+(\d+)\s+(\d{2}):(\d{2}):(\d{2})\s(\d{4})} );
        my $dt = $year . "-" . sprintf("%02d",$month2num{ lc substr($month, 0, 3) }) . "-" . sprintf("%02d",$day) . " " . $hour . ":" . $min . ":" . $sec;

        $sth->bind_param(1, $dt, SQL_DATETIME);
        $sth->bind_param(2, $table{$ifIndex}{RTT});
        $sth->bind_param(3, $table{$ifIndex}{Status}, SQL_TINYINT);
        $sth->bind_param(4, $ipslaRouter);
        $sth->bind_param(5, $table{$ifIndex}{DestIP});
        $sth->bind_param(6, $table{$ifIndex}{ToS});
        $sth->bind_param(7, $table{$ifIndex}{Tag});

#        printf("CALL InsertIpSlaResult(%s, %s, %s, %s, %s, %s, %s)\n", $dt, SQL_DATETIME, $table{$ifIndex}{RTT}, $table{$ifIndex}{Status}, $ipslaRouter, $table{$ifIndex}{DestIP}, $table{$ifIndex}{ToS}, $table{$ifIndex}{Tag});

        eval { $sth->execute() };       #|| die "Couldn't execute statement: $dbh->errstr\n";
}

#$dbh->commit || die "Couldn't commit: $dbh->errstr\n";
$sth->finish();
$dbh->disconnect();



# =======================================================================================
#
#   SUBROUTINES
#
# =======================================================================================

# ---------------------------------------------------------------------------------------
# callback subrouting
# ---------------------------------------------------------------------------------------
sub table_callback
{
        my ($session, $table) = @_;

        my $list = $session->var_bind_list();

        if (!defined $list) {
                printf "ERROR: %s\n", $session->error();
                return;
        }

        # Loop through each of the OIDs in the response and assign
        # the key/value pairs to the reference that was passed with
        # the callback.  Make sure that we are still in the table
        # before assigning the key/values.

        my @names = $session->var_bind_names();
        my $next  = undef;

        while (@names) {
                $next = shift @names;
                if (!oid_base_match($baseOID, $next)) {
                        return; # Table is done.
                }

                my $ifIndex = $next;
                my $replace = $baseOID . '\.(\d\.)?';
                $ifIndex =~ s/$replace//g;

                if ($next =~ m/1.3.6.1.4.1.9.9.42.1.2.10.1.5/) {
#                       print localtime(time() - ($uptime - $list->{$next})/100) . "\n";
                        $table{$ifIndex}{OperTime} = localtime(time() - ($uptime - $list->{$next})/100);
                } elsif ( $next =~ m/1.3.6.1.4.1.9.9.42.1.2.10.1.6/ ) {
                        my $hexIP = unpack('H*', $list->{$next});
                        $table{$ifIndex}{DestIP} = join '.', unpack "C*", pack "H*", $hexIP;
                } elsif ( $next =~ m/1.3.6.1.4.1.9.9.42.1.2.10.1.1/ ) {
                        $table{$ifIndex}{RTT} = $list->{$next};
                } elsif ( $next =~ m/1.3.6.1.4.1.9.9.42.1.2.10.1.2/ ) {
                        $table{$ifIndex}{Status} = $list->{$next};
                } elsif ( $next =~ m/1.3.6.1.4.1.9.9.42.1.2.1.1.3/ ) {
                        $table{$ifIndex}{Tag} = $list->{$next};
                } elsif ( $next =~ m/1.3.6.1.4.1.9.9.42.1.2.2.1.9/ ) {
                        $table{$ifIndex}{ToS} = $list->{$next};
                }
        }

        # Table is not done, send another request, starting at the last
        # OBJECT IDENTIFIER in the response.  No need to include the
        # calback argument, the same callback that was specified for the
        # original request will be used.

        my $result = $session->get_bulk_request(
                        -varbindlist    => [ $next ],
                        -maxrepetitions => 10,
        );

        if (!defined $result) {
                printf "ERROR: %s.\n", $session->error();
        }

        return;
}



sub validate_sysUpTime_cb
{
   if (!defined($session->var_bind_list)) {
      printf("%-15s  ERROR: %s\n", $session->hostname, $session->error);
   } else {
      $uptime = $session->var_bind_list->{$sysUpTime};
   }

   $session->error_status;
}
