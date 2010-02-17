package Staple::DB::Factory;

#
# Copyright (C) 2007-2010 Hebrew University Of Jerusalem, Israel
# See the LICENSE file.
#
# Author: Yair Yarom <irush@cs.huji.ac.il>
#

use strict;
use warnings;
use Staple::Misc;
use Staple::DB::FS;
use Staple::DB::SQL;
require Exporter;


=head1 NAME

  Staple::DB::Factory - Miscellaneous Staple utilities

=head1 SYNOPSIS

use Staple::DB::Factory;

=head1 DESCRIPTION

Factory for Staple::DB

=head1 EXPORT

=over

=item createDB([db type, db parameters])

=back

=cut

our @ISA = qw(Exporter);
our @EXPORT_OK = qw();
our @EXPORT = qw(
                    createDB
               );
our $VERSION = '005';


################################################################################
#   Exported
################################################################################


=head1 DESCRIPTION

=over

=item B<createDB(I<[db type, db parameters]>)>

Returns a new Staple::DB. If no parameters are given, use the __STAPLE_DB__
token from the conf file: /etc/staple/staple.conf. If doesn't exists, defaults
to fs /boot/staple.

On failure retruns an error string. This will change once a Staple::DB::Error
will be created (or a different method for returning errors). So better check
for $db->error also.

The fist parameter is the db type, either "fs" (Staple::DB::FS) or "sql"
(Staple::DB::SQL). The rest of the parameters are db type dependant.

=over

=item * fs

Use Staple::DB::FS as the staple database. The only parameter is the directory,
defaults to "/boot/staple".

=item * sql

Use Staple::DB::SQL as the staple database. The first parameter is the schema
to use, the default is "staple". If an empty string is given, no schema
assumed.  The second parameter is database connection parameters, the default
is "dbi:Pg:dbname=staple;host=pghost;port=5432;". The third and forth are the
username and password (defaults to undef).

=back

=cut

sub createDB {
    my $type = shift;
    my @params = @_;
    unless ($type) {
        my %tokens = readTokensFile($Staple::defaultTokens{__STAPLE_CONF__}->{value}, "static");
        ($type, @params) = split /\s+/, $tokens{__STAPLE_DB__}->{value} if ($tokens{__STAPLE_DB__});
    }
    $type = "fs" unless $type;
    my $db = "Unknown error";
    if ($type eq "fs") {
        my $stapleDir = $params[0] ? $params[0] : "/boot/staple";
        $db = Staple::DB::FS->new($stapleDir);
        $db = "can't open filesystem database" unless $db;
    } elsif ($type eq "sql") {
        $params[0] = "$params[0]" if defined $params[0];
        $params[0] = "staple" unless defined $params[0];
        $params[1] = "dbi:Pg:dbname=staple;host=pghost;port=5432;" unless $params[1];
        $db = Staple::DB::SQL->new(@params);
        $db = "can't open sql database" unless $db;
    } else {
        $db = "Unknown database \"$type\"";
    }
    return $db;
}

################################################################################
#   The end
################################################################################

1;

__END__

=back

=head1 SEE ALSO

L<Staple> - Staple main module.

=head1 AUTHOR

Yair Yarom, E<lt>irush@cs.huji.ac.ilE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2007-2010 Hebrew University Of Jerusalem, Israel
See the LICENSE file.

=cut
