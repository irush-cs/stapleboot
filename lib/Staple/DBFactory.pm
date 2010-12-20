package Staple::DBFactory;

#
# Copyright (C) 2007-2010 Hebrew University Of Jerusalem, Israel
# See the LICENSE file.
#
# Author: Yair Yarom <irush@cs.huji.ac.il>
#

use strict;
use warnings;
use Staple::Misc;
use File::Find;
require Exporter;


=head1 NAME

  Staple::DBFactory - Staple DB factory

=head1 SYNOPSIS

use Staple::DBFactory;

=head1 DESCRIPTION

Factory for Staple::DB

=head1 EXPORT

=over

=item createDB([db type, db parameters])

=item createDBinit([db type, db parameters])

=item listDB( )

=back

=cut

our @ISA = qw(Exporter);
our @EXPORT_OK = qw();
our @EXPORT = qw(
                    createDB
                    createDBinit
                    listDB
               );
our $VERSION = '006snap';


################################################################################
#   Exported
################################################################################


=head1 DESCRIPTION

=over

=item B<createDB(I<[db type, db parameters]>)>

Returns a new Staple::DB. If no parameters are given, use the __STAPLE_DB__
token from the conf file: /etc/staple/staple.conf. If doesn't exists, defaults
to fs /boot/staple.

On failure returns Staple::DB::Error, with $db->{error} set appropriately.

The first parameter is the db type, e.g. "fs" (Staple::DB::FS) or "sql"
(Staple::DB::SQL). The rest of the parameters are db type dependant.

=cut

sub createDB {
    my $type = shift;
    my @params = @_;
    my %dbs = listDB();
    unless ($type) {
        my %tokens = readTokensFile($Staple::defaultTokens{__STAPLE_CONF__}->{value}, "static");
        ($type, @params) = split /\s+/, $tokens{__STAPLE_DB__}->{value} if ($tokens{__STAPLE_DB__});
    }
    $type = "fs" unless $type;
    my $db;
    if ($type eq "error") {
        $db = $dbs{$type}->{new}($params[0] || "Error database with no error");
    } else {
        if ($dbs{$type}) {
            $db = $dbs{$type}->{new}(@params);
        } else {
            $db = $dbs{error}->{new}("Unknown database type \"$type\"");
        }
    }
    return $db;
}

=item B<createDBinit(I<[db type, db parameters]>)>

The same as createDB, but also initializes the database if it's empty

=cut

sub createDBinit {
    # for now, just copy/paste code from createDB...
    my $type = shift;
    my @params = @_;
    my %dbs = listDB();
    unless ($type) {
        my %tokens = readTokensFile($Staple::defaultTokens{__STAPLE_CONF__}->{value}, "static");
        ($type, @params) = split /\s+/, $tokens{__STAPLE_DB__}->{value} if ($tokens{__STAPLE_DB__});
    }
    $type = "fs" unless $type;
    my $db;
    if ($dbs{$type}) {
        $db = $dbs{$type}->{create}(@params);
    } else {
        $db = $dbs{error}->{new}("Unknown database type \"$type\"");
    }
    return $db;    
}

=item B<listDB( )>

Returns a hash of available databases. Each value is a hash contains:
desc   - description string
long   - usage string
new    - new function
create - create function

=cut

sub listDB {
    my %results;
    my @dbs;
    find({wanted => sub {$_ =~ m/\.pm$/ and $File::Find::name =~ s,.*/Staple/DB/,, and push @dbs, $File::Find::name;}}, grep {-d "$_"} map {"$_/Staple/DB/"} @INC);
    foreach my $db (@dbs) {
        (my $name) = $db =~ m/(.*)\.pm$/;
        eval {require "Staple/DB/$db"};
        next if ($@);
        my $desc;
        my $long;
        eval "(\$desc,\$long) = Staple::DB::${name}::describe()";
        next if ($@);
        my %hash = (desc => $desc, long => $long);
        eval "\$hash{new} = sub {return Staple::DB::${name}->new(\@_)};
              \$hash{create} = sub {return Staple::DB::${name}->create(\@_)};";
        $results{lc($name)} = \%hash;
    }
    return %results;
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
