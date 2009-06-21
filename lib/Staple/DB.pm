package Staple::DB;

#
# Copyright (C) 2007-2009 Hebrew University Of Jerusalem, Israel
# See the LICENSE file.
#
# Author: Yair Yarom <irush@cs.huji.ac.il>
#

use strict;
use warnings;
our $VERSION = '003';

=head1 NAME

  Staple::DB - API for database connection

=head1 DESCRIPTION

Staple::DB module, provides an abstract class for staple database 

=head1 METHODS

=over

=item addHost(host)

=item removeHost(host)

=item getTokens(group|configuration, [...])

=back

=cut

################################################################################
#   Methods
################################################################################

=head1 DESCRIPTION

=over

=item B<info>

Returns a string with the information about the database

=cut

sub info {
    my $self = shift;
    die "info not implemented in this database yet";
}

=item B<addHost(I<host>)>

Add host to the database, returns 1 on success, or 0 on failure. $error is set
to the error.

=cut

sub addHost {
    my $self = shift;
    die "add Host not implemented in this database yet";
}

=item B<removeHost(I<host>)>

Deletes a host, returns 1 on success, and 0 on failure (and sets the error)

=cut

sub removeHost {
    my $self = shift;
    die "removeHost not implemented in this database yet";
}


=item B<addGroup(I<group>)>

Add group to the database, returns the number of groups created, or undef on failure. $error is set
to the error.

=cut

sub addGroup {
    my $self = shift;
    die "addGroup not implemented in this database yet";
}


=item B<removeGroup(I<group>)>

Deletes a group, returns 1 on success, or undef on failure (and sets the error)

=cut

sub removeGroup {
    my $self = shift;
    die "removeGroup not implemented in this database yet";
}

=item B<addDistribution(I<distribution>)>

Add distribution to the database, returns 1 on success, or undef on failure. $error is set
to the error.

=cut

sub addDistribution {
    my $self = shift;
    die "addDistribution not implemented in this database yet";
}


=item B<removeDistribution(I<distribution>)>

Deletes a distribution, returns 1 on success, or undef on failure (and sets the error)

=cut

sub removeDistribution {
    my $self = shift;
    die "removeDistribution not implemented in this database yet";
}

=item B<addConfiguration(I<distribution, configuration>)>

Add configuration to the database, returns 1 on success, or undef on failure. $error is set
to the error.

=cut

sub addConfiguration {
    my $self = shift;
    $self->{error} = "addConfiguration not implemented in this database yet";
    return undef;
}

=item B<addTokens(I<tokens hash ref, group|configuration>)>

Adds the tokens (tokens hash ref) to the group or configuration in the
database, returns 1 on success, or undef on failure. $error is set to the
error.

=cut

sub addTokens {
    my $self = shift;
    $self->{error} = "addTokens not implemented in this database yet";
    return undef;
}

=item B<removeTokens(I<tokens name list ref, group|configuration>)>

Removes the tokens (list ref of strings) from the group or configuration in the
database, returns 1 on success, or undef on failure. $error is set to the
error.

=cut

sub removeTokens {
    my $self = shift;
    $self->{error} = "removeTokens not implemented in this database yet";
    return undef;
}

=item B<addMount(I<configuration, mount, [location]>)>

Adds the mount (hash ref) to the configuration at location, returns 1 on
success, or undef on failure. $error is set to the error.

=cut

sub addMount {
    my $self = shift;
    $self->{error} = "addMount not implemented in this database yet";
    return undef;
}

=item B<removeMounts(I<mount [mount [...]]>)>

Deletes the list of mounts (list of hashes), returns 1 on success, or undef on
failure (and sets the error).

=cut

sub removeMounts {
    my $self = shift;
    $self->{error} =  "removeMounts is not implemented in this database yet";
    return undef;
}

=item B<removeConfiguration(I<distribution, configuration>)>

Deletes a configuration, returns 1 on success, or undef on failure (and sets the error)

=cut

sub removeConfiguration {
    my $self = shift;
    $self->{error} =  "removeConfiguration not implemented in this database yet";
    return undef;
}

=item B<copyConfiguration(I<conf from to>)>

Copies the configuration I<conf> (a string) from distribution I<from> to
distribution I<to>, returns 1 on success, or undef on failure (error is set).

Copies configuration and everything below it. i.e. coping '/' will copy the
entire configuration tree. If the configuration above doesn't exists, an ampty
one will be created. If a subconfiguration exists in the destination but not in
the source, it will be deleted.

=cut

sub copyConfiguration {
    my $self = shift;
    $self->{error} = "copyConfiguration not implemented in this database yet";
    return undef;
}

=item B<getTokens(I<group|configuration, [...]>)>

Get raw tokens from the database, returns the raw tokens hash reference. On
failure returns undef (and sets the error)

=cut

sub getTokens {
    my $self = shift;
    $self->{error} = "getTokens not implemented in this database yet";
    return undef;
}

=item B<getMounts(I<configuration, [configuration [...]]>)>

Get raw mounts from the database. Recives an ordered list of full
configurations, and returns an ordered list of raw mount hashes (might be
inactive and includes only: destination, configuraitons, and active).

Returns undef on error and sets the error.

=cut

sub getMounts {
    my $self = shift;
    $self->{error} = "getMounts not implemented in this database yet";
    return undef;
}


=item B<getTemplates(I<configuration, [configuration [...]]>)>

Get templates hashes from the database. Recives an ordered list of full
configurations, and returns an (unordered) list of templates (same templates +
stage will be overriden by the last one).

Returns undef on error and sets the error.

=cut

sub getTemplates {
    my $self = shift;
    $self->{error} = "getTemplates not implemented in this database yet";
    return undef;
}

=item B<addTemplates(I<template [template [...]]>)>

Adds the given templates (hashes) to their distributions.

If I<source> is available, it will be taken as the source for the
template. otherwise I<data> will be taken. The source will be copied, so it can
be deleted after it was added.

Returns 1 or undef.

=cut

sub addTemplates {
    my $self = shift;
    $self->{error} = "addTemplates not implemented in this database yet";
    return undef;
}

=item B<getScripts(I<configuration [configuration [...]]>)>

Returns an ordered list of scripts hashes. The script are ordered first by the
configurations (with the given order) and second by the internal order per
configuration.

On error undef is returned.

=cut

sub getScripts {
    my $self = shift;
    $self->{error} = "getScripts not implemented in this database yet";
    return undef;
}


=item B<getAutos(I<configuration [configuration [...]]>)>

Returns an ordered list of autos hashes. The autos are ordered first by the
configurations (with the given order) and second by the internal order per
configuration (like I<getScripts>).

On error undef is retuned

=cut

sub getAutos {
    my $self = shift;
    $self->{error} = "getAutos not implemented in this database yet";
    return undef;
}

=item B<addAutos(I<auto [auto [...]]>)>

Adds the given autos (hashes, contains the configurations to add to). The
autos will be inserted in the specified location (order).

If I<source> is available, it will be taken as the source for the
auto. otherwise I<data> will be taken. The source will be copied, so it can
be deleted after it was added.

Returns 1 or undef.

=cut

sub addAutos {
    my $self = shift;
    $self->{error} = "addAutos not implemented in this database yet";
    return undef;
}


=item B<removeAutos(I<auto [auto [...]]>)>

Removes the given autos (full auto hashes).

Returns 1 or undef;

=cut

sub removeScripts {
    my $self = shift;
    $self->{error} = "removeAutos not implemented in this database yet";
    return undef;
}

=item B<addScripts(I<script [script [...]]>)>

Adds the given scripts (hashes, contains the configurations to add to). The
scripts will be inserted in the specified location (order).

If I<source> is available, it will be taken as the source for the
script. otherwise I<data> will be taken. The source will be copied, so it can
be deleted after it was added.

Returns 1 or undef.

=cut

sub addScripts {
    my $self = shift;
    $self->{error} = "addScripts not implemented in this database yet";
    return undef;
}

=item B<removeScripts(I<script [script [...]]>)>

Removes the given scripts (full script hashes).

Returns 1 or undef;

=cut

sub removeAutos {
    my $self = shift;
    $self->{error} = "removeAutos not implemented in this database yet";
    return undef;
}

=item B<removeTemplates(I<template [template [...]]>)>

Removes the given templates (hashes with configuration hash (name +
distribution), destination and stage).

Returns 1 or undef;

=cut

sub removeTemplates {
    my $self = shift;
    $self->{error} = "removeTemplates not implemented in this database yet";
    return undef;
}

=item B<getGroups(I<group>)>

Gets an orderd group list associated with the given group. returns a list of
group names (strings), which can be built using getGroupsByName. On failure
returns undef (and sets the error).

=cut

sub getGroups {
    my $self = shift;
    $self->{error} = "getGroups not implemented in this database yet";
    return undef;
}

=item B<getGroupConfigurations(I<group>)>

Gets a group hash, Returns an ordered configuration list associated with the
given group. The returned list is of raw configurations (i.e. no path, no
distribution, and includes inactive configurations). On failure returns undef
(and sets the error).

=cut

sub getGroupConfigurations {
    my $self = shift;
    $self->{error} = "getGroupConfigurations not implemented in this database yet";
    return undef;
}

=item B<addGroupConfiguration(I<group, configuration, location>)>

Adds I<configuration> (name, and active should be set) to I<group>'s
configuration list at I<location> (or at the end if omitted). Returns 1 or
undef.

=cut

sub addGroupConfiguration {
    my $self = shift;
    $self->{error} = "addGroupConfiguration not implemented in this database yet";
    return undef;
}

=item B<removeGroupConfigurations(I<group, configuration, [configuration [...]]>)>

Removes the configuration list (configuration hashes) from the list of
configurations in I<group>. Returns 1 or undef.

=cut

sub removeGroupConfigurations {
    my $self = shift;
    $self->{error} = "removeGroupConfigurations not implemented in this database yet";
    return undef;
}

=item B<addGroupGroup(I<group, group name, location>)

Adds I<group name> to the list of groups in I<group> at I<location>. returns 1
or undef.

=cut

sub addGroupGroup {
    my $self = shift;
    $self->{error} = "addGroupGroup not implemented in this database yet";
    return undef;
}

=item B<removeGroupGroups(I<group, group name, [group name ...]>)

Removes the groups list (strings) from the list of groups in I<group>. returns
1 or undef.

=cut

sub removeGroupGroups {
    my $self = shift;
    $self->{error} = "removeGroupGroups not implemented in this database yet";
    return undef;
}

=item B<getAllHosts()>

Get a list of all hosts in the database, sorted alphabetically.

=cut

sub getAllHosts {
    my $self = shift;
    die "getAllHosts not implemented in this database yet";
}

=item B<getAllGroups()>

Get a list of all groups (list of strings), sorted alphabetically.

=cut

sub getAllGroups {
    my $self = shift;
    die "getAllGroups not implemented in this database yet";
}

=item B<getAllConfigurations(I<distribution>)>

Get a list of all configurations (list of strings) for the given distribution, sorted.

=cut

sub getAllConfigurations {
    my $self = shift;
    $self->{error} = "getAllConfigurations not implemented in this database yet";
    return undef;
}

=item B<getFullConfigurations(I<configuration list ref>, distribution name>)>

Receives a list of configuration (hashes) without the path or dist set and a
distribution name (string).Returns a list of configurations, with the dist and
path field set according to the distribution given. Order is preserved, the
returned configurations are not the same as the input (i.e. new
references). Non existing configurations are removed.

Can also accept list of configurations names (strings).

=cut

sub getFullConfigurations {
    my $self = shift;
    my @inputConfigurations = @{$_[0]};
    my $distribution = $_[1];
    my @configurations = ();
    foreach my $conf (@inputConfigurations) {
        my %newConf = ();
        if (ref $conf) {
            %newConf = %$conf;
        } else {
            %newConf = (name => $conf, active => 1, group => undef, path => undef, dist => undef);
        }
        $newConf{dist} = $distribution;
        $newConf{path} = $self->getConfigurationPath($newConf{name}, $distribution);
        push @configurations, \%newConf if defined $newConf{path};
    }
    return @configurations;
}

=item B<getAllDistributions(I<distribution>)>

Get a list of all distributions (list of strings), sorted.

=cut

sub getAllDistributions {
    my $self = shift;
    $self->{error} = "getAllDistributions not implemented in this database yet";
    return undef;
}


=item B<whoHasGroup(I<group name>)>

Receives a single group name (string), and returns a group (hash) list, of the
groups that are attached to the given group. The output can be a group, host,
or distribution groups.
On error undef is returned, and the error is set.

=cut

sub whoHasGroup {
    my $self = shift;
    $self->{error} = "whoHasGroup not implemented in this database yet";
    return undef;
}

=item B<whoHasToken(I<token key, distribution>)>

Receives a single token key (string) and a distribution name, and returns a two
list refs, first of groups (can be hosts, distributions or groups), and the
second is configurations (for the given distribution). Both lists, contains
groups/configurations with values for the given token.

On error undef is returned, and the error is set.

=cut

sub whoHasToken {
    my $self = shift;
    $self->{error} = "whoHasToken not implemented in this database yet";
    return undef;
}

=item B<whoHasConfiguration(I<configuration name>)>

Receives a single configuration name (string), and returns a group (hash) list,
of the groups that are attached to the given configuration. The output can be a
group, host, or distribution groups. The output also includes group which
contains a removed configurations.

On error undef is returned, and the error is set.

=cut

sub whoHasConfiguration {
    my $self = shift;
    $self->{error} = "whoHasConfiguration not implemented in this database yet";
    return undef;
}

#=item B<getConfigurationPath(I<conf string, distribution>)
#
#Returns the path of the configuration. On some databases this is undefined
#(yet?). You probably shouldn't use this, as it is used internally by
#I<Staple.pm>. Use getCompleteConfigurations with getConfigurationsByName
#instead.
#
#Returns undef if configuration doesn't exists
#
#=cut

sub getConfigurationPath {
    my $self = shift;
    $self->{error} = "getConfigurationPath not implemented in this database yet";
    return undef;
}

=item B<getGroupPath(I<group string>)

Returns the path of the group. On some databases this is undefined (yet?). You
probably shouldn't use this, as it is used internally by I<Staple.pm>. Use
getGroupsByName instead.

Returns undef if group doesn't exists

=cut

sub getGroupPath {
    my $self = shift;
    $self->{error} = "getGroupPath not implemented in this database yet";
    return undef;
}

#=item B<getDistributionPath(I<distribution string>)
#
#Returns the path of the distribution. On some databases this is undefined
#(yet?). You probably shouldn't use this, as it is used internally. Use
#getDistributionGroup instead.
#
#Returns undef if distribution doesn't exists
#
#=cut

sub getDistributionPath {
    my $self = shift;
    $self->{error} = "getDistributionPath not implemented in this database yet";
    return undef;
}

=item B<getDistributionGroup(I<distribution string>)

Returns the distribution group hash ref. If the distribution does not exists,
undef is returned and the error is set

=cut

sub getDistributionGroup {
    my $self = shift;
    my $distribution = shift;
    my $path = $self->getDistributionPath($distribution);
    return {name => $distribution, path => $path, type => "distribution"} if $path;
    $self->{error} = "Distribution \"$distribution\" does not exist";
    return undef;
}

=item B<getGroupsByName(I<group name [group name [...]]>)>

Receives a list of groups names (strings), and returns a list of groups (order
preserved). Only existing groups are returned.

=cut

sub getGroupsByName {
    my $self = shift;
    my @groups = ();
    while (my $group = shift) {
        my $path = $self->getGroupPath($group);
        push @groups, {name => $group, path => $path, type => "group"} if $path;
    }
    return @groups;
}

=item B<getHostGroup(I<host>)>

Returns the group of the host. Returns undef if host doesn't exist and sets the
error.

=cut

sub getHostGroup {
    my $self = shift;
    my $host = shift;
    my $hostPath = $self->getHostPath($host);
    return {name => $host, path => $hostPath, type => "host"} if $hostPath;
    $self->{error} = "Host \"$host\" does not exist";
    return undef;
}

#=item B<getHostPath(I<host string>)
#
#Returns the path of the host. On some databases this is undefined (yet?). You
#probably shouldn't use this, as it is used internally by I<Staple.pm>. Use
#I<getHostGroup> instead.
#
#Returns undef if host doesn't exists
#
#=cut

sub getHostPath {
    my $self = shift;
    $self->{error} = "getHostPath not implemented in this database yet";
    return undef;
}

  
################################################################################
#   The end
################################################################################

1;

__END__

=back

=head1 SEE ALSO

L<Staple> - Staple main module.

L<Staple::DB::FS> - Filesyste database

L<Staple::DB::DB> - SQL - Database

=head1 AUTHOR

Yair Yarom, E<lt>irush@cs.huji.ac.ilE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2007-2009 Hebrew University Of Jerusalem, Israel
See the LICENSE file.

=cut
