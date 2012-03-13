package Staple::DB;

#
# Copyright (C) 2007-2011 Hebrew University Of Jerusalem, Israel
# See the LICENSE file.
#
# Author: Yair Yarom <irush@cs.huji.ac.il>
#

use strict;
use warnings;
use Staple::Misc;
use Staple::Configuration;
use Staple::Group;
use Staple::Host;
use Staple::Distribution;

our $VERSION = '007snap';

=head1 NAME

  Staple::DB - API for database connection

=head1 DESCRIPTION

Staple::DB module, provides an abstract class for staple database 

=head1 FUNCTIONS

=over

=item B<new(I<params>)>

Creates a new instance of this database.

=item B<create(I<params>)>

Like new but builds the database if doesn't exists (directory tree, sql tables, etc.)

=item B<describe( )>

Returns two strings describing the database. First string is a single line base
description, second string is a usage information (i.e. the new/create
parameters).

In case of error, the first string is undef and the second starts with "Error:"
with the error message.

=cut

sub describe {
    return (undef, "Error: not a real database\n");
}

################################################################################
#   Methods
################################################################################

=back

=head1 METHODS

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

=item B<addDistribution(I<distribution>, [version])>

Add distribution to the database, returns 1 on success, or undef on failure. $error is set
to the error.

If version is provided, sets as the initial version. Otherwise, uses the
current version $Staple::VERSION.

=cut

sub addDistribution {
    my $self = shift;
    $self->{error} = "addDistribution not implemented in this database yet";
    return undef;
}

=item B<removeDistribution(I<distribution>)>

Deletes a distribution, returns 1 on success, or undef on failure (and sets the error)

=cut

sub removeDistribution {
    my $self = shift;
    $self->{error} = "removeDistribution not implemented in this database yet";
    return undef;
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

=item B<addTokens(I<tokens hash ref, node>)>

Adds the tokens (tokens hash ref) to the group or configuration in the
database, returns 1 on success, or undef on failure. $error is set to the
error.

=cut

sub addTokens {
    my $self = shift;
    $self->{error} = "addTokens not implemented in this database yet";
    return undef;
}

=item B<removeTokens(I<tokens name list ref, node>)>

Removes the tokens (list ref of strings) from the group or configuration in the
database, returns 1 on success, or undef on failure. $error is set to the
error.

=cut

sub removeTokens {
    my $self = shift;
    $self->{error} = "removeTokens not implemented in this database yet";
    return undef;
}

=item B<setTokens(I<tokens name list ref, node>)>

Sets the tokens (tokens hash ref) of the group or configuration in the
database, returns 1 on success, or undef on failure. error is set to the error.

The default implementation calls removeTokens(getTokens(group)), followed by
addTokens. Databases which support a faster method of replacing all tokens
should reimplement this.

=cut

sub setTokens {
    my $self = shift;
    my $tokens = shift;
    my $group = shift;
    
    my $origTokens = $self->getTokens($group);
    return undef unless defined $origTokens;

    return undef unless $self->removeTokens([keys %$origTokens], $group);

    return $self->addTokens($tokens, $group);
}

=item B<addMount(I<configuration, mount, [location]>)>

Adds the mount (Staple::Mount) to the configuration at location, returns 1 on
success, or undef on failure. $error is set to the error.

=cut

sub addMount {
    my $self = shift;
    $self->{error} = "addMount not implemented in this database yet";
    return undef;
}

=item B<removeMounts(I<mount [mount [...]]>)>

Deletes the list of mounts (Staple::Mount), returns 1 on success, or undef on
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

=item B<hasDistribution(I<distribution>)>

Checks if the database has the given distribution (string). Returns 1 if
distribution exists, 0 if not and undef on error (and sets the error).

=cut

sub hasDistribution {
    my $self = shift;
    my $dist = shift;
    if (not defined $dist) {
        $self->{error} = "Missing parameter";
        return undef;
    }
    my @distributions = $self->getAllDistributions();
    if (@distributions and not defined $distributions[0]) {
        # getAllDistributions already sets the error
        return undef;
    } elsif (grep {$_ eq $dist} @distributions) {
        return 1;
    }
    return 0;
}

=item B<copyConfiguration(I<conf, from, to>)>

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

=item B<getTokens(I<node, [...]>)>

Returns a tokens hash reference (where the key is the token key, and the value
is the token hash). The tokens are taken from the nodes (types can be
intermixed in the input list), by the same order, so if token appears twice it
will be overridden.

The tokens are returned raw from the database/filesystem, they are not
C<initialized>. i.e. they aren't checked for mistakes, no auto and default
tokens are added, and the regexp and dynamic tokens' values are empty (not
evaluated). To C<initialize> them, use the B<getCompleteTokens> function.

If an error occurs, undef is returned, and the error is set.

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

Get templates (Staple::Template) from the database. Recives an ordered list of
full configurations, and returns an (unordered) list of templates (same
templates + stage will be overriden by the last one).

Returns undef on error and sets the error.

=cut

sub getTemplates {
    my $self = shift;
    $self->{error} = "getTemplates not implemented in this database yet";
    return undef;
}

=item B<addTemplates(I<template [template [...]]>)>

Adds the given templates (Staple::Template) to their distributions.

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

Returns an ordered list of scripts (Staple::Script). The script are ordered
first by the configurations (with the given order) and second by the internal
order per configuration.

On error undef is returned.

=cut

sub getScripts {
    my $self = shift;
    $self->{error} = "getScripts not implemented in this database yet";
    return undef;
}


=item B<getAutos(I<configuration [configuration [...]]>)>

Returns an ordered list of autos (Staple::Auto). The autos are ordered first by
the configurations (with the given order) and second by the internal order per
configuration (like I<getScripts>).

On error undef is retuned

=cut

sub getAutos {
    my $self = shift;
    $self->{error} = "getAutos not implemented in this database yet";
    return undef;
}

=item B<addAutos(I<auto [auto [...]]>)>

Adds the given autos (Staple::Autogroup, contains the configurations to add
to). The autos will be inserted in the specified location (order).

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


=item B<removeScripts(I<script [script [...]]>)>

Removes the given scripts (Staple::Script).

Returns 1 or undef;

=cut

sub removeScripts {
    my $self = shift;
    $self->{error} = "removeScripts not implemented in this database yet";
    return undef;
}

=item B<addScripts(I<script [script [...]]>)>

Adds the given scripts (Staple::Script). The scripts will be inserted in the
specified location (order).

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

=item B<removeAutos(I<auto [auto [...]]>)>

Removes the given autos (Staple::Autogroup).

Returns 1 or undef;

=cut

sub removeAutos {
    my $self = shift;
    $self->{error} = "removeAutos not implemented in this database yet";
    return undef;
}

=item B<removeTemplates(I<template [template [...]]>)>

Removes the given templates (Staple::Template).

Returns 1 or undef;

=cut

sub removeTemplates {
    my $self = shift;
    $self->{error} = "removeTemplates not implemented in this database yet";
    return undef;
}

=item B<getGroups(I<group>)>

Gets an orderd group list associated with the given group
(Staple::Group). returns a list of group names (strings), which can be built
using getGroupsByName. On failure returns undef (and sets the error).

=cut

sub getGroups {
    my $self = shift;
    $self->{error} = "getGroups not implemented in this database yet";
    return undef;
}

=item B<getGroupGroups(I<group>)>

Returns an ordered list of raw group (no intermediate, no recursive) associated
with the given group (Staple::Group). In case of error, undef will be returned
and the error will be set.

=cut

sub getGroupGroups {
    my $self = shift;
    my $group = shift;
    my @groups = $self->getGroups($group);
    return undef if (@groups and not defined $groups[0]);
    return $self->getGroupsByName(@groups);
}


=item B<getCompleteGroups(I<group [group [...]]>)>

Given a list of groups (Staple::Group) returns a complete list of groups
(Staple::Group). Groups that have extra group, are computed and placed before
the given group. Groups will be splitted into intermediate groups, and
duplicate groups will be removed.

=cut

sub getCompleteGroups {
    my $self = shift;
    my @rawGroups = @_;
    my @groups = ();
    $self->getCompleteGroups1(\@groups, [], \@rawGroups);
    return @groups;
}


=item B<getGroupConfigurations(I<group>)>

Gets a group, Returns an ordered configuration list associated with the given
group. The returned list is of raw configurations (i.e. no path, no
distribution, and includes inactive configurations). On failure returns undef
(and sets the error).

=cut

sub getGroupConfigurations {
    my $self = shift;
    $self->{error} = "getGroupConfigurations not implemented in this database yet";
    return undef;
}

=item B<getGroupsConfigurations(I<group [group [...]]>)>

Returns an ordered list of configurations (both active and inactive) from the
groups list. The configurations aren't full, i.e. I<path> and I<dist> are
undef. To get complete configurations, as they would appear in the boot
process, pass them through I<getCompleteConfigurations>.  In case of error,
undef will be returned and the error will be set.

=cut

sub getGroupsConfigurations {
    my $self = shift;
    my @groups = @_;
    my @configurations = ();
    foreach my $group (@groups) {
        my @localConfs = $self->getGroupConfigurations($group);
        return undef if (@localConfs == 1 and not defined $localConfs[0]);
        push @configurations, @localConfs;
    }
    return @configurations;
}

=item B<getCompleteConfigurations(I<configurations ref, distribution, [bad list ref]>)>

Receives an ordered list reference of configurations (Staple::Configuration)
and a distribution name (string), and returns a complete ordered list of
configuration. The configurations are full (i.e. includes I<path> and I<dist>),
and include all intermediate configurations and recursive configurations;
inactive configurations are removed.

If the (empty) hash ref of bad list is also supplied, it will be filled with
configurations that doesn't exist under the given distribution. 

WARNING: This method doesn't check for loops in recursive configurations!

=cut

sub getCompleteConfigurations {
    my $self = shift;
    my @remaining = @{$_[0]};
    my $distribution = $_[1];
    my $badConfigurations = $_[2];
    my $version = $self->getDistributionVersion($distribution);

    my @final = ();
    my %final = ();
    # _source, the originating confs (list ref)
    # _done, the stage: undef, super, recursive
    map {$_->{_source} = [$_->name()]} @remaining;
    while (@remaining) {
        my $conf = shift @remaining;
        #use Data::Dumper;
        #print "PROCESSING: ".($conf->{active} ? "+" : "-")."$conf->{name} ".(exists $conf->{_done} ? "(after $conf->{_done}) " : "").Dumper($conf->{_source});

        # remove a configuration
        if (not $conf->active()) {
            my @toremove = $conf->name();
            while (@toremove) {
                my $c = shift @toremove;
                next unless $final{$c};
                #use Data::Dumper;
                #print "DELETE: $c\n";

                @final = grep {$_->name() ne $c} @final;
                delete $final{$c};
                push @toremove, grep m!^$c/!, map {$_->name()} @final;

                # go over all configuration and check for source (might be more efficient to just super and recursive)
                foreach my $c2 (@final) {
                    my @s = @{$c2->{_source}};
                    next unless grep {$_ eq $c} @s;
                    #use Data::Dumper;
                    #print "MIGHT DELETE\n";
                    #print "$c2->{name}: ".Dumper(\@s);
                    @s = grep {$_ ne $c} @s;
                    if (@s) {
                        $c2->{_source} = [@s];
                    } else {
                        push @toremove, $c2->name();
                    }
                }
                  
                # # for now, check sources only for super configurations (instead of recursive as well)
                # foreach my $super (splitData($c)) {
                #     next unless $final{$super};
                #     my @sources = grep {$_ ne $c} @{$final{$super}->{_source}};
                #     if (@sources) {
                #         $final{$super}->{_source} = [@sources];
                #     } else {
                #         push @toremove, $super;
                #     }
                # }
            }
        }

        # next if already done, just add source
        elsif ($final{$conf->name()}) {
            push @{$final{$conf->name()}->{_source}}, @{$conf->{_source}};
        }

        # add superconfs
        elsif (not exists $conf->{_done}) {
            $conf->{_done} = "super";

            # fillIntermediate copies also _source and _done
            # but don't change the _source of $conf
            my @confs = grep {$_->name() ne $conf->name() and $_->name() ne "common"} fillIntermediate($conf);
            map {push @{$_->{_source}}, $conf->name()} @confs;
            unshift @remaining, (@confs, $conf);
        }

        # add recursive confs
        elsif ($conf->{_done} eq "super") {
            (my $aconf) = $self->getFullConfigurations([$conf], $distribution);
            unless ($aconf) {
                push @$badConfigurations, $conf->name() if ($badConfigurations);
                next;
            }
            $aconf->{_source} = $conf->{_source};
            $conf = $aconf;
            $conf->{_done} = "recursive";
            unshift @remaining, $conf;
            if (versionCompare($version, "004") >= 0) {
                my @confs = $self->getConfigurationConfigurations($conf);
                # XXX what to do if fails, but version > 004?
                if (defined $confs[0]) {
                    map {$_->{_source} = [@{$conf->{_source}}]} @confs;
                    #use Data::Dumper;
                    #print "NEW\n";
                    #print map {"$_->{name}: ".Dumper($_->{_source})} @confs;
                    unshift @remaining, @confs;
                }
            }
        }

        # add the damn conf already
        elsif ($conf->{_done} eq "recursive") {
            push @final, $conf;
            $final{$conf->name()} = $conf;
        } else {
            die "something really bad happened, probably a bug\n";
        }
    }
    map {delete $_->{_done}; delete $_->{_source}} @final;
    return @final;
}


=item B<getConfigurationsByName(I<configuration [configuration [...]]>)>

Receives a list of configurations names (strings), and returns a list an
incomplete (missing path, distribution, and group), active configuration hash
refs (order preserved). The configurations can be prefixed with +/- sign
indicating active/inactive configurations

=cut

sub getConfigurationsByName {
    my $self = shift;
    my @configurations = grep {ref $_} Staple::Configuration->new(map {{name => $_}} @_);
    return @configurations;
}

=item B<addConfigurationConfiguration(I<configuration1, configuration2, [location]>)>

Adds I<configuration2> (Staple::Configuration) to I<configuration1>'s
configuration list at I<location> (or at the end if omitted). Returns 1 or
undef.

=cut

sub addConfigurationConfiguration {
    my $self = shift;
    $self->{error} = "addConfigurationConfiguration not implemented in this database yet";
    return undef;
}

=item B<getConfigurationConfigurations(I<configuration>)>

Gets a full Staple::Configuration (with dist), Returns an ordered configuration
list associated with the given configuration. The returned list is of raw
configurations (i.e. no path, no distribution, and includes inactive
configurations). On failure returns undef (and sets the error).

=cut

sub getConfigurationConfigurations {
    my $self = shift;
    $self->{error} = "getConfigurationConfigurations not implemented in this database yet";
    return undef;
}

=item B<removeConfigurationConfigurations(I<conf1, configuration, [configuration [...]]>)>

Removes the configuration list (Staple::Configuration) from the list of
configurations of I<conf1>. Returns 1 or undef.

=cut

sub removeConfigurationConfigurations {
    my $self = shift;
    $self->{error} = "removeConfigurationConfigurations not implemented in this database yet";
    return undef;
}

=item B<addGroupConfiguration(I<group, configuration, location>)>

Adds I<configuration> (Staple::Configuration) to I<group>'s configuration list
at I<location> (or at the end if omitted). Returns 1 or undef.

=cut

sub addGroupConfiguration {
    my $self = shift;
    $self->{error} = "addGroupConfiguration not implemented in this database yet";
    return undef;
}

=item B<removeGroupConfigurations(I<group, configuration, [configuration [...]]>)>

Removes the configuration list (Staple::Configuration) from the list of
configurations in I<group>. Returns 1 or undef.

=cut

sub removeGroupConfigurations {
    my $self = shift;
    $self->{error} = "removeGroupConfigurations not implemented in this database yet";
    return undef;
}

=item B<addGroupGroup(I<group, group name, location>)

The first group is the receiver. The second group (name - string), is the group
to add to the first group. The third, optional, parameter is the location in
the group list, if omitted adds to the end of the list.

Returns 1 on succes or undef on failure (error is set).

=cut

sub addGroupGroup {
    my $self = shift;
    $self->{error} = "addGroupGroup not implemented in this database yet";
    return undef;
}

=item B<removeGroupGroups(I<group, group name, [group name ...]>)

Removes the groups list (strings) from the list of groups in I<group>
(Staple::Group). returns 1 or undef.

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

Receives a list of configuration (Staple::Configuration) without the path or
dist set and a distribution name (string). Returns a list of configurations,
with the dist and path field set according to the distribution given. Order is
preserved, the returned configurations are not the same as the input (i.e. new
references). Non existing configurations are removed.

Input can have configuration names, in which case they are set to active
configurations with no originating group.

Can also accept list of configurations names (strings).

=cut

sub getFullConfigurations {
    my $self = shift;
    my @inputConfigurations = @{$_[0]};
    my $distribution = $_[1];
    my @configurations = ();
    foreach my $conf (@inputConfigurations) {
        my $newConf;
        if (ref $conf) {
            ($newConf) = Staple::Configuration->new($conf);
        } else {
            ($newConf) = Staple::Configuration->new({name => $conf});
            next unless ref $newConf;
        }
        $newConf->dist($distribution);
        my $path = $self->getConfigurationPath($newConf->name(), $distribution);
        next unless defined $path;
        $newConf->path($path);
        push @configurations, $newConf;
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

=item B<getDistributionVersion(I<distribution string>)>

Returns the distribution version on this database, "none" if not available,
undef on error.

=cut

sub getDistributionVersion {
    my $self = shift;
    $self->{error} = "getDistributionVersion not implemented in this database yet";
    return undef;
}

=item B<setDistributionVersion(I<distribution string>, I<version>)>

Sets the distribution version on this database. version can be either version
number or "none"/undef. Returns the old version or undef on error. Not all
databases support moving between all options of versions. Most accept upgrade
though...

=cut

sub setDistributionVersion {
    my $self = shift;
    $self->{error} = "setDistributionVersion not implemented in this database yet";
    return undef;
}

=item B<getMinimumDistributionVersion()>

Gets the minimum version of all distributions.

=cut

sub getMinimumDistributionVersion {
    my $self = shift;
    my @distributions = $self->getAllDistributions();
    return undef if (@distributions and not defined $distributions[0]);
    my $min = $Staple::VERSION;
    for my $dist (@distributions) {
        my $version = $self->getDistributionVersion($dist);
        return undef unless $version;
        $min = $version if versionCompare($version, $min) < 0;
    }
    return $min;
}

=item B<getVersionOf(I<Staple::Node>)

For distribution returns its version. For configuration returns it's
distribution version. For the rest (group, host) returns the minimum
distribution version.

=cut

sub getVersionOf {
    my $self = shift;
    my $node = shift;
    return $self->getDistributionVersion($node->name()) if $node->type() eq "distribution";
    return $self->getDistributionVersion($node->dist()) if $node->type() eq "configuration";
    return $self->getMinimumDistributionVersion();
}

=item B<whoHasGroup(I<group name>)>

Receives a single group name (string), and returns a group (Staple::Group)
list, of the groups that are attached to the given group. The output can be a
group, host, or distribution groups.  On error undef is returned, and the error
is set.

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
    my $key = shift;
    my $distribution = shift;
    
    if (not defined $self->getDistributionGroup($distribution)) {
        # error is already set by $self
        return undef;
    }
    
    # hosts, distributions, groups, configurations
    my @groups;
    foreach my $group ((map {$self->getHostGroup($_)} $self->getAllHosts()),
                       (map {$self->getDistributionGroup($_)} $self->getAllDistributions()),
                       $self->getGroupsByName($self->getAllGroups()),
                       $self->getFullConfigurations([$self->getAllConfigurations($distribution)], $distribution)) {
        my $tokens = $self->getTokens($group);
        push @groups, $group if grep /^${key}$/, keys %$tokens;
    }
    return undef if (grep {not defined $_} @groups);

    
    my @configurations = grep {$_->type() eq "configuration"} @groups;
    @groups = grep {$_->type() ne "configuration"} @groups;

    return ([@groups], [@configurations])
}

=item B<whoHasConfiguration(I<configuration name, [distribution]>)>

Receives a single configuration name (string), and returns a node list, of the
nodes that are attached to the given configuration. The output can be a group,
host, distribution groups or configurations. The output also includes nodes
which contains a removed configurations.

If the second argument is a valide distribution name, then configurations for
that distribution are also checked.

On error undef is returned, and the error is set.

=cut

sub whoHasConfiguration {
    my $self = shift;
    my $configuration = shift;
    my $distribution = shift;
    $distribution = undef if defined $distribution and not $self->getDistributionGroup($distribution);

    my @groups;
    foreach my $group ((map {$self->getHostGroup($_)} $self->getAllHosts()),
                       (map {$self->getDistributionGroup($_)} $self->getAllDistributions()),
                       $self->getGroupsByName($self->getAllGroups())) {
        my @configurations = $self->getGroupConfigurations($group);
        push @groups, $group if grep {$_->name() =~ /^${configuration}/} @configurations;
    }

    # recursive configurations and common configuration only from 004
    if ($distribution and versionCompare($self->getDistributionVersion($distribution), "004") >= 0) {
        foreach my $conf ($self->getFullConfigurations([$self->getAllConfigurations($distribution)], $distribution)) {
            my @configurations = $self->getConfigurationConfigurations($conf);
            push @groups, $conf if grep {$_->name() =~ /^${configuration}/} @configurations;
        }
    }

    return @groups;
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

sub getCommonPath {
    my $self = shift;
    $self->{error} = "getCommonPath not implemented in this database yet";
    return undef;
}

=item B<getGroupPath(I<group string>)

Returns the path of the group. On some databases this is undefined (yet?). You
probably shouldn't use this, as it is used internally by Staple. Use
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

=item B<getDistributionGroup(I<distribution string>)>

Returns the distribution group (Staple::Distribution). If the distribution does
not exists, undef is returned and the error is set

=cut

sub getDistributionGroup {
    my $self = shift;
    my $distribution = shift;
    my $path = $self->getDistributionPath($distribution);
    return (Staple::Distribution->new({name => $distribution, path => $path}))[0] if $path;
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
        push @groups, Staple::Group->new({name => $group, path => $path}) if $path;
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
    return (Staple::Host->new({name => $host, path => $hostPath}))[0] if $hostPath;
    $self->{error} = "Host \"$host\" does not exist";
    return undef;
}



=item B<setNote(node, note)>

Sets the note for the given node. Returns 1 on success and undef on failure
(and sets the error). To delete a note set it to undef.

=cut

sub setNote {
    my $self = shift;
    $self->{error} = "setNote not implemented in this database yet";
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

=item B<getNote(I<node>)>

Returns the node's note. returns undef on error and sets the error. If no note,
returns empty string and error is empty.

=cut

sub getNote {
    my $self = shift;
    $self->{error} = "getNote not implemented in this database yet";
    return undef;
}

=over

=item B<getCompleteTokens(I<tokens ref [host] [distribution]>)>

Receives a tokens hash (as B<DB::getTokens> outputs) and returns a tokens hash,
with simple value checking (e.g. for __STAPLE_*__ tokens), auto and default
tokens (auto tokens that were wrongly inserted, will be removed), and dynamic
and regexp tokens are evaluated (in that order).

The optional I<host> and I<distribution> parameters are for the
__AUTO_HOSTNAME__ and __AUTO_DISTRIBUTION__ tokens. If host is omitted, this
token will not be included in the results

=cut

sub getCompleteTokens {
    my $self = shift;
    my %tokens = %{$_[0]};
    my $host;
    my $distribution;
    $host = $_[1] if $_[1];
    $distribution = $_[2] if $_[2];
    
    my @delete = ();
    foreach my $key (keys %tokens) {
        push @delete, $key if $key =~ m/^__AUTO_/;
    }
    delete @tokens{@delete};
    $tokens{__AUTO_DISTRIBUTION__} = {key => "__AUTO_DISTRIBUTION__", value => $distribution, raw => $distribution, type => "static", source => "auto"} if $distribution;
    $tokens{__AUTO_HOSTNAME__} = {key => "__AUTO_HOSTNAME__", value => $host, raw => $host, type => "static", source => "auto"} if $host;
    $tokens{__AUTO_TMP__} = {key => "__AUTO_TMP__", value => $self->getTmpDir(), raw => $self->getTmpDir(), type => "static", source => "auto"};
    $tokens{__AUTO_STAPLE_LIB__} = {key => "__AUTO_STAPLE_LIB__", value => $INC{"Staple.pm"}, type => "static", source => "auto"};
    $tokens{__AUTO_STAPLE_LIB__}{value} =~ s,/Staple.pm$,,;
    $tokens{__AUTO_STAPLE_LIB__}{raw} = $tokens{__AUTO_STAPLE_LIB__}{value};
    $tokens{__AUTO_DB__} = {key => "__AUTO_DB__", value => $self->info(), raw => $self->info(), type => "static", source => "auto"};
    #$tokens{__AUTO_IP__} = $ip;
    %tokens = setDefaultTokens(\%tokens, \%Staple::defaultTokens);
    %tokens = verifyTokens(\%tokens, \%Staple::allowedTokensValues);
    for (my $i = 0; $i < 2; $i++) {
        %tokens = setDynamicTokens(%tokens);
        %tokens = setRegexpTokens(%tokens);
    }
    #setVariablesFromTokens(\%tokens, \%tokensToVariables);
    return %tokens;
}

=item B<getTmpDir( )>

Returns the temporary directory. If not explicitly set, will search for
existing relevant directories (/boot/staple/tmp, /private/staple/tmp,
/staple/tmp), If none exists, will results in /boot/staple/tmp. On some
databases, this has some initial value (FS).

=cut

sub getTmpDir {
    my $self = shift;
    unless ($self->{tmpDir}) {
        my @tmps = qw(/boot/staple/tmp /private/staple/tmp /staple/tmp);
        foreach my $tmp (@tmps) {
            if (-d $tmp) {
                $self->{tmpDir} = $tmp;
                last;
            }
        }
        $self->{tmpDir} = "/boot/staple/tmp" unless $self->{tmpDir};
    }
    return $self->{tmpDir};
}

=item B<setTmpDir(I<dir>)>

Sets the temporary directory.

=cut

sub setTmpDir {
    my $self = shift;
    $self->{tmpDir} = shift;
}

=item B<syncTo(I<db>, [I<debug>])>

Gets another Staple::DB (should exist, i.e. created with create (or
DBFactory::createDBInit)), and syncs all data from this DB to the given db,
deleting any other data that was in the given db. On failure, the given db will
be in inconsistent state.

If I<debug> is defined, prints various debug messages.

On error returns undef and sets the error.

=cut

sub syncTo {
    my $self = shift;
    my $from = $self;
    my $to = shift;
    my $debug = shift;
    my %fromDists; # name => hash
    my %toDists;   
    my %fromGroups; # name => hash
    my %toGroups;  
    my %fromHosts; # name => hash
    my %toHosts;
    my %fromConfs; # dist/"/common/" => {name => fullconf}
    my %toConfs;

    local $| = 1;
    
    ################################################################################
    print "Syncing distributions tree...\n" if $debug;
    
    my @from = $from->getAllDistributions();
    my @to = $to->getAllDistributions();
    @to = () unless $to[0];
    return undef if (@from and not defined $from[0]);
    
    my %to = ();
    my %from = ();
    @from{@from} = @from if @from;
    %fromDists = map {$_ => $from->getDistributionGroup($_)} keys %from;
    @to{@to} = @to if @to;

    
    for my $distribution (@to) {
        if ($from{$distribution}) {
            # can't downgrade, need to delete. Otherwise, just upgrade version
            if (versionCompare($from->getDistributionVersion($distribution), $to->getDistributionVersion($distribution)) < 0) {
                unless ($to->removeDistribution($distribution)) {
                    $self->{error} = $to->{error};
                    return undef;
                }
            } else {
                unless (defined $to->setDistributionVersion($distribution, $from->getDistributionVersion($distribution))) {
                    $self->{error} = $to->{error};
                    return undef;
                }
                delete $from{$distribution};
            }
        } else {
            unless ($to->removeDistribution($distribution)) {
                $self->{error} = $to->{error};
                return undef;
            }
        }
    }
    
    for my $distribution (keys %from) {
        unless ($to->addDistribution($distribution, $from->getDistributionVersion($distribution))) {
            $self->{error} = $to->{error};
            return undef;
        }
        unless ($to->setDistributionVersion($distribution, $from->getDistributionVersion($distribution))) {
            $self->{error} = $to->{error};
            return undef;
        }
    }
    %toDists = map {$_ => $to->getDistributionGroup($_)} keys %fromDists;

    ################################################################################
    print "Syncing configurations tree (" if $debug;
    
    my $commondist = 0;
    foreach my $distribution (keys %fromDists) {
        print "$distribution.. " if $debug;
        my @from = $from->getAllConfigurations($distribution);
        my @to = $to->getAllConfigurations($distribution);

        @to = () unless $to[0];
        return undef if (@from and not defined $from[0]);

        $fromConfs{$distribution} = {map {$_->name() => $_} $from->getFullConfigurations([grep !/^common\//, @from], $distribution)};
        if (grep /^common\//, @from) {
            $fromConfs{"/common/"} = {map {$_->name() => $_} $from->getFullConfigurations([grep /^common\//, @from], $distribution)};
            $commondist = $distribution;
        }

        # remove common configurations if any, we'll add them later
        @from = grep !/^common\//, @from;

        my %from;
        my %to;
        @from{@from} = @from if @from;
        @to{@to} = @to if @to;

        # remove configurations not in source
        for my $configuration (sort {$b cmp $a} @to) {
            if ($from{$configuration}) {
                delete $from{$configuration};
            } else {
                unless ($to->removeConfiguration($distribution, $configuration)) {
                    $self->{error} = $to->{error};
                    return undef;
                }
            }
        }
        # add configurations not in destination
        for my $configuration (sort {$a cmp $b} keys %from) {
            unless ($to->addConfiguration($distribution, $configuration)) {
                $self->{error} = $to->{error};
                return undef;
            }
        }
    }
    
    foreach my $distribution (grep {$_ ne "/common/"} keys %fromConfs) {
        $toConfs{$distribution} = {map {$_->name() => $_} $to->getFullConfigurations([keys %{$fromConfs{$distribution}}], $distribution)};
    }
    if ($fromConfs{"/common/"}) {
        print "/common/.." if $debug;
        for my $configuration (sort {$a cmp $b} keys %{$fromConfs{"/common/"}}) {
            unless ($to->addConfiguration($commondist, $configuration)) {
                $self->{error} = $to->{error};
                return undef;
            }
        }
        $toConfs{"/common/"} = {map {$_->name() => $_} $to->getFullConfigurations([keys %{$fromConfs{"/common/"}}], $commondist)};
    }
    print ")\n" if $debug;

    ################################################################################
    print "Syncing groups tree...\n" if $debug;

    @from = $from->getAllGroups();
    @to = $to->getAllGroups();
    @to = () unless $to[0];
    return undef if (@from and not defined $from[0]);

    %from = ();
    %to = ();
    @from{@from} = @from if @from;
    @to{@to} = @to if @to;
    %fromGroups = map {$_ => $from->getGroupsByName($_)} keys %from;
    
    for my $group (sort {$b cmp $a} @to) {
        if ($from{$group}) {
            delete $from{$group};
        } else {
            unless ($to->removeGroup($group)) {
                $self->{error} = $to->{error};
                return undef;
            }
        }
    }
    
    for my $group (sort {$a cmp $b} keys %from) {
        unless ($to->addGroup($group)) {
            $self->{error} = $to->{error};
            return undef;
        }
    }

    %toGroups = map {$_ => $to->getGroupsByName($_)} keys %fromGroups;
    
    ################################################################################
    print "Syncing hosts tree...\n" if $debug;

    @from = $from->getAllHosts();
    @to = $to->getAllHosts();
    @to = () unless $to[0];
    return undef if (@from and not defined $from[0]);

    %from = ();
    %to = ();
    @from{@from} = @from;
    @to{@to} = @to;
    %fromHosts = map {$_ => $from->getHostGroup($_)} keys %from;
    
    for my $host (@to) {
        if ($from{$host}) {
            delete $from{$host};
        } else {
            unless ($to->removeHost($host)) {
                $self->{error} = $to->{error};
                return undef;
            }
        }
    }
    
    for my $host (keys %from) {
        unless ($to->addHost($host)) {
            $self->{error} = $to->{error};
            return undef;
        }
    }

    %toHosts = map {$_ => $to->getHostGroup($_)} keys %fromHosts;
    
    ################################################################################
    print "Syncing tokens/notes..." if $debug;
    
    my $sub = sub {
        (my $from, my $to, my $fromGroup, my $toGroup) = @_;
        unless ($to->setTokens($from->getTokens($fromGroup), $toGroup)) {
            $from->{error} = $to->{error};
            return undef;
        }
        
        my $fromNote = $from->getNote($fromGroup);
        return undef unless (defined $fromNote);
        unless ($to->setNote($toGroup, $fromNote)) {
            $from->{error} = $to->{error};
            return undef;
        }
        return 1;
    };

    print " (distributions.." if $debug;
    foreach my $fromDist (keys %fromDists) {
        my $toDist = $toDists{$fromDist};
        $fromDist = $fromDists{$fromDist};
        return undef unless &$sub($from, $to, $fromDist, $toDist);
    }
    
    print ", groups.." if $debug;
    foreach my $fromGroup (keys %fromGroups) {
        my $toGroup = $toGroups{$fromGroup};
        $fromGroup = $fromGroups{$fromGroup};
        return undef unless &$sub($from, $to, $fromGroup, $toGroup);
    }
    
    print ", hosts.." if $debug;
    foreach my $fromHost (keys %fromHosts) {
        my $toHost = $toHosts{$fromHost};
        $fromHost = $fromHosts{$fromHost};
        return undef unless &$sub($from, $to, $fromHost, $toHost);
    }
    
    print ", configurations.." if $debug;
    foreach my $dist (keys %fromConfs) {
        foreach my $fromConf (keys %{$fromConfs{$dist}}) {
            my $toConf = $toConfs{$dist}{$fromConf};
            $fromConf = $fromConfs{$dist}{$fromConf};
            return undef unless &$sub($from, $to, $fromConf, $toConf);
        }
    }

    print ")\n" if $debug;
    

    ################################################################################
    print "Syncing group groups/configurations" if $debug;

    $sub = sub {
        my $from = shift;
        my $to = shift;
        my $fromGroup = shift;
        my $toGroup = shift;

        my @fromGroups = $from->getGroups($fromGroup);
        my @toGroups = $to->getGroups($toGroup);
        unless ($to->removeGroupGroups($toGroup, @toGroups)) {
            $from->{error} = $to->{error};
            return undef;
        }
        foreach my $group (@fromGroups) {
            unless ($to->addGroupGroup($toGroup, $group)) {
                $self->{error} = $to->{error};
                return undef;
            }
        }

        my @fromConfigurations = $from->getGroupConfigurations($fromGroup);
        my @toConfigurations = $to->getGroupConfigurations($toGroup);
        unless ($to->removeGroupConfigurations($toGroup, @toConfigurations)) {
            $from->{error} = $to->{error};
            return undef;
        }
        foreach my $configuration (@fromConfigurations) {
            unless ($to->addGroupConfiguration($toGroup, $configuration)) {
                $from->{error} = $to->{error};
                return undef;
            }
        }
        
        return 1;
    };

    print " (distributions.." if $debug;
    foreach my $fromDist (keys %fromDists) {
        my $toDist = $toDists{$fromDist};
        $fromDist = $fromDists{$fromDist};
        return undef unless &$sub($from, $to, $fromDist, $toDist);
    }
    
    print ", hosts.." if $debug;
    foreach my $fromHost (keys %fromHosts) {
        my $toHost = $toHosts{$fromHost};
        $fromHost = $fromHosts{$fromHost};
        return undef unless &$sub($from, $to, $fromHost, $toHost);
    }
    
    print ", groups.." if $debug;
    foreach my $fromGroup (keys %fromGroups) {
        my $toGroup = $toGroups{$fromGroup};
        $fromGroup = $fromGroups{$fromGroup};
        return undef unless &$sub($from, $to, $fromGroup, $toGroup);
    }

    print ")\n" if $debug;

    ################################################################################
    print "Syncing settings (" if $debug;

    
    foreach my $dist (keys %fromConfs) {
        print "$dist.. " if $debug;
        foreach my $fromConf (keys %{$fromConfs{$dist}}) {
            my $toConf = $toConfs{$dist}{$fromConf};
            $fromConf = $fromConfs{$dist}{$fromConf};

            # mounts
            my @fromMounts = $from->getMounts($fromConf);
            my @toMounts = $to->getMounts($toConf);

            unless ($to->removeMounts(@toMounts)) {
                $from->{error} = $to->{error};
                return undef;
            }

            foreach my $mount (@fromMounts) {
                unless ($to->addMount($toConf, $mount)) {
                    $from->{error} = $to->{error};
                    return undef;
                }
            }

            # templates
            my @fromTemplates = $from->getTemplates($fromConf);
            my @toTemplates = $to->getTemplates($toConf);
            
            unless ($to->removeTemplates(@toTemplates)) {
                $from->{error} = $to->{error};
                return undef;
            }

            map {$_->configuration($toConf)} @fromTemplates;

            unless ($to->addTemplates(@fromTemplates)) {
                $from->{error} = $to->{error};
                return undef;
            }

            # scripts
            my @fromScripts = $from->getScripts($fromConf);
            my @toScripts = $to->getScripts($toConf);

            unless ($to->removeScripts(@toScripts)) {
                $from->{error} = "syncScripts failed(removing): ".$to->{error};
                return undef;
            }

            map {$_->configuration($toConf)} @fromScripts;

            unless ($to->addScripts(@fromScripts)) {
                $from->{error} = "syncScripts failed(adding): ".$to->{error};
                return undef;
            }
            
            # autos
            my @fromAutos = $from->getAutos($fromConf);
            my @toAutos = $to->getAutos($toConf);

            unless ($to->removeAutos(@toAutos)) {
                $from->{error} = $to->{error};
                return undef;
            }

            map {$_->configuration($toConf)} @fromAutos;

            unless ($to->addAutos(@fromAutos)) {
                $from->{error} = $to->{error};
                return undef;
            }

            # configurations

            # on sql (and fsql) the configuration configuration list, when
            # pointing to common configuration, doesn't not work until version
            # 006 but unfortunately, here we only check for version 004 (where
            # configuration configuration list was introduced)
            
            if ($dist eq "/common/" or versionCompare($from->getDistributionVersion($dist), "004") >= 0) {
                my @fromConfigurations = $from->getConfigurationConfigurations($fromConf);
                my @toConfigurations = $to->getConfigurationConfigurations($toConf);
                unless ($to->removeConfigurationConfigurations($toConf, @toConfigurations)) {
                    $from->{error} = $to->{error};
                    return undef;
                }
                foreach my $configuration (@fromConfigurations) {
                    unless ($to->addConfigurationConfiguration($toConf, $configuration)) {
                        $from->{error} = $to->{error};
                        return undef;
                    }
                }
            }
        }
    }

    print ")\n" if $debug;

    return 1;
}

################################################################################
#   Internals
################################################################################

# input: [$self], array hash of current results, array hash of not allowed, array hash or groups to check
sub getCompleteGroups1 {
    my $self = shift;
    my $result = shift;
    my $nogood = shift;
    my $tocheck = shift;
    my @groups = ();
    my %groups = ();
    @groups{map {$_->name()} @$result} = @$result;
    @groups{map {$_->name()} @$nogood} = @$nogood;
    
    my @rawGroups = fillIntermediate(@$tocheck);
    map {$_->path($self->getGroupPath($_->name())) if $_->type() eq "group"} @rawGroups;
    
    foreach my $rawGroup (@rawGroups) {
        push @$nogood, $rawGroup;
        unless ($groups{$rawGroup->name()}) {
            $self->getCompleteGroups1($result, $nogood, [$self->getGroupGroups($rawGroup)]);
            push @$result, $rawGroup;
            @groups{map {$_->name()} @$result} = @$result;
        }
    }

    return;
}

################################################################################
#   Tokens Internals
################################################################################

# =item B<setDefaultTokens(I<tokens ref>, I<default tokens ref>)>
# 
# Receives a tokens hash (as B<DB::getTokens> outputs), and a default tokens hash,
# and returns a tokens hash. The results hash is the original hash (a copy), with
# the defaults if not set. if __STAPLE_CONF__ is set to a readable file (either
# by the tokens, or by the default tokens), than first the file is read and
# applied (not recursivally). The tokens read from file are ignored if not valid.
# 
# =cut


# input: tokens hash ref, default hash ref
# output: token hashes list (with defaults, if undefined)
sub setDefaultTokens {
    my %tokens = %{$_[0]};
    my %defaults = %{$_[1]};
    my $conf = "";
    $conf = $tokens{__STAPLE_CONF__}->{value} if $tokens{__STAPLE_CONF__};
    $conf = $defaults{__STAPLE_CONF__}->{value} if $defaults{__STAPLE_CONF__} and (not $conf or not -r $conf);
    if (-r $conf) {
        my %files = readTokensFile($conf, "static");
        map {exists $_->{source} and $_->{source} = "default:$_->{source}" or $_->{source} = "default"} values %files;
        %files = verifyTokens(\%files, \%Staple::allowedTokensValues);
        foreach my $key (keys %files) {
            $tokens{$key} = $files{$key} unless exists $tokens{$key};
        }
    }

    foreach my $key (keys %defaults) {
        $tokens{$key} = {key => $key, value => $defaults{$key}->{value}, raw => $defaults{$key}->{raw}, type => "static", source => "default"} unless exists $tokens{$key};
    }
    return %tokens;
}


# input: tokens hash ref, allowed hash ref
# output: tokens hash list (without bad tokens)
sub verifyTokens {
    my %tokens = %{$_[0]};
    my %allowed = %{$_[1]};
    foreach my $key (keys %allowed) {
        if ($tokens{$key}) {
            delete $tokens{$key} unless $tokens{$key}->{value} =~ m/^$allowed{$key}$/;
        }
    }
    return %tokens;
}

# input: tokens hash
# output: same hashes with dynamic evaluated
sub setDynamicTokens {
    my %tokens = @_;
    foreach my $tokenName (grep {$tokens{$_}->{type} eq "dynamic"} keys %tokens) {
        my $token = $tokens{$tokenName};
        my %currentTokens = %tokens;
        delete $currentTokens{$tokenName};
        my $value;
        $token->{value} = $token->{raw};
        do {
            $value = $token->{value};
            $token->{value} = applyTokens($value, \%currentTokens);
        } while ($token->{value} ne $value);
    }
    return %tokens;
}

# input: tokens hash
# output: same hash with regexp evaluated
sub setRegexpTokens {
    my %tokens = @_;
    foreach my $token (sort {$a cmp $b} grep {$tokens{$_}->{type} eq "regexp"} keys %tokens) {
        $tokens{$token}->{value} = join $tokens{$token}->{raw}, map {$tokens{$_}->{value}} sort {$a cmp $b} grep {/^$token$/ and $tokens{$_}->{type} ne "regexp"} keys %tokens;
    }
    return %tokens;
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

L<Staple::DB::SQL> - SQL - Database

=head1 AUTHOR

Yair Yarom, E<lt>irush@cs.huji.ac.ilE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2007-2011 Hebrew University Of Jerusalem, Israel
See the LICENSE file.

=cut
