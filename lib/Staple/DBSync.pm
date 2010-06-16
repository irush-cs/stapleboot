package Staple::DBSync;

#
# Copyright (C) 2007-2010 Hebrew University Of Jerusalem, Israel
# See the LICENSE file.
#
# Author: Yair Yarom <irush@cs.huji.ac.il>
#

use strict;
use warnings;
require Exporter;
use Staple;
use Staple::DB::FS;
use Staple::DB::SQL;
use Staple::Misc;
use Staple::DBFactory;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw();
our @EXPORT = qw(
                    sync
                    syncError
               );
our $VERSION = '005';

my $error;

=head1 NAME

  Staple::DBSync - module for syncing between different databases

=head1 EXPORTS

=over

=item sync(from, from params, to, to params)

I<from> and I<to> are the databases to use (currently C<sql> or C<fs>). the
params are a hash list of parameters to pass to the constructor of those
databases. Returns 1 on success or 0 on failure.

=item syncError( )

=back

=cut

################################################################################
#   Exports
################################################################################

sub syncError {
    return $error;
}

sub sync {
    $error = "";
    
    my $db1 = createDB(shift, @{+shift});
    return 0 if $error = $db1->{error};
    my $db2 = createDB(shift, @{+shift});
    return 0 if $error = $db2->{error};
    
    return 0 unless $db1;
    return 0 unless $db2;

    return 0 unless syncHosts($db1, $db2);
    return 0 unless syncGroups($db1, $db2);
    return 0 unless syncDistributions($db1, $db2);
    return 0 unless syncConfigurations($db1, $db2);

    return 0 unless syncTokens($db1, $db2);
    return 0 unless syncGroupGroups($db1, $db2);
    return 0 unless syncGroupConfigurations($db1, $db2);
    return 0 unless syncMounts($db1, $db2);
    return 0 unless syncTemplates($db1, $db2);
    return 0 unless syncScripts($db1, $db2);
    return 0 unless syncAutos($db1, $db2);
    return 0 unless syncNotes($db1, $db2);
    
    return 1;
}

################################################################################
#   Internals
################################################################################


sub syncAutos {
    (my $from, my $to) = @_;
    my @fromDistributions = $from->getAllDistributions();
    my @toDistributions = $to->getAllDistributions();

    my @fromConfigurations = ();
    my @toConfigurations = ();
    
    foreach my $distribution (@fromDistributions) {
        push @fromConfigurations, $from->getFullConfigurations([$from->getAllConfigurations($distribution)], $distribution);
        push @toConfigurations, $to->getFullConfigurations([$to->getAllConfigurations($distribution)], $distribution);
    }

    my @fromAutos = ();
    my @toAutos = ();
    
    foreach my $configuration (@fromConfigurations) {
        push @fromAutos, $from->getAutos($configuration);
        push @toAutos, $to->getAutos(shift @toConfigurations);
    }

    unless ($to->removeAutos(@toAutos)) {
        $error = $to->{error};
        return 0;
    }

    map {($_->{configuration}) = $to->getFullConfigurations([$_->{configuration}->{name}], $_->{configuration}->{dist})} @fromAutos;

    unless ($to->addAutos(@fromAutos)) {
        $error = $to->{error};
        return 0;
    }

    return 1;
}

sub syncScripts {
    (my $from, my $to) = @_;
    my @fromDistributions = $from->getAllDistributions();
    my @toDistributions = $to->getAllDistributions();

    my @fromConfigurations = ();
    my @toConfigurations = ();
    
    foreach my $distribution (@fromDistributions) {
        push @fromConfigurations, $from->getFullConfigurations([$from->getAllConfigurations($distribution)], $distribution);
        push @toConfigurations, $to->getFullConfigurations([$to->getAllConfigurations($distribution)], $distribution);
    }

    my @fromScripts = ();
    my @toScripts = ();
    
    foreach my $configuration (@fromConfigurations) {
        push @fromScripts, $from->getScripts($configuration);
        push @toScripts, $to->getScripts(shift @toConfigurations);
    }

    unless ($to->removeScripts(@toScripts)) {
        $error = "syncScripts failed(removing): ".$to->{error};
        return 0;
    }

    map {($_->{configuration}) = $to->getFullConfigurations([$_->{configuration}->{name}], $_->{configuration}->{dist})} @fromScripts;

    unless ($to->addScripts(@fromScripts)) {
        $error = "syncScripts failed(adding): ".$to->{error};
        return 0;
    }

    return 1;
}

sub syncTemplates {
    (my $from, my $to) = @_;
    my @fromDistributions = $from->getAllDistributions();
    my @toDistributions = $to->getAllDistributions();

    my @fromConfigurations = ();
    my @toConfigurations = ();
    
    foreach my $distribution (@fromDistributions) {
        push @fromConfigurations, $from->getFullConfigurations([$from->getAllConfigurations($distribution)], $distribution);
        push @toConfigurations, $to->getFullConfigurations([$to->getAllConfigurations($distribution)], $distribution);
    }

    my @fromTemplates = ();
    my @toTemplates = ();
    
    foreach my $configuration (@fromConfigurations) {
        push @fromTemplates, $from->getTemplates($configuration);
        push @toTemplates, $to->getTemplates(shift @toConfigurations);
    }

    unless ($to->removeTemplates(@toTemplates)) {
        $error = $to->{error};
        return 0;
    }

    map {($_->{configuration}) = $to->getFullConfigurations([$_->{configuration}->{name}], $_->{configuration}->{dist})} @fromTemplates;
    
    unless ($to->addTemplates(@fromTemplates)) {
        $error = $to->{error};
        return 0;
    }

    return 1;
}

sub syncMounts {
    (my $from, my $to) = @_;
    my @fromDistributions = $from->getAllDistributions();
    my @toDistributions = $to->getAllDistributions();
    
    my @fromConfigurations = ();
    my @toConfigurations = ();
    
    foreach my $distribution (@fromDistributions) {
        push @fromConfigurations, $from->getFullConfigurations([$from->getAllConfigurations($distribution)], $distribution);
        push @toConfigurations, $to->getFullConfigurations([$to->getAllConfigurations($distribution)], $distribution);
    }
    
    foreach my $fromConfiguration (@fromConfigurations) {
        my $toConfiguration = shift @toConfigurations;
        my @fromMounts = $from->getMounts($fromConfiguration);
        my @toMounts = $to->getMounts($toConfiguration);

        unless ($to->removeMounts(@toMounts)) {
            $error = $to->{error};
            return 0;
        }

        foreach my $mount (@fromMounts) {
            unless ($to->addMount($toConfiguration, $mount)) {
                $error = $to->{error};
                return 0;
            }
        }
    }
    return 1;
}


sub syncGroupConfigurations {
    (my $from, my $to) = @_;

    my @fromGroups = map {$from->getDistributionGroup($_)} $from->getAllDistributions();
    my @toGroups = map {$to->getDistributionGroup($_)} $to->getAllDistributions();
    
    push @fromGroups, map {$from->getHostGroup($_)} $from->getAllHosts();
    push @toGroups, map {$to->getHostGroup($_)} $to->getAllHosts();

    push @fromGroups, map {$from->getGroupsByName($_)} $from->getAllGroups();
    push @toGroups, map {$to->getGroupsByName($_)} $to->getAllGroups();

    foreach my $currentFrom (@fromGroups) {
        my $currentTo = shift @toGroups;
        my @currentFromConfigurations = $from->getGroupConfigurations($currentFrom);
        my @currentToConfigurations = $to->getGroupConfigurations($currentTo);
        unless ($to->removeGroupConfigurations($currentTo, @currentToConfigurations)) {
            $error = $to->{error};
            return 0;
        }
        foreach my $configuration (@currentFromConfigurations) {
            unless ($to->addGroupConfiguration($currentTo, $configuration)) {
                $error = $to->{error};
                return 0;
            }
        }
    }
    return 1;
}

sub syncGroupGroups {
    (my $from, my $to) = @_;

    my @fromGroups = map {$from->getDistributionGroup($_)} $from->getAllDistributions();
    my @toGroups = map {$to->getDistributionGroup($_)} $to->getAllDistributions();
    
    push @fromGroups, map {$from->getHostGroup($_)} $from->getAllHosts();
    push @toGroups, map {$to->getHostGroup($_)} $to->getAllHosts();

    push @fromGroups, map {$from->getGroupsByName($_)} $from->getAllGroups();
    push @toGroups, map {$to->getGroupsByName($_)} $to->getAllGroups();

    foreach my $currentFrom (@fromGroups) {
        my $currentTo = shift @toGroups;
        my @currentFromGroups = $from->getGroups($currentFrom);
        my @currentToGroups = $to->getGroups($currentTo);
        unless ($to->removeGroupGroups($currentTo, @currentToGroups)) {
            $error = $to->{error};
            return 0;
        }
        foreach my $group (@currentFromGroups) {
            unless ($to->addGroupGroup($currentTo, $group)) {
                $error = $to->{error};
                return 0;
            }
        }
    }
    return 1;
}

sub syncTokens {
    (my $from, my $to) = @_;
    my @fromDistributions = $from->getAllDistributions();
    my @toDistributions = $to->getAllDistributions();
    
    my @fromGroups = map {$from->getDistributionGroup($_)} @fromDistributions;
    my @toGroups = map {$to->getDistributionGroup($_)} @toDistributions;
    
    push @fromGroups, map {$from->getHostGroup($_)} $from->getAllHosts();
    push @toGroups, map {$to->getHostGroup($_)} $to->getAllHosts();

    push @fromGroups, map {$from->getGroupsByName($_)} $from->getAllGroups();
    push @toGroups, map {$to->getGroupsByName($_)} $to->getAllGroups();

    foreach my $distribution (@fromDistributions) {
        push @fromGroups, $from->getFullConfigurations([$from->getAllConfigurations($distribution)], $distribution);
        push @toGroups, $to->getFullConfigurations([$to->getAllConfigurations($distribution)], $distribution);
    }
    
    foreach my $fromGroup (@fromGroups) {
        my $toGroup = shift @toGroups;
        my $fromTokens = $from->getTokens($fromGroup);
        my $toTokens = $to->getTokens($toGroup);
        unless ($to->removeTokens([keys %$toTokens], $toGroup)) {
            $error = $to->{error};
            return 0;
        }
        unless ($to->addTokens($fromTokens, $toGroup)) {
            $error = $to->{error};
            return 0;
        }
    }
    return 1;
}

sub syncNotes {
    (my $from, my $to) = @_;
    my @fromDistributions = $from->getAllDistributions();
    my @toDistributions = $to->getAllDistributions();
    
    my @fromGroups = map {$from->getDistributionGroup($_)} @fromDistributions;
    my @toGroups = map {$to->getDistributionGroup($_)} @toDistributions;
    
    push @fromGroups, map {$from->getHostGroup($_)} $from->getAllHosts();
    push @toGroups, map {$to->getHostGroup($_)} $to->getAllHosts();
    
    push @fromGroups, map {$from->getGroupsByName($_)} $from->getAllGroups();
    push @toGroups, map {$to->getGroupsByName($_)} $to->getAllGroups();
    
    foreach my $distribution (@fromDistributions) {
        push @fromGroups, $from->getFullConfigurations([$from->getAllConfigurations($distribution)], $distribution);
        push @toGroups, $to->getFullConfigurations([$to->getAllConfigurations($distribution)], $distribution);
    }
    
    foreach my $fromGroup (@fromGroups) {
        my $toGroup = shift @toGroups;
        my $fromNote = $from->getNote($fromGroup);
        unless (defined $fromNote) {
            $error = $from->{error};
            return 0;
        }
        unless ($to->setNote($toGroup, $fromNote)) {
            $error = $to->{error};
            return 0;
        }
    }
    return 1;
}

sub syncHosts {
    (my $from, my $to) = @_;
    my @from = $from->getAllHosts();
    my @to = $to->getAllHosts();
    @to = () unless $to[0];
    if (@from and not defined $from[0]) {
        $error = $from->{error};
        return undef;
    }
    my %from = ();
    my %to = ();
    @from{@from} = @from if @from;
    @to{@to} = @to if @to;
    
    for my $host (@to) {
        if ($from{$host}) {
            delete $from{$host};
        } else {
            unless ($to->removeHost($host)) {
                $error = $to->{error};
                return 0;
            }
        }
    }
    
    for my $host (keys %from) {
        unless ($to->addHost($host)) {
            $error = $to->{error};
            return 0;
        }
    }
    return 1;
}

sub syncGroups {
    (my $from, my $to) = @_;
    my @from = $from->getAllGroups();
    my @to = $to->getAllGroups();
    @to = () unless $to[0];
    if (@from and not defined $from[0]) {
        $error = $from->{error};
        return undef;
    }
    my %from;
    my %to;
    @from{@from} = @from;
    @to{@to} = @to;
    
    for my $group (sort {$b cmp $a} @to) {
        if ($from{$group}) {
            delete $from{$group};
        } else {
            unless ($to->removeGroup($group)) {
                $error = $to->{error};
                return 0;
            }
        }
    }
    
    for my $group (sort {$a cmp $b} keys %from) {
        unless ($to->addGroup($group)) {
            $error = $to->{error};
            return 0;
        }
    }
    return 1;
}

sub syncConfigurations {
    (my $from, my $to) = @_;
    my $didcommon = 0;
    my @distributions = $from->getAllDistributions();
    foreach my $distribution (@distributions) {
        my @from = $from->getAllConfigurations($distribution);
        my @to = $to->getAllConfigurations($distribution);
        @to = () unless $to[0];
        if (@from and not defined $from[0]) {
            $error = $from->{error};
            return undef;
        }

        if ($didcommon) {
            @to = grep !/^common\//, @to;
            @from = grep !/^common\//, @from;
        }
        $didcommon = 1 if (grep /^common\//,@from or grep /^common\//, @to);
        
        my %from;
        my %to;
        @from{@from} = @from;
        @to{@to} = @to;

        for my $configuration (sort {$b cmp $a} @to) {
            if ($from{$configuration}) {
                delete $from{$configuration};
            } else {
                unless ($to->removeConfiguration($distribution, $configuration)) {
                    $error = $to->{error};
                    return 0;
                }
            }
        }
        
        for my $configuration (sort {$a cmp $b} keys %from) {
            unless ($to->addConfiguration($distribution, $configuration)) {
                $error = $to->{error};
                return 0;
            }
        }
    }
    return 1;
}

sub syncDistributions {
    (my $from, my $to) = @_;
    my @from = $from->getAllDistributions();
    my @to = $to->getAllDistributions();
    @to = () unless $to[0];
    my @allFrom = @to;
    if (@from and not defined $from[0]) {
        $error = $from->{error};
        return undef;
    }
    my %from = ();
    my %to = ();
    @from{@from} = @from if @from;
    @to{@to} = @to if @to;
    
    for my $distribution (@to) {
        if ($from{$distribution}) {
            # can't downgrade, need to delete. Otherwise, just upgrade version
            if (versionCompare($from->getDistributionVersion($distribution), $to->getDistributionVersion($distribution)) < 0) {
                unless ($to->removeDistribution($distribution)) {
                    $error = $to->{error};
                    return 0;
                }
            } else {
                unless (defined $to->setDistributionVersion($distribution, $from->getDistributionVersion($distribution))) {
                    $error = $to->{error};
                    return 0;
                }
                delete $from{$distribution};
            }
        } else {
            unless ($to->removeDistribution($distribution)) {
                $error = $to->{error};
                return 0;
            }
        }
    }
    
    for my $distribution (keys %from) {
        unless ($to->addDistribution($distribution, $from->getDistributionVersion($distribution))) {
            $error = $to->{error};
            return 0;
        }
        unless ($to->setDistributionVersion($distribution, $from->getDistributionVersion($distribution))) {
            $error = $to->{error};
            return 0;
        }
    }

    return 1;
}

################################################################################
#   The end
################################################################################

1;

__END__

=back

=head1 SEE ALSO

L<Staple> - Staple main module.

L<Staple::DB> - database connection API

L<Staple::DB::FS> - Filesystem Database

L<Staple::DB::SQL> - SQL Database

=head1 AUTHOR

Yair Yarom, E<lt>irush@cs.huji.ac.ilE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2007-2010 Hebrew University Of Jerusalem, Israel
See the LICENSE file.

=cut
