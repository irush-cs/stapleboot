package Staple::DB::FS;

#
# Copyright (C) 2007-2009 Hebrew University Of Jerusalem, Israel
# See the LICENSE file.
#
# Author: Yair Yarom <irush@cs.huji.ac.il>
#

use strict;
use warnings;
require Exporter;
use Staple::DB;
use Staple::Misc;
our @ISA = ("Staple::DB");
our $VERSION = '003';

=head1 NAME

  Staple::DB::FS - API for filesystem as database connection

=cut

################################################################################
#   Exported
################################################################################

=head1 DESCRIPTION

=over

=item B<new(path)>

creates a new instance, the path is the staple direcotry.

=cut

sub new {
    my $proto = shift;
    my $path = shift;
    return undef unless $path;
    my $class = ref($proto) || $proto;
    my $self = {};
    $self->{error} = "";
    $self->{stapleDir} = $path;
    bless ($self, $class);
    return $self;
}

sub info {
    my $self = shift;
    my $db = "fs $self->{stapleDir}";
    if (-r "/proc/mounts" and open(PROC, "/proc/mounts")) {
        my @mounts = <PROC>;
        close(PROC);
        (my $fs) = (grep {(split /\s/,$_)[1] eq $self->{stapleDir}} @mounts);
        $db = $db." (".(split /\s/,$fs)[0].")" if defined $fs;   
    }
    return $db;
}

sub addHost {
    my $self = shift;
    my $host = shift;
    return 0 if ($self->{error} = invalidHost($host));
    my $hostPath = $self->getHostPath($host, 1);
    if (not -d "$hostPath") {
        return $self->mkdirs($hostPath);
    }
    $self->{error} = "Host already exists";
    return 0;
}

sub removeHost {
    my $self = shift;
    my $host = shift;
    return 0 if ($self->{error} = invalidHost($host));
    my $hostPath = $self->getHostPath($host);
    if ($hostPath) {
        my @dirList = getDirectoryList("$hostPath");
        $! = undef;
        foreach my $file (grep {! -d} @dirList) {
            unless (unlink $file) {
                $self->{error} = "Error cleaning directory: $!";
                return 0;
            }
        }
        foreach my $dir (grep {-d} @dirList) {
            unless (rmdir $dir) {
                $self->{error} = "Error deleting directory: $!";
                return 0;
            }
        }
        unless (rmdir "$hostPath") {
            $self->error = "Error deleting host: $!";
            return 0;
        }
        return 1;
    }
    $self->{error} = "Host \"$host\" doesn't exist";
    return 0;
}

sub addGroup {
    my $self = shift;
    my $group = shift;
    return 0 if ($self->{error} = invalidGroup($group));
    my $groupPath = $self->getGroupPath($group, 1);
    if (not -d "$groupPath") {
        return $self->mkdirs($groupPath);
    }
    $self->{error} = "Group already exists";
    return 0;
}

sub removeGroup {
    my $self = shift;
    my $group = shift;
    return undef if ($self->{error} = invalidGroup($group));
    my $groupPath = $self->getGroupPath($group);
    if ($groupPath) {
        my @dirList = sort {$b cmp $a} getDirectoryList("$groupPath");
        $! = undef;
        foreach my $file (grep {! -d} @dirList) {
            unless (unlink $file) {
                $self->{error} = "Error deleting file $file: $!";
                return undef;
            }
        }
        foreach my $dir (grep {-d} @dirList) {
            unless (rmdir $dir) {
                $self->{error} = "Error deleting directory $dir: $!";
                return undef;
            }
        }
        unless (rmdir "$groupPath") {
            $self->{error} = "Error deleting directory $groupPath: $!";
            return undef;
        }
        return 1
    }
    $self->{error} = "Group doesn't exists";
    return undef;
}

sub addDistribution {
    my $self = shift;
    my $distribution = shift;
    return 0 if ($self->{error} = invalidDistribution($distribution));
    my $distributionPath = $self->getDistributionPath($distribution, 1);
    if (not -d "$distributionPath") {
        return $self->mkdirs($distributionPath);
    }
    $self->{error} = "Distribution already exists";
    return 0;
}

sub removeDistribution {
    my $self = shift;
    my $distribution = shift;
    return undef if ($self->{error} = invalidDistribution($distribution));
    my $distributionPath = $self->getDistributionPath($distribution);
    if ($distributionPath) {
        my @dirList = sort {$b cmp $a} getDirectoryList("$distributionPath");
        $! = undef;
        foreach my $file (grep {! -d} @dirList) {
            unless (unlink $file) {
                $self->{error} = "Error deleting file $file: $!";
                return undef;
            }
        }
        foreach my $dir (grep {-d} @dirList) {
            unless (rmdir $dir) {
                $self->{error} = "Error deleting directory $dir: $!";
                return undef;
            }
        }
        unless (rmdir "$distributionPath") {
            $self->{error} = "Error deleting directory $distributionPath: $!";
            return undef;
        }
        return 1
    }
    $self->{error} = "Distribution doesn't exists";
    return undef;
}

sub addConfiguration {
    my $self = shift;
    my $distribution = shift;
    my $configuration = shift;
    return 0 if ($self->{error} = invalidDistribution($distribution));
    return 0 if ($self->{error} = invalidConfiguration($configuration));
    my $path = $self->getConfigurationPath($configuration, $distribution, 1);
    unless (-d $path) {
        return $self->mkdirs($path);
    }
    $self->{error} = "Configuration already exists";
    return 0;
}

sub removeConfiguration {
    my $self = shift;
    my $distribution = shift;
    my $configuration = shift;
    return undef if ($self->{error} = invalidDistribution($distribution));
    return undef if ($self->{error} = invalidConfiguration($configuration));
    my $path = $self->getConfigurationPath($configuration, $distribution);
    if ($path) {
        my @dirList = sort {$b cmp $a} getDirectoryList("$path");
        $! = undef;
        foreach my $file (grep {! -d} @dirList) {
            unless (unlink $file) {
                $self->{error} = "Error deleting file $file: $!";
                return undef;
            }
        }
        foreach my $dir (grep {-d} @dirList) {
            unless (rmdir $dir) {
                $self->{error} = "Error deleting directory $dir: $!";
                return undef;
            }
        }
        unless (rmdir "$path") {
            $self->{error} = "Error deleting directory $path: $!";
            return undef;
        }
        return 1
    }
    $self->{error} = "Configuration doesn't exists";
    return undef;
}

sub copyConfiguration {
    my $self = shift;
    my $conf = shift;
    my $from = shift;
    my $to = shift;
    my $fromPath = $self->getConfigurationPath($conf, $from);
    my $toPath = $self->getConfigurationPath($conf, $to, 1);
    unless ($fromPath) {
        $self->{error} = "Configuration \"$conf\" doesn't exist";
        return undef;
    }
    unless ($self->getDistributionPath($from)) {
        $self->{error} = "Distribution \"$from\" doesn't exist";
        return undef;        
    }
    unless ($self->getDistributionPath($to)) {
        $self->{error} = "Distribution \"$to\" doesn't exist";
        return undef;        
    }
    if ($self->getConfigurationPath($conf, $to) and not $self->removeConfiguration($to, $conf)) {
        return undef;
    }
    unless ($self->mkdirs($toPath)) {
        return undef;
    }
    unless (system("cp -RPp $fromPath/* $toPath/ 2>&1") == 0) {
        $self->{error} = "Error coping $fromPath to $toPath";
        return undef;
    }
    return 1;
}

sub addTokens {
    my $self = shift;
    my $tokens = shift;
    my $group = shift;
    unless (-d $group->{path}) {
        if ($group->{type}) {
            $self->{error} = "Group $group->{name} does not exist";
        } else {
            $self->{error} = "Configuration $group->{name} does not exist";
        }
        return undef;
    }

    # old tokens style
    return undef unless ($self->mkdirs("$group->{path}/tokens"));
    foreach my $type ("static", "dynamic", "regexp") {
        my @tokens = map {$_->{key}} grep {$_->{type} eq $type} values %$tokens;
        next unless @tokens;
        my %newTokens = ();
        @newTokens{@tokens} = @{$tokens}{@tokens};
        my $file = "$group->{path}/tokens/$type";
        my %oldTokens = readTokensFile($file, $type);
        @oldTokens{keys %newTokens} = values %newTokens;
        unless (writeTokensFile($file, \%oldTokens)) {
            $self->{error} = "Can't write tokens file $file: $!\n";
            return undef;
        }
    }

    # new xml style
    my $file = "$group->{path}/tokens.xml";
    my @read = readTokensXMLFile($file);
    my %newTokens = ();
    %newTokens = @read if (@read > 1);
    @newTokens{keys %$tokens} = values %$tokens;
    unless (writeTokensXMLFile($file, \%newTokens)) {
        $self->{error} = "Can't write tokens file $file: $!\n";
        return undef;
    }
    return 1;
}

sub removeTokens {
    my $self = shift;
    my $tokens = shift;
    my $group = shift;
    unless (-d $group->{path}) {
        if ($group->{type}) {
            $self->{error} = "Group $group->{name} does not exist";
        } else {
            $self->{error} = "Configuration $group->{name} does not exist";
        }
        return undef;
    }

    # old tokens style
    foreach my $type ("static", "dynamic", "regexp") {
        my $file = "$group->{path}/tokens/$type";
        my %oldTokens = readTokensFile($file);
        if (delete @oldTokens{@$tokens}) {
            unless (writeTokensFile($file, \%oldTokens)) {
                $self->{error} = "Can't write tokens file $file: $!\n";
                return undef;
            }
        }
    }

    # new xml style
    my $file = "$group->{path}/tokens.xml";
    my @read = readTokensXMLFile($file);
    my %oldTokens = ();
    %oldTokens = @read if (@read > 1);
    if (delete @oldTokens{@$tokens}) {
        unless (writeTokensXMLFile($file, \%oldTokens)) {
            $self->{error} = "Can't write tokens to file $file: $!\n";
            return undef;
        }
    }
    
    return 1;
}

sub getTokens {
    return getTokensXML(@_);
    #return getTokensOLD(@_);
}

sub getTokensXML {
    my $self = shift;
    my @gorc = @_;
    my %tokens = ();
    my @tokenFiles = map {$_->{path} ? "$_->{path}/tokens.xml" : undef} @gorc;
    foreach my $gorc (@_) {
        my $tokenFile = shift @tokenFiles;
        next if not defined $tokenFile or not -r $tokenFile;
        my %currentTokens = readTokensXMLFile($tokenFile);
        my $prefix;
        if ($gorc->{type}) {
            # only group has type
            $prefix = $gorc->{type};
        } else {
            # configuration has no type
            $prefix = "configuration";
        }
        map {$_->{source} = "$prefix:$gorc->{name}";} values %currentTokens;
        @tokens{keys %currentTokens} = values %currentTokens;
    }
    return \%tokens;
}

sub getTokensOLD {
    my $self = shift;
    my @tokenFiles = grep {$_} map {$_->{path} ? "$_->{path}/tokens" : undef} @_;
    my %tokens = ();
    foreach my $token (@tokenFiles) {
        if (-r "$token/static") {
            my %currentTokens = readTokensFile("$token/static", "static");
            @tokens{keys %currentTokens} = values %currentTokens;
        }
        
        if (-r "$token/regexp") {
            my %currentTokens = readTokensFile("$token/regexp", "regexp");
            map {$currentTokens{$_}->{value} = ""} keys %currentTokens;
            @tokens{keys %currentTokens} = values %currentTokens;
        }
        
        if (-r "$token/dynamic") {
            my %currentTokens = readTokensFile("$token/dynamic", "dynamic");
            map {$currentTokens{$_}->{value} = ""} keys %currentTokens;
            @tokens{keys %currentTokens} = values %currentTokens;
        }
    }
    return \%tokens;
}

sub getGroups {
    my $self = shift;
    my $group = shift;
    my $groupsFile = "$group->{path}/groups";
    if (open(FILE, "<$groupsFile")) {
        my @groups = <FILE>;
        close(FILE);
        chomp @groups;
        return @groups;
    }
    return ();
}

sub getMounts {
    my $self = shift;    
    my @configurations = @_;
    my @mounts = ();
    foreach my $configuration (@configurations) {
        my $path = "$configuration->{path}/mounts";
        if (-r "$path") {
            open(FILE, "<$path");
            my @rawMounts = <FILE>;
            close(FILE);
            chomp @rawMounts;
            push @mounts, map {/^([+-])(.*)$/; {destination => "$2", active => "$1", configuration => $configuration}} @rawMounts;
        }
    }
    foreach my $mount (@mounts) {
        $mount->{active} = 1 if $mount->{active} eq '+';
        $mount->{active} = 0 if $mount->{active} eq '-';
    }
    return @mounts;   
}

sub getTemplates {
    my $self = shift;
    my @configurations = @_;
    my %templates = ();
    foreach my $configuration (@configurations) {
        my $path = "$configuration->{path}/templates";
        next unless -d "$path";
        my @raw = map {(my $a = $_) =~ s/$path//; $a} grep {! -d} getDirectoryList("$path");
        foreach my $rawTemplate (@raw) {
            if ($rawTemplate =~ m!^/(.*?)(/.*/?)([^/]*)$!) {
                (my $mode, my $uid, my $gid) = (stat("$path/$rawTemplate"))[2,4,5];
                $mode &= 07777;
                $templates{"$1.$2$3"} = {source => "$path$rawTemplate",
                                         data => undef,
                                         destination => "$2$3",
                                         stage => "$1",
                                         configuration => $configuration,
                                         mode => "$mode",
                                         uid => "$uid",
                                         gid => "$gid",
                                        };
            }
        }
    }
    return values %templates;
}

sub addTemplates {
    my $self = shift;
    my @templates = @_;
    my @errors = ();
    foreach my $template (@templates) {
        unless ($template->{destination}) {
            push @errors, "Template missing destination";
            next;
        }
        if ($template->{uid} !~ /^\d+$/) {
            my $uid;
            (undef, undef, $uid) = getpwnam($template->{uid});
            unless (defined $uid) {
                push @errors, "can't find uid of \"$template->{uid}\"";
                next;
            }
            $template->{uid} = $uid;
        }
        if ($template->{gid} !~ /^\d+$/) {
            my $gid;
            (undef, undef, $gid) = getgrnam($template->{gid});
            unless (defined $gid) {
                push @errors, "can't find gid of \"$template->{gid}\"";
                next;
            }
            $template->{gid} = $gid;
        }
        my $path = "$template->{configuration}->{path}/templates/$template->{stage}/$template->{destination}";
        if (-e $path) {
            unless (unlink $path) {
                push @errors, "can't remove previous template \"$path\": $!";
                next;
            }
        }
        my $dir = $path;
        $dir =~ s,/[^/]*$,,;
        unless ($self->mkdirs($dir)) {
            push @errors, $self->{error};
            next;
        }
        if ($template->{source}) {
            unless(system("cp $template->{source} $path >/dev/null 2>&1") == 0) {
                push @errors, "can't copy \"$template->{source}\" to \"$path\"";
                next;
            }
        } else {
            unless (open(FILE, ">$path")) {
                push @errors, "can't open \"$path\" for writing: $!";
                next;
            }
            print FILE $template->{data};
            close(FILE);
        }
        unless (chown $template->{uid}, $template->{gid}, $path) {
            push @errors, "can't chown \"$path\": $!";
            next;
        }
        unless (chmod($template->{mode}, $path)) {
            push @errors, "can't chmod \"$path\": $!";
            next;
        }
    }
    if (@errors) {
        $self->{error} = join "\n", @errors;
        return undef;
    }
    return 1;
}

sub removeTemplates {
    my $self = shift;
    my @templates = @_;
    my @errors = ();
    foreach my $template (@templates) {
        my $path = "$template->{configuration}->{path}/templates/$template->{stage}/$template->{destination}";
        unless (-e $path) { 
            push @errors, "Template \"$template->{destination}\" does not exist in the configuration \"$template->{configuration}->{name}\"";
            next;
        }
        unless (unlink $path) {
            push @errors, "Can't remove template $path: $!";
            next;
        }
    }
    if (@errors) {
        $self->{error} = join "\n", @errors;
        return undef;
    }
    return 1;
}

sub getScripts {
    my $self = shift;
    my @configurations = @_;
    my @results = ();
    foreach my $configuration (@configurations) {
        my $path = "$configuration->{path}/scripts";
        my @scripts = ();
        next unless (-d "$path");
        my @raw = map {(my $a = $_) =~ s/$path\///; $a} grep { ! -d $_ } getDirectoryList("$path");
        foreach my $script (@raw) {
            if ($script =~ m!^(auto|mount|sysinit|final)/(\d+)\.(c?)(t?)(m?)\.(.*)$!) {
                push @scripts, {name => "$6",
                                source => "$path/$script",
                                stage => "$1",
                                order => "$2",
                                critical => "$3",
                                tokens => "$4",
                                tokenScript => "$5",
                                configuration => $configuration};
            }
        }
        map {$_->{critical} = $_->{critical} ? 1 : 0} @scripts;
        map {$_->{tokens} = $_->{tokens} ? 1 : 0} @scripts;
        map {$_->{tokenScript} = $_->{tokenScript} ? 1 : 0} @scripts;
        push @results, sort {$a->{stage} eq $b->{stage} ? $a->{order} <=> $b->{order} : stageCmp($a->{stage}, $b->{stage})} @scripts;
    }
    return @results;
}

sub addScripts {
    my $self = shift;
    my @scripts = @_;
    my @errors = ();
    foreach my $script (@scripts) {
        my $data = $script->{data};
        if ($script->{source}) {
            unless (open(FILE, "<$script->{source}")) {
                push @errors, "can't open source \"$script->{source}\" for reading: $!";
                next;
            }
            $data = join "", <FILE>;
            close(FILE);
        }
        my @oldScripts = grep {$_->{stage} eq $script->{stage}} $self->getScripts($script->{configuration});
        $script->{order} = scalar(@oldScripts) + 1 if not defined $script->{order} or $script->{order} > scalar(@oldScripts) or $script->{order} < 1;
        unless($self->mkdirs("$script->{configuration}->{path}/scripts/$script->{stage}/")) {
            push @errors, $self->{error};
            next;
        }
        unless ($self->openOrdering($script->{order}, "$script->{configuration}->{path}/scripts/$script->{stage}/")) {
            push @errors, $self->{error};
            next;
        }
        my $file = "$script->{configuration}->{path}/scripts/$script->{stage}/$script->{order}.".($script->{critical} ? "c" : "").($script->{tokens} ? "t" : "").($script->{tokenScript} ? "m" : "").".$script->{name}";
        unless (open(FILE, ">$file")) {
            push @errors, "can't open file for writing \"$file\": $!";
            next;
        }
        print FILE $data;
        close(FILE);
        unless (chmod 0755, $file) {
            push @errors, "can't chmod 0755 \"$file\": $!";
            return;
        }
    }
    if (@errors) {
        $self->{error} = join "\n", @errors;
        return undef;
    }
    return 1;
}

sub removeScripts {
    my $self = shift;
    my @scripts = @_;
    my @errors = ();
    foreach my $script (sort {$b->{order} <=> $a->{order}} @scripts) {
        unless (unlink $script->{source}) {
            push @errors, "Can't delete \"$script->{source}\": $!";
            next;
        }
        (my $dir) = $script->{source} =~ m,^(.*/)[^/]+$,;
        unless ($self->closeOrdering($script->{order}, $dir)) {
            push @errors, $self->{error};
            next;
        }
    }
    if (@errors) {
        $self->{error} = join "\n", @errors;
        return undef;
    }
    return 1;
}

sub getAutos {
    my $self = shift;
    my @configurations = @_;
    my @results = ();
    foreach my $configuration (@configurations) {
        my $path = "$configuration->{path}/autos";
        my @autos = ();
        next unless (-d "$path");
        my @raw = map {(my $a = $_) =~ s/$path\///; $a} grep { ! -d $_ } getDirectoryList("$path");
        foreach my $auto (@raw) {
            if ($auto =~ m!^(\d+)\.(c?)(t?)\.(.*)$!) {
                push @autos, {name => "$4",
                              source => "$path/$auto",
                              order => "$1",
                              critical => "$2",
                              tokens => "$3",
                              configuration => $configuration};
            }
        }
        push @results, @autos;
    }
    map {$_->{critical} = $_->{critical} ? 1 : 0} @results;
    map {$_->{tokens} = $_->{tokens} ? 1 : 0} @results;
    return sort {$a->{order} <=> $b->{order}} @results;
}


sub addAutos {
    my $self = shift;
    my @autos = @_;
    my @errors = ();
    foreach my $auto (@autos) {
        my $data = $auto->{data};
        if ($auto->{source}) {
            unless (open(FILE, "<$auto->{source}")) {
                push @errors, "can't open source \"$auto->{source}\" for reading: $!";
                next;
            }
            $data = join "", <FILE>;
            close(FILE);
        }
        my @oldAutos = $self->getAutos($auto->{configurations});
        $auto->{order} = scalar(@oldAutos) + 1 if not defined $auto->{order} or $auto->{order} > scalar(@oldAutos) or $auto->{order} < 1;
        unless($self->mkdirs("$auto->{configuration}->{path}/autos/")) {
            push @errors, $self->{error};
            next;
        }
        unless ($self->openOrdering($auto->{order}, "$auto->{configuration}->{path}/autos/")) {
            push @errors, $self->{error};
            next;
        }
        my $file = "$auto->{configuration}->{path}/autos/$auto->{order}.".($auto->{critical} ? "c" : "").($auto->{tokens} ? "t" : "").".$auto->{name}";
        unless (open(FILE, ">$file")) {
            push @errors, "can't open file for writing \"$file\": $!";
            next;
        }
        print FILE $data;
        close(FILE);
        unless (chmod 0755, $file) {
            push @errors, "can't chmod 0755 \"$file\": $!";
            return;
        }
    }
    if (@errors) {
        $self->{error} = join "\n", @errors;
        return undef;
    }
    return 1;
}

sub removeAutos {
    my $self = shift;
    my @autos = @_;
    my @errors = ();
    foreach my $auto (sort {$b->{order} <=> $a->{order}} @autos) {
        unless (unlink $auto->{source}) {
            push @errors, "Can't delete \"$auto->{source}\": $!";
            next;
        }
        (my $dir) = $auto->{source} =~ m,^(.*/)[^/]+$,;
        unless ($self->closeOrdering($auto->{order}, $dir)) {
            push @errors, $self->{error};
            next;
        }
    }
    if (@errors) {
        $self->{error} = join "\n", @errors;
        return undef;
    }
    return 1;
}

sub addMount {
    my $self = shift;
    my $configuration = shift;
    my $mount = shift;
    my $location = shift;
    $location = int $location if $location;
    my @results;
    my @mounts = $self->getMounts($configuration);
    my $i = 0;
    @mounts = grep {$_->{destination} ne $mount->{destination} or $_->{active} ne $mount->{active}} @mounts;
    $location = scalar(@mounts) + 1 if not $location or $location > @mounts;
    foreach my $mnt (@mounts) {
        $i++;
        if ($i == $location) {
            push @results, $mount;
            redo;
        }
        push @results, $mnt;
    }
    push @results, $mount if $location > @mounts;
    return $self->setMounts($configuration, @results);
}

sub removeMounts {
    my $self = shift;
    my @allMounts = @_;
    my @errors = ();
    my @configurations = map {"$_->{configuration}->{name}:$_->{configuration}->{dist}"} @allMounts;
    my %configurations = ();
    @configurations{@configurations} = @configurations;
    foreach my $conf (keys %configurations) {
        my @mounts = grep {"$_->{configuration}->{name}:$_->{configuration}->{dist}" eq $conf} @allMounts;
        my @oldMounts = $self->getMounts($mounts[0]->{configuration});
        my @results = ();
        foreach my $mount (@oldMounts) {
            push @results, $mount unless grep {$mount->{destination} eq $_->{destination} and $mount->{active} eq $_->{active}} @mounts;
        }
        unless ($self->setMounts($mounts[0]->{configuration}, @results)) {
            push @errors, $self->{error};
        }
    }
    if (@errors) {
        $self->{error} = join "\n", @errors;
        return undef;
    }
    return 1;
}

sub getGroupConfigurations {
    my $self = shift;
    my $group = shift;
    my @configurations = ();
    if (-r "$group->{path}/configurations") {
        my @configurationData = ();
        if (open(FILE, "<$group->{path}/configurations")) {
            @configurationData = <FILE>;
            close(FILE);
        } else {
            $self->{error} = "failed to open $group->{path}/configurations: $!";
            return undef;
        }
        push @configurations, map {my $a = $_; chomp $a; $a} @configurationData;
        @configurations = map {m/^([+-])(.*)$/; {name => $2, active => $1, group => $group, path => undef, dist => undef}} @configurations;
        map {if ($_->{active} eq '+') {$_->{active} = 1} else {$_->{active} = 0}} @configurations;
    }
    return @configurations;
}

sub addGroupConfiguration {
    my $self = shift;
    my $group = shift;
    my $configuration = shift;
    my $location = shift;
    $location = int $location if $location;
    my @results;
    my @configurations = $self->getGroupConfigurations($group);
    my $i = 0;
    @configurations = grep {$_->{name} ne $configuration->{name} or $_->{active} ne $configuration->{active}} @configurations;
    $location = scalar(@configurations) + 1 if not $location or $location > @configurations;
    foreach my $conf (@configurations) {
        $i++;
        if ($i == $location) {
            push @results, $configuration;
            redo;
        }
        push @results, $conf;
    }
    push @results, $configuration if $location > @configurations;
    return $self->setGroupConfigurations($group, @results);
}

sub removeGroupConfigurations {
    my $self = shift;
    my $group = shift;
    my @toRemove = map {($_->{active} ? "+" : "-").$_->{name}} @_;
    my %toRemove = ();
    @toRemove{@toRemove} = @toRemove;
    my @configurations = $self->getGroupConfigurations($group);
    my @results = ();
    foreach my $configuration (@configurations) {
        push @results, $configuration unless $toRemove{($configuration->{active} ? "+" : "-").$configuration->{name}};
    }
    return $self->setGroupConfigurations($group, @results);
}

sub addGroupGroup {
    my $self = shift;
    my $group = shift;
    my $name = shift;
    my $location = shift;
    unless ($self->getGroupPath($name)) {
        $self->{error} = "Group \"$name\" does not exist";
        return undef;
    }
    $location = int $location if $location;
    my @results;
    my @groups = $self->getGroups($group);
    my $i = 0;
    @groups = grep {$_ ne $name} @groups;
    $location = scalar(@groups) + 1 if not $location or $location > @groups;
    foreach my $element (@groups) {
        $i++;
        if ($i == $location) {
            push @results, $name;
            redo;
        }
        push @results, $element if $element ne $name;
    }
    push @results, $name if $location > @groups;
    return $self->setGroupGroups($group, @results);
}

sub removeGroupGroups {
    my $self = shift;
    my $group = shift;
    my %toRemove = ();
    @toRemove{@_} = @_;
    my @groups = $self->getGroups($group);
    my @results = ();
    foreach my $group (@groups) {
        push @results, $group unless $toRemove{$group};
    }
    return $self->setGroupGroups($group, @results);
}

sub getAllHosts {
    my $self = shift;
    my @hosts = ();
    my $path = $self->getHostPath("/");
    if ($path) {
        if (opendir(DIR, $path)) {
            @hosts = readdir(DIR);
            closedir(DIR);
            @hosts = grep { !/(^\.$)|(^\.\.$)/ } @hosts;
            @hosts = grep { $self->getHostPath($_) } @hosts;
        } else {
            $self->{error} = "error opening $path: $!";
            return undef;
        }
    } else {
        # no "/", database isn't build
        return ();
    }
    return sort {$a cmp $b} @hosts;
}

sub getAllGroups {
    my $self = shift;
    my $path = $self->getGroupPath("/");
    return () unless $path;
    my @groups = getDirectoryList($path);
    @groups = grep { -d $_ } @groups;
    @groups = grep {!m#(?<!subgroups)/tokens(?:/|$)#} @groups;
    @groups = grep {!m#(?<!subgroups)/subgroups$#} @groups;
    @groups = map { s/^$self->{stapleDir}//; $_ } @groups;
    @groups = map { s!/subgroups/!/!g; $_ } @groups;
    @groups = map { s!/groups/!/!; $_ } @groups;
    @groups = map { fixPath($_) } @groups;
    return sort {$a cmp $b} @groups;
}

sub whoHasGroup {
    my $self = shift;
    my $group = shift;
    my $suffix = "(\$|/)";
    if ($group =~ m/\$$/) {
        $group =~ s/\$$//;
        $suffix = "\$";
    }
    if (not defined $self->getGroupPath($group)) {
        $self->{error} = "Unknown group \"$group\"";
        return undef;
    }

    # hosts
    my $cmd = "find ".$self->{stapleDir}."/hosts -type f -name groups -print0 | xargs -0 egrep -l '^".$group.$suffix."'";
    my @hosts = `$cmd`;
    chomp @hosts;
    # grep returns an error if nothing is found
    #if ($? >> 8) {
    #    $self->{error} = "Error executing \"$cmd\": ".($? >> 8);
    #    return undef;
    #}
    @hosts = map {$a = $_; $a =~ s,^.*/([^/]+)/groups$,$1,;$a} @hosts;
    @hosts = map {$self->getHostGroup($_)} @hosts;
    return undef if (grep {not defined $_} @hosts);

    # distributions
    $cmd = "find ".$self->{stapleDir}."/distributions -type f -name groups -print0 | xargs -0 egrep -l '^".$group.$suffix."'";
    my @distributions = `$cmd`;
    chomp @distributions;
    #if ($? >> 8) {
    #    $self->{error} = "Error executing \"$cmd\": ".($? >> 8);
    #    return undef;
    #}
    
    @distributions = map {$a = $_; $a =~ s,^.*/([^/]+)/groups$,$1,;$a} @distributions;
    @distributions = map {$self->getDistributionGroup($_)} @distributions;
    return undef if (grep {not defined $_} @distributions);

    # groups
    $cmd = "find ".$self->{stapleDir}."/groups -type f -name groups -print0 | xargs -0 egrep -l '^".$group.$suffix."'";
    my @groups = `$cmd`;
    chomp @groups;
    #if ($? >> 8) {
    #    $self->{error} = "Error executing \"$cmd\": ".($? >> 8);
    #    return undef;
    #}

    @groups = map {$a = $_; $a =~ s,^$self->{stapleDir}/groups(/.+)/groups$,$1,; $a =~ s,/subgroups/,/,g; $a} @groups;
    @groups = $self->getGroupsByName(@groups);
    return undef if (grep {not defined $_} @groups);

    return @hosts, @distributions, @groups;
}


sub whoHasConfiguration {
    my $self = shift;
    my $configuration = shift;
    my $suffix = "(\$|/)";
    if ($configuration =~ m/\$$/) {
        $configuration =~ s/\$$//;
        $suffix = "\$";
    }
    
    # can't check if configuration exists, on which distribution to check?
    
    # hosts
    my $cmd = "find ".$self->{stapleDir}."/hosts -type f -name configurations -print0 | xargs -0 egrep -l '^[+-]".$configuration.$suffix."'";
    my @hosts = `$cmd`;
    chomp @hosts;
    @hosts = map {$a = $_; $a =~ s,^.*/([^/]+)/configurations$,$1,;$a} @hosts;
    @hosts = map {$self->getHostGroup($_)} @hosts;
    return undef if (grep {not defined $_} @hosts);

    # distributions
    $cmd = "find ".$self->{stapleDir}."/distributions -type f -name configurations -print0 | xargs -0 egrep -l '^[+-]".$configuration.$suffix."'";
    my @distributions = `$cmd`;
    chomp @distributions;

    @distributions = map {$a = $_; $a =~ s,^.*/([^/]+)/configurations$,$1,;$a} @distributions;
    @distributions = map {$self->getDistributionGroup($_)} @distributions;
    return undef if (grep {not defined $_} @distributions);
     
    # groups
    $cmd = "find ".$self->{stapleDir}."/groups -type f -name configurations -print0 | xargs -0 egrep -l '^[+-]".$configuration.$suffix."'";
    my @groups = `$cmd`;
    chomp @groups;

    @groups = map {$a = $_; $a =~ s,^$self->{stapleDir}/groups(/.+)/configurations$,$1,; $a =~ s,/subgroups/,/,g; $a} @groups;
    @groups = $self->getGroupsByName(@groups);
    return undef if (grep {not defined $_} @groups);

    return @hosts, @distributions, @groups;
}

sub whoHasToken {
    my $self = shift;
    my $key = shift;
    my $distribution = shift;
    
    if (not defined $self->getDistributionGroup($distribution)) {
        # error is already set by $self
        return undef;
    }
    
    # hosts
    my $cmd = "find ".$self->{stapleDir}."/hosts -type f \\( -path \\*/tokens/static -o -path \\*/tokens/dynamic -o -path \\*/tokens/regexp \\)  -print0 | xargs -0 grep -l '^".$key."='";
    my @hosts = `$cmd`;
    chomp @hosts;
    # grep returns an error if nothing is found
    #if ($? >> 8) {
    #    $self->{error} = "Error executing \"$cmd\": ".($? >> 8);
    #    return undef;
    #}
    @hosts = map {$a = $_; $a =~ s,^.*/([^/]+)/tokens/(?:static|dynamic|regexp)$,$1,;$a} @hosts;
    @hosts = map {$self->getHostGroup($_)} @hosts;
    return undef if (grep {not defined $_} @hosts);

    # distributions
    #$cmd = "find ".$self->{stapleDir}."/distributions -type f \\( -path \\*/tokens/static -o -path \\*/tokens/dynamic -o -path \\*/tokens/regexp \\) -print0 | xargs -0 grep -l '^".$key."='";
    $cmd = "find ".$self->{stapleDir}."/distributions/*/tokens/{static,dynamic,regexp} -print0 2>/dev/null | xargs -0 grep -l '^".$key."='";
    my @distributions = `$cmd`;
    chomp @distributions;
    #if ($? >> 8) {
    #    $self->{error} = "Error executing \"$cmd\": ".($? >> 8);
    #    return undef;
    #}
    
    @distributions = map {$a = $_; $a =~ s,^.*/([^/]+)/tokens/(?:static|dynamic|regexp)$,$1,;$a} @distributions;
    @distributions = map {$self->getDistributionGroup($_)} @distributions;
    return undef if (grep {not defined $_} @distributions);

    # groups
    $cmd = "find ".$self->{stapleDir}."/groups -type f \\( -path \\*/tokens/static -o -path \\*/tokens/dynamic -o -path \\*/tokens/regexp \\) -print0 | xargs -0 grep -l '^".$key."='";
    my @groups = `$cmd`;
    chomp @groups;
    #if ($? >> 8) {
    #    $self->{error} = "Error executing \"$cmd\": ".($? >> 8);
    #    return undef;
    #}
    
    @groups = map {$a = $_; $a =~ s,^$self->{stapleDir}/groups(/.+)/tokens/(?:static|dynamic|regexp)$,$1,; $a =~ s,/subgroups/,/,g; $a} @groups;
    @groups = $self->getGroupsByName(@groups);
    return undef if (grep {not defined $_} @groups);
    
    # configurations
    $cmd = "find ".$self->{stapleDir}."/distributions/$distribution/confs -type f \\( -path */tokens/static -o -path */tokens/dynamic -o -path */tokens/regexp \\) -print0 | xargs -0 grep -l '^".$key."='";
    my @configurations = `$cmd`;
    chomp @configurations;
    #if ($? >> 8) {
    #    $self->{error} = "Error executing \"$cmd\": ".($? >> 8);
    #    return undef;
    #}
    
    @configurations = map {
        $a = $_;
        $a =~ s,^$self->{stapleDir}/distributions/${distribution}/confs(/.+)/tokens/(?:static|dynamic|regexp)$,$1,;
        $a =~ s,/configurations/,/,g;
        $a} @configurations;

    @configurations = map {{name => $_, path => undef, dist => undef, active => 1, group => undef}} @configurations;
    @configurations = $self->getFullConfigurations(\@configurations, $distribution);
    return undef if (grep {not defined $_} @configurations);

    return ([@hosts, @distributions, @groups], [@configurations])
}

sub getAllConfigurations {
    my $self = shift;
    my $distribution = shift;
    my $path = $self->getConfigurationPath("/", $distribution);
    return () unless $path;
    my @configurations = getDirectoryList($path);
    @configurations = grep {-d $_ } @configurations;
    @configurations = grep { s/^$path//; $_} @configurations;
    @configurations = grep { m!^/[^/]+$! or m!configurations/[^/]+$! } @configurations;
    @configurations = map { s!/configurations/!/!g; $_ } @configurations;
    return sort {$a cmp $b} @configurations;
}

sub getAllDistributions {
    my $self = shift;
    my $path = $self->getDistributionPath("/");
    return () unless $path;
    unless (opendir(DIR, $path)) {
        $self->{error} = "Can't open direcotry $path: $!";
        return undef;
    }
    my @distributions = grep { !/(^\.$)|(^\.\.$)/ } readdir(DIR);
    closedir DIR;
    chomp @distributions;
    return sort {$a cmp $b} @distributions;
}

sub getConfigurationPath {
    my $self = shift;
    my $configuration = shift;
    my $distribution = shift;
    my $force = shift;
    my $path = $self->getDistributionPath($distribution,$force);
    return undef unless $path;
    $configuration =~ s!^/!!;
    $configuration =~ s!/!/configurations/!g;
    $configuration = fixPath("$path/confs/${configuration}");
    return $configuration if -d $configuration or $force;
    return undef;
}

sub getGroupPath {
    my $self = shift;
    my $group = shift;
    my $force = shift;
    $group =~ s!/!/subgroups/!g;
    $group =~ s!^/subgroups!/groups!g;
    $group = fixPath("$self->{stapleDir}${group}");
    return $group if -d $group or $force;
    return undef;
}

sub getDistributionPath {
    my $self = shift;
    my $distribution = shift;
    my $force = shift;
    my $path = fixPath("$self->{stapleDir}/distributions/$distribution");
    return $path if -d $path or $force;
    return undef;
}

sub getHostPath {
    my $self = shift;
    my $host = shift;
    my $force = shift;
    my $path = fixPath("$self->{stapleDir}/hosts/$host");
    return $path if -d $path or $force;
    return undef;
}

################################################################################
#   Internals
################################################################################

# input: (self), location, directory
# output: 1 or undef
# moves all files in direcotry with starting number >= location, to -1
sub closeOrdering {
    my $self = shift;
    my $location = shift;
    my $dir = shift;
    return unless ($location);
    unless(opendir(DIR, $dir)) {
        $self->{error} = "Can't open directory \"$dir\": $!";
        return undef;
    }
    my @files = readdir(DIR);
    closedir(DIR);
    @files = grep { !/(^\.$)|(^\.\.$)/ } @files;
    my %files = map {(my $loc) = ($_ =~ m/^(\d+)\./); "$loc" => $_} @files;
    $location = scalar(keys %files) + 1 unless $location;
    foreach my $fileLoc (sort {$a <=> $b} grep {$_ >= $location} keys %files) {
        $files{$fileLoc} =~ m/^(\d+)\.(.*)$/;
        my $newName = $1 - 1;
        $newName .= ".$2";
        unless (rename "$dir/$files{$fileLoc}", "$dir/$newName") {
            $self->{error} = "Can't move \"$dir/$files{$fileLoc}\": $!";
            return undef;
        }
    }
    return 1;    
}

# input: (self), location, directory
# output: 1 or undef
# moves all files in directory with starting number >= location, to +1
sub openOrdering {
    my $self = shift;
    my $location = shift;
    my $dir = shift;
    return unless ($location);
    unless(opendir(DIR, $dir)) {
        $self->{error} = "Can't open directory \"$dir\": $!";
        return undef;
    }
    my @files = readdir(DIR);
    closedir(DIR);
    @files = grep { !/(^\.$)|(^\.\.$)/ } @files;
    my %files = map {(my $loc) = ($_ =~ m/^(\d+)\./); "$loc" => $_} @files;
    $location = scalar(keys %files) + 1 unless $location;
    foreach my $fileLoc (sort {$b <=> $a} grep {$_ >= $location} keys %files) {
        $files{$fileLoc} =~ m/^(\d+)\.(.*)$/;
        my $newName = $1 + 1;
        $newName .= ".$2";
        unless (rename "$dir/$files{$fileLoc}", "$dir/$newName") {
            $self->{error} = "Can't move \"$dir/$files{$fileLoc}\": $!";
            return undef;
        }
    }
    return 1;
}

# input: full pathed directory
# output: 1 on success, 0 on failure
# like mkdir -p
sub mkdirs {
    my $self = shift;
    my $fulldir = shift;
    foreach my $dir (splitData($fulldir)) {
        next if -d $dir;
        unless (mkdir "$dir", 0755) {
            $self->{error} = "Failed mkdir: $!";
            return 0;
        }
    }
    return 1;
}

# input: configuration hash, list of mounts
# output: 1 or undef
# writes the mounts file
sub setMounts {
    my $self = shift;
    my $configuration = shift;
    my @mounts = @_;
    my $mountsFile = "$configuration->{path}/mounts";
    if (@mounts) {
        if (open(FILE, ">$mountsFile")) {
            print FILE join "\n", map {($_->{active} ?  "+" : "-").$_->{destination}} @mounts;
            print FILE "\n";
            close(FILE);
            return 1;
        }
        $self->{error} = "Can't open file $mountsFile: $!";
        return undef;
    } else {
        if (-r $mountsFile) {
            unless (unlink $mountsFile) {
                $self->{error} = "Can't delte file $mountsFile: $!";
                return undef;
            }
        }
    }
    return 1;
}

# input: group hash, list of group names
# output: 1 or undef
# writes the groups file
sub setGroupGroups {
    my $self = shift;
    my $group = shift;
    my @groups = @_;
    my $groupsFile = "$group->{path}/groups";
    if (@groups) {
        if (open(FILE, ">$groupsFile")) {
            print FILE join("\n", @groups)."\n";
            close(FILE);
            return 1;
        }
        $self->{error} = "Can't open file $groupsFile: $!";
        return undef;
    } else {
        if (-r $groupsFile) {
            unless (unlink $groupsFile) {
                $self->{error} = "Can't delete file $groupsFile: $!";
                return undef;
            }
        }
    }
    return 1;
}

# input: group hash, list of configuration hashes (name + active)
# output: 1 or undef
# writes the groups file
sub setGroupConfigurations {
    my $self = shift;
    my $group = shift;
    my @configurations = @_;
    my $configurationsFile = "$group->{path}/configurations";
    if (@configurations) {
        if (open(FILE, ">$configurationsFile")) {
            print FILE join "\n", map {($_->{active} ?  "+" : "-").$_->{name}} @configurations;
            print FILE "\n";
            close(FILE);
            return 1;
        }
        $self->{error} = "Can't open file $configurationsFile: $!";
        return undef;
    } else {
        if (-r $configurationsFile) {
            unless (unlink $configurationsFile) {
                $self->{error} = "Can't delete file $configurationsFile: $!";
                return undef;
            }
        }
    }
    return 1;
}

################################################################################
#   Host Internals
################################################################################

################################################################################
#   Group Internals
################################################################################

################################################################################
#   Distribution Internals
################################################################################

################################################################################
#   Tokens Internals
################################################################################

################################################################################
#   The end
################################################################################

1;

__END__

=back

=head1 SEE ALSO

L<Staple> - Staple main module.

L<Staple::DB> - the DB interface

L<Staple::DB::SLQ> - SQL - Database

=head1 AUTHOR

Yair Yarom, E<lt>irush@cs.huji.ac.ilE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2007-2009 Hebrew University Of Jerusalem, Israel
See the LICENSE file.

=cut
