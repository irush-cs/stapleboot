package Staple::DB::FS;

#
# Copyright (C) 2007-2020 Hebrew University Of Jerusalem, Israel
# See the LICENSE file.
#
# Author: Yair Yarom <irush@cs.huji.ac.il>
#

use strict;
use warnings;
require Exporter;
use Staple::DB;
use Staple::Misc;
use File::Path;
use Staple::Template;
use Staple::Link;
use Staple::Script;
use Staple::Mount;
use Staple::DBFactory;
use Fcntl ':mode';

our @ISA = ("Staple::DB");
our $VERSION = '0.2.x';

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
    return Staple::DB::Error::new("error", "Missing database path") unless $path;
    my $class = ref($proto) || $proto;
    my $self = {};
    $self->{error} = "";
    $self->{stapleDir} = $path;
    bless ($self, $class);
    $self->setTmpDir("$path/tmp");
    #opendir($self->{openeddir}, $self->{stapleDir});
    return $self;
}

=item B<create(path)>

Creates a new instance and build the directory tree if needed

=cut

sub create {
    my $self = new(@_);
    mkpath([$self->{stapleDir}, $self->{tmpDir}]);
    unless (-d $self->{stapleDir} and -d $self->{tmpDir}) {
        return createDB("error", "Can't create paths: $!\n");
    }
    unless (open(TEMP, ">$self->{tmpDir}/tmp-is-not-mounted")) {
        return createDB("error", "Can't create file: $!\n");
    }
    close(TEMP);
    return $self;
}

sub describe {
    return ("Filesystem database",
            "Receives a single parameter: the directory of the database.");
}

sub info {
    my $self = shift;
    my $db = "fs $self->{stapleDir}";
    my @mounts;
    if (-r "/proc/mounts" and open(PROC, "/proc/mounts")) {
        @mounts = <PROC>;
        close(PROC);
    } else {
        @mounts = map {join " ", (split /\s+/,$_)[0, 2]} `mount`;
    }
    (my $fs) = (grep {(split /\s/,$_)[1] eq $self->{stapleDir}} @mounts);
    $db = $db." (".(split /\s/,$fs)[0].")" if defined $fs;   
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
    my $version = shift;
    $version = $Staple::VERSION unless defined $version;
    return 0 if ($self->{error} = invalidDistribution($distribution));
    my $distributionPath = $self->getDistributionPath($distribution, 1);
    if (not -d "$distributionPath") {
        if ($self->mkdirs($distributionPath)) {
            if (open(VER, ">$distributionPath/version")) {
                print VER "$version\n";
                close(VER);
                return 1;
            } else {
                $self->{error} = "Can't set distribution version: $!\n";
                return undef;
            }
        } else {
            return undef;
        }
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
    $self->{error} = "Configuration \"$configuration\" already exists on distribution \"$distribution\"";
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
    if (index($conf, "common/") == 0) {
        $self->{error} = "Can't copy common configuration between distributions";
        return undef;
    }
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
    my $fromVersion = $self->getDistributionVersion($from);
    my $toVersion = $self->getDistributionVersion($to);
    my $fromvcmp = versionCompare($fromVersion, "004");
    my $tovcmp = versionCompare($toVersion, "004");
    if (($fromvcmp >= 0 and $tovcmp < 0) or
        ($fromvcmp < 0 and $tovcmp >= 0)) {
        $self->{error} = "Can't copy configurations (yet) from different distribution versions ($fromVersion to $toVersion)";
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
    my $node = shift;
    unless (-d $node->path()) {
        $self->{error} = $node->type()." ".$node->name()." does not exist";
        return undef;
    }

    # new xml style
    my $file = $node->path()."/tokens.xml";
    my @read = readTokensXMLFile($file);
    my %newTokens = ();
    %newTokens = @read if (@read > 1);
    @newTokens{keys %$tokens} = values %$tokens;
    unless (writeTokensXMLFile($file, \%newTokens)) {
        $self->{error} = "Can't write tokens file $file: $!\n";
        return undef;
    }

    # old tokens style, only before 004
    if (versionCompare($self->getVersionOf($node), "004") < 0) {
        return undef unless ($self->mkdirs($node->path()."/tokens"));
        foreach my $type ("static", "dynamic", "regexp") {
            my @tokens = map {$_->{key}} grep {$_->{type} eq $type} values %$tokens;
            next unless @tokens;
            my %newTokens = ();
            @newTokens{@tokens} = @{$tokens}{@tokens};
            my $file = $node->path()."/tokens/$type";
            my %oldTokens = readTokensFile($file, $type);
            @oldTokens{keys %newTokens} = values %newTokens;
            unless (writeTokensFile($file, \%oldTokens)) {
                $self->{error} = "Can't write tokens file $file: $!\n";
                return undef;
            }
        }
    }
    return 1;
}

sub removeTokens {
    my $self = shift;
    my $tokens = shift;
    my $node = shift;
    unless (-d $node->path()) {
        $self->{error} = $node->type()." ".$node->name()." does not exist";
        return undef;
    }

    # new xml style
    my $file = $node->path()."/tokens.xml";
    my @read = readTokensXMLFile($file);
    my %oldTokens = ();
    %oldTokens = @read if (@read > 1);
    if (delete @oldTokens{@$tokens}) {
        unless (writeTokensXMLFile($file, \%oldTokens)) {
            $self->{error} = "Can't write tokens to file $file: $!\n";
            return undef;
        }
    }

    # old tokens style, only before 004
    if (versionCompare($self->getVersionOf($node), "004") < 0) {
        foreach my $type ("static", "dynamic", "regexp") {
            my $file = $node->path()."/tokens/$type";
            my %oldTokens = readTokensFile($file);
            if (delete @oldTokens{@$tokens}) {
                unless (writeTokensFile($file, \%oldTokens)) {
                    $self->{error} = "Can't write tokens file $file: $!\n";
                    return undef;
                }
            }
        }
    }
    
    return 1;
}

sub setTokens {
    my $self = shift;
    my $tokens = shift;
    my $node = shift;

    unless (-d $node->path()) {
        $self->{error} = $node->type()." ".$node->name()." does not exist";
    }
    unlink $node->path()."/tokens.xml";

    # not sure if we still need to care about these
    if (versionCompare($self->getVersionOf($node), "004") < 0) {
        unlink map {$node->path()."/tokens/$_"} qw(static dynamic regexp)
    }

    return $self->addTokens($tokens, $node);
}

sub getTokens {
    return getTokensXML(@_);
    #return getTokensOLD(@_);
}

sub getTokensXML {
    my $self = shift;
    my @nodes = @_;
    my %tokens = ();
    my @tokenFiles = map {$_->path() ? $_->path()."/tokens.xml" : undef} @nodes;
    foreach my $node (@_) {
        my $tokenFile = shift @tokenFiles;
        next if not defined $tokenFile or not -r $tokenFile;
        my %currentTokens = readTokensXMLFile($tokenFile);
        my $prefix = $node->type();
        map {$_->{source} = "$prefix:".$node->name();} values %currentTokens;
        @tokens{keys %currentTokens} = values %currentTokens;
    }
    return \%tokens;
}

sub getTokensOLD {
    my $self = shift;
    my @tokenFiles = grep {$_} map {$_->path() ? $_->path()."/tokens" : undef} @_;
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
    my $groupsFile = $group->path()."/groups";
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
        my $path = $configuration->path()."/mounts";
        if (-r "$path") {
            open(FILE, "<$path");
            my @rawMounts = <FILE>;
            close(FILE);
            chomp @rawMounts;
            push @mounts, map {Staple::Mount->new({destination => "$_", configuration => $configuration})} @rawMounts;
        }
    }
    return @mounts;   
}

sub getTemplates {
    my $self = shift;
    my @configurations = @_;
    my %templates = ();
    my %links = ();
    foreach my $configuration (@configurations) {
        my $path = $configuration->path()."/templates";
        next unless -d "$path";
        my @raw = map {(my $a = $_) =~ s/$path//; $a} grep {! -d} getDirectoryList("$path");
        foreach my $rawTemplate (@raw) {
            if ($rawTemplate =~ m!^/(.*?)(/.*/?)([^/]*)$!) {
                (my $mode, my $uid, my $gid) = (lstat("$path/$rawTemplate"))[2,4,5];
                if (S_ISLNK($mode)) {
                    $links{"$1.$2$3"} = {source => readlink "$path$rawTemplate",
                                         destination => "$2$3",
                                         stage => "$1",
                                         configuration => $configuration,
                                         };
                } else {
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
    }
    my @templates =  (%templates ? Staple::Template->new(values %templates) : (),
                      %links ? Staple::Link->new(values %links) : ());
    return @templates;
}

sub addTemplates {
    my $self = shift;
    my @templates = @_;
    my @errors = ();
    foreach my $template (@templates) {
        unless ($template->destination()) {
            push @errors, "Template missing destination";
            next;
        }
        if ($template->type() eq "template") {
            if ($template->uid() !~ /^\d+$/) {
                my $uid;
                (undef, undef, $uid) = getpwnam($template->uid());
                unless (defined $uid) {
                    push @errors, "can't find uid of \"".$template->uid()."\"";
                    next;
                }
                $template->uid($uid);
            }
            if ($template->gid() !~ /^\d+$/) {
                my $gid;
                (undef, undef, $gid) = getgrnam($template->gid());
                unless (defined $gid) {
                    push @errors, "can't find gid of \"".$template->gid()."\"";
                    next;
                }
                $template->gid($gid);
            }
        } elsif ($template->type() eq "link") {
            my $version = $self->getVersionOf($template->configuration());
            # links only available after 006
            if (versionCompare($version, "006") <= 0) {
                push @errors, "Links only available after version 006 (this distribution version is $version)";
                next;
            }
        } else {
            push @errors, "Unknown template type for ".$template->destination().": ".$template->type();
            next;
        }
        my $path = $template->configuration()->path()."/templates/".$template->stage()."/".$template->destination();
        if (-e $path) {
            unless (unlink $path) {
                push @errors, "can't remove previous template \"$path\": $!";
                next;
            }
        }
        my $dir = $path;
        $dir =~ s,/[^/]*$,,;
        unless ($self->mkdirs($dir)) {
            push @errors, $self->error();
            next;
        }
        if ($template->type() eq "template") {
            my $data = $template->data();
            if ($template->error()) {
                push @errors, $template->error();
                next;
            }
            unless (open(FILE, ">$path")) {
                push @errors, "can't open \"$path\" for writing: $!";
                next;
            }
            print FILE $data;
            close(FILE);
            unless (chown $template->uid(), $template->gid(), $path) {
                push @errors, "can't chown \"$path\": $!";
                next;
            }
            unless (chmod($template->mode(), $path)) {
                push @errors, "can't chmod \"$path\": $!";
                next;
            }
        } elsif ($template->type() eq "link") {
            my $source = $template->source();
            if ($template->error()) {
                push @errors, $template->error();
                next;
            }
            unless (symlink $source, $path) {
                push @errors, "Can't create link $path -> $source: $!";
                next;
            }
        } else {
            push @errors, "Unknown template type for ".$template->destination().": ".$template->type()." (I've checked this already, this shouldn't be)";
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
        my $path = $template->configuration()->path()."/templates/".$template->stage()."/".$template->destination();
        unless (-e $path or ($template->type() eq "link" and -l $path)) {
            push @errors, "Can't remove ".$template->type()." \"".$template->destination()."\", it does not exist in the configuration \"".$template->configuration()->name()."\"";
            next;
        }
        unless (unlink $path) {
            push @errors, "Can't remove ".$template->type()." $path: $!";
            next;
        }
        # remove empty dirs
        rmdir for map {$template->configuration()->path()."/templates/".$template->stage()."/".$_} reverse ("", splitData($template->destination()));
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
        my $path = $configuration->path()."/scripts";
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
    return Staple::Script->new(@results) if @results;
    return ();
}

sub addScripts {
    my $self = shift;
    my @scripts = @_;
    my @errors = ();
    foreach my $script (@scripts) {
        my $data = $script->data();
        if ($script->error()) {
            push @errors, $script->error();
            next;
        }

        my @oldScripts = grep {$_->stage() eq $script->stage()} $self->getScripts($script->configuration());
        $script->order(scalar(@oldScripts) + 1) if not defined $script->order() or $script->order() > scalar(@oldScripts) or $script->order() < 1;
        unless($self->mkdirs($script->configuration()->path()."/scripts/".$script->stage()."/")) {
            push @errors, $self->{error};
            next;
        }
        unless ($self->openOrdering($script->order(), $script->configuration()->path()."/scripts/".$script->stage()."/")) {
            push @errors, $self->{error};
            next;
        }
        my $file = $script->configuration()->path()."/scripts/".$script->stage()."/".$script->order().".".($script->critical() ? "c" : "").($script->tokens() ? "t" : "").($script->tokenScript() ? "m" : "").".".$script->name();
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
    foreach my $script (sort {$b->order() <=> $a->order()} @scripts) {
        unless (unlink $script->source()) {
            push @errors, "Can't delete \"".$script->source()."\": $!";
            next;
        }
        (my $dir) = $script->source() =~ m,^(.*/)[^/]+$,;
        unless ($self->closeOrdering($script->order(), $dir)) {
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
        my $path = $configuration->path()."/autos";
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
        push @results, sort {$a->{order} <=> $b->{order}} @autos;
    }
    map {$_->{critical} = $_->{critical} ? 1 : 0} @results;
    map {$_->{tokens} = $_->{tokens} ? 1 : 0} @results;
    return Staple::Autogroup->new(@results) if @results;
    return ();
}


sub addAutos {
    my $self = shift;
    my @autos = @_;
    my @errors = ();
    foreach my $auto (@autos) {
        my $data = $auto->data();
        if ($auto->error()) {
            push @errors, $auto->error();
            next;
        }

        my @oldAutos = $self->getAutos($auto->configuration());
        $auto->order(scalar(@oldAutos) + 1) if not defined $auto->order() or $auto->order() > scalar(@oldAutos) or $auto->order() < 1;
        unless($self->mkdirs($auto->configuration()->path()."/autos/")) {
            push @errors, $self->{error};
            next;
        }
        unless ($self->openOrdering($auto->order(), $auto->configuration()->path()."/autos/")) {
            push @errors, $self->{error};
            next;
        }
        my $file = $auto->configuration()->path()."/autos/".$auto->order().".".($auto->critical() ? "c" : "").($auto->tokens() ? "t" : "").".".$auto->name();
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
    foreach my $auto (sort {$b->order() <=> $a->order()} @autos) {
        unless (unlink $auto->source()) {
            push @errors, "Can't delete \"".$auto->source()."\": $!";
            next;
        }
        (my $dir) = $auto->source() =~ m,^(.*/)[^/]+$,;
        unless ($self->closeOrdering($auto->order(), $dir)) {
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
    @mounts = grep {$_->description() ne $mount->description()} @mounts;
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
    my @configurations = map {$_->configuration()->name().":".$_->configuration()->dist()} @allMounts;
    my %configurations = ();
    @configurations{@configurations} = @configurations;
    foreach my $conf (keys %configurations) {
        my @mounts = grep {$_->configuration()->name().":".$_->configuration()->dist() eq $conf} @allMounts;
        my @oldMounts = $self->getMounts($mounts[0]->configuration());
        my @results = ();
        foreach my $mount (@oldMounts) {
            push @results, $mount unless grep {$mount->description() eq $_->description()} @mounts;
        }
        unless ($self->setMounts($mounts[0]->configuration(), @results)) {
            push @errors, $self->{error};
        }
    }
    if (@errors) {
        $self->{error} = join "\n", @errors;
        return undef;
    }
    return 1;
}

sub removeConfigurationConfigurations {
    my $self = shift;
    my $conf = shift;
    my @confs = @_;
    my $version = $self->getDistributionVersion($conf->dist());
    if (versionCompare($version, "004") < 0) {
        $self->{error} = "distribution \"".$conf->dist()."\" is version $version (needs at least 004)";
        return undef;
    }
    $self->removeGroupConfigurations($conf, @confs);
}

sub addConfigurationConfiguration {
    my $self = shift;
    my $conf1 = shift;
    my $conf2 = shift;
    my $location = shift;
    my $version = $self->getDistributionVersion($conf1->dist());
    if (versionCompare($version, "004") < 0) {
        $self->{error} = "distribution \"".$conf1->dist()."\" is version $version (needs at least 004)";
        return undef;
    }
    unless ($self->getFullConfigurations([$conf2], $conf1->dist())) {
        $self->{error} = "distribution ".$conf1->dist()." doesn't have ".$conf2->name();
        return undef;
    }
    $self->addGroupConfiguration($conf1, $conf2, $location);
}

sub getConfigurationConfigurations {
    my $self = shift;
    my $conf = shift;
    my $version = $self->getDistributionVersion($conf->dist());
    if (versionCompare($version, "004") < 0) {
        $self->{error} = "distribution \"".$conf->dist()."\" is version $version (needs at least 004)";
        return undef;
    }
    return $self->getGroupConfigurations($conf);
}

# applies also to getConfigurationConfigurations
sub getGroupConfigurations {
    my $self = shift;
    my $group = shift;
    my @configurations = ();
    if (-r $group->path()."/configurations") {
        my @configurationData = ();
        if (open(FILE, "<".$group->path()."/configurations")) {
            @configurationData = <FILE>;
            close(FILE);
        } else {
            $self->{error} = "failed to open ".$group->path()."/configurations: $!";
            return undef;
        }
        push @configurations, map {my $a = $_; chomp $a; $a} @configurationData;
        @configurations = map {m/^([+-])(.*)$/; Staple::Configuration->new({name => $2, active => $1, group => $group})} @configurations;
        #map {if ($_->{active} eq '+') {$_->{active} = 1} else {$_->{active} = 0}} @configurations;
    }
    return @configurations;
}

# applies also to addConfigurationConfiguration
sub addGroupConfiguration {
    my $self = shift;
    my $group = shift;
    my $configuration = shift;
    my $location = shift;
    $location = int $location if $location;
    my @results;
    my @configurations = $self->getGroupConfigurations($group);
    @configurations = grep {$_->name() ne $configuration->name() or $_->active() ne $configuration->active()} @configurations;
    my $i = 0;
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

# applies also to removeConfigurationConfigurations
sub removeGroupConfigurations {
    my $self = shift;
    my $node = shift;
    my @toRemove = map {($_->active() ? "+" : "-").$_->name()} @_;
    my %toRemove = ();
    @toRemove{@toRemove} = @toRemove;
    my @configurations = $self->getGroupConfigurations($node);
    my @results = ();
    foreach my $configuration (@configurations) {
        push @results, $configuration unless $toRemove{($configuration->active() ? "+" : "-").$configuration->name()};
    }
    return $self->setGroupConfigurations($node, @results);
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
    foreach my $toremove (keys %toRemove) {
        unless (grep {$_ eq $toremove} @groups) {
            $self->{error} = "group \"$toremove\" not in group \"".$group->name()."\"";
            return undef;
        }
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
    my $cmd = "find ".$self->{stapleDir}."/hosts/ -type f -name groups -print0 | xargs -0 egrep -l '^".$group.$suffix."'";
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
    $cmd = "find ".$self->{stapleDir}."/distributions/ -type f -name groups -print0 | xargs -0 egrep -l '^".$group.$suffix."'";
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
    $cmd = "find ".$self->{stapleDir}."/groups/ -type f -name groups -print0 | xargs -0 egrep -l '^".$group.$suffix."'";
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

sub getAllConfigurations {
    my $self = shift;
    my $distribution = shift;
    my $path = $self->getConfigurationPath("/", $distribution);
    my @configurations = ();
    if ($path) {
        @configurations = getDirectoryList($path);
        @configurations = grep {-d $_ } @configurations;
        @configurations = grep { s/^$path//; $_} @configurations;
    }
    my $version = $self->getDistributionVersion($distribution);
    if (versionCompare($version, "004") < 0) {
        @configurations = grep { m!^/[^/]+$! or m!configurations/[^/]+$! } @configurations;
        @configurations = map { s!/configurations/!/!g; $_ } @configurations;
    } else {
        # get common configurations
        if ($path = $self->getConfigurationPath("common/", $distribution)) {
            my @cconfigurations = getDirectoryList($path);
            @cconfigurations = grep {-d $_ } @cconfigurations;
            @cconfigurations = grep { s/^$path/common\/subconfs/; $_} @cconfigurations;
            push @configurations, @cconfigurations;
        }
        @configurations = grep { m!^/[^/]+$! or m!subconfs/[^/]+$! } @configurations;
        @configurations = map { s!/subconfs/!/!g; $_ } @configurations;
    }
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

sub getDistributionVersion {
    my $self = shift;
    my $dist = shift;
    my $path = $self->getDistributionPath($dist);
    if ($path) {
        if (open(VER, "$path/version")) {
            my $version = <VER>;
            close(VER);
            chomp($version);
            return $version;
        } else {
            return "none"
        }
    }
    $self->{error} = "no such distribution \"$dist\"";
    return undef;
}

sub setDistributionVersion {
    my $self = shift;
    my $dist = shift;
    my $ver = shift;
    $ver = "none" unless defined $ver;
    my $path = $self->getDistributionPath($dist);
    if (versionCompare($ver, $Staple::VERSION) > 0) {
        $self->{error} = "Don't know version $ver, max version is $Staple::VERSION";
        return undef;
    }
    if ($path) {
        my $old = $self->getDistributionVersion($dist);
        return $old if $old eq $ver;
        if (versionCompare($old, $ver) > 0) {
            $self->{error} = "Can't change to older configuration ($ver from $old)";
            return undef;
        }
        my @configurations = $self->getFullConfigurations([$self->getAllConfigurations($dist)], $dist);
        if (open(VER, ">$path/version")) {
            print VER "$ver\n";
            close(VER);
            # 004: remove old tokens, rename configuration trees
            if (versionCompare($ver, "004") >= 0 and
                versionCompare($old, "004") < 0) {
                # let's hope there's no configuration named tokens...
                `find \`find $path -name tokens -type d\` -maxdepth 1 \\( -name static -o -name dynamic -o -name regexp \\) -delete`;
                `find $path -name tokens -type d -delete`;
                foreach my $conf (sort {length($b) <=> length($a)} map {$_->path()} @configurations) {
                    if (-d "$conf/configurations") {
                        rename "$conf/configurations", "$conf/subconfs";
                    }
                }
                rename "$path/confs", "$path/subconfs";
            }
            return $old;
        }
        $self->{error} = "Can't open $path/version for writing: $!";
        return undef;
    }
    $self->{error} = "no such distribution \"$dist\"";
    return undef;
}

sub getConfigurationPath {
    my $self = shift;
    my $configuration = shift;
    my $distribution = shift;
    my $force = shift;
    my $common = index($configuration, "common") == 0;
    my $path = $self->getDistributionPath($distribution,$force);
    $path = $self->getCommonPath() if $common;
    return undef unless $path;
    my $version = $self->getDistributionVersion($distribution);
    if ($common) {
        $version = $self->getMinimumDistributionVersion();
        # common configurations only from 004
        $version = "004" if (versionCompare($version, "004") < 0);
    }
    return undef unless $version;
    if (versionCompare($version, "004") < 0) {
        $configuration =~ s!^/!!;
        $configuration =~ s!/!/configurations/!g;
        $configuration = fixPath("$path/confs/${configuration}");
    } else {
        $configuration =~ s/^common\/?// if $common;
        $configuration =~ s!^/!!;
        $configuration =~ s!/!/subconfs/!g;
        $configuration = fixPath("$path/subconfs/${configuration}");
    }
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

sub getCommonPath {
    my $self = shift;
    return fixPath("$self->{stapleDir}/common");
}

sub getHostPath {
    my $self = shift;
    my $host = shift;
    my $force = shift;
    my $path = fixPath("$self->{stapleDir}/hosts/$host");
    return $path if -d $path or $force;
    return undef;
}

sub getStapleDir {
    my $self = shift;
    return $self->{stapleDir};
}

sub getNote {
    my $self = shift;
    my $node = shift;
    my $note = "";
    $self->{error} = "";
    if (ref $node and $node->path() and -e $node->path()."/note") {
        if (open(NOTE, "<".$node->path()."/note")) {
            $note = join "", <NOTE>;
            close(NOTE);
        } else {
            $self->{error} = "note is not readable";
            $note = undef;
        }
    }
    return $note;
}

sub setNote {
    my $self = shift;
    my $node = shift;
    my $note = shift;
    if (defined $note and $note ne "") {
        if (defined $node->path() and -d $node->path()) {
            if (open(NOTE, ">".$node->path()."/note")) {
                print NOTE $note;
                close(NOTE);
            } else {
                $self->{error} = "Can't save note: $!\n";
                return undef;
            }
        } else {
            $self->{error} = "Don't know ".$node->type()." path";
            return undef;
        }
    } else {
        if (-e $node->path()."/note") {
            unless (unlink $node->path()."/note") {
                $self->{error} = "Can't delete note: $!\n";
                return undef;
            }
        }
    }
    return 1;
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
        chmod 0755, $dir;
    }
    return 1;
}

# input: configuration, list of mounts
# output: 1 or undef
# writes the mounts file
sub setMounts {
    my $self = shift;
    my $configuration = shift;
    my @mounts = @_;
    my $mountsFile = $configuration->path()."/mounts";
    if (@mounts) {
        if (open(FILE, ">$mountsFile")) {
            print FILE join "\n", map {$_->description()} @mounts;
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
    my $groupsFile = $group->path()."/groups";
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

# input: Staple::Node, list of configuration (name + active)
# output: 1 or undef
# writes the groups file
sub setGroupConfigurations {
    my $self = shift;
    my $node = shift;
    my @configurations = @_;
    my $configurationsFile = $node->path()."/configurations";
    if (@configurations) {
        if (open(FILE, ">$configurationsFile")) {
            print FILE join "\n", map {($_->active() ?  "+" : "-").$_->name()} @configurations;
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
#   The end
################################################################################

1;

__END__

=back

=head1 SEE ALSO

L<Staple> - Staple main module.

L<Staple::DB> - the DB interface

L<Staple::DB::SQL> - SQL - Database

=head1 AUTHOR

Yair Yarom, E<lt>irush@cs.huji.ac.ilE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2007-2011 Hebrew University Of Jerusalem, Israel
See the LICENSE file.

=cut
