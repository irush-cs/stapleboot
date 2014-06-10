package Staple::DB::FSQL;

#
# Copyright (C) 2007-2011 Hebrew University Of Jerusalem, Israel
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
use Staple::DBFactory;
use Fcntl;
use Staple::Script;
use Staple::Template;

no if $] >= 5.018 && $] < 5.019, warnings => 'experimental::smartmatch';

our @ISA = ("Staple::DB::SQL");
our $VERSION = '0.2.x';

=head1 NAME

  Staple::DB::FSQL - API for filesystem as database connection, and sql as metadata

=cut

################################################################################
#   Exported
################################################################################

=head1 DESCRIPTION

=over

=item B<new(path, params, schema [,username, password])>

creates a new instance, the path is the staple direcotry. params, schema
username and passwords are for the sql database which defaults to
dbi:SQLite:<path>/metadata.sqlite3.

=cut

sub new {
    my $proto = shift;
    my $path = shift;
    my $class = ref($proto) || $proto;
    my $self = {};
    bless ($self, $class);

    return createDB("error", "Missing database path") unless $path;
    
    $self->{error} = "";
    $self->{stapleDir} = $path;
    $self->setTmpDir("$path/tmp");
    $self->{sqlparams} = [@_];
    $self->{sqlparams}[0] ||= "dbi:SQLite:$path/metadata.sqlite3";
    $self = $self->SUPER::_init(@{$self->{sqlparams}});
    $self->{saveData} = 0;
    $self->{checkData} = 0;
    return $self;
}

=item B<new(path, params, schema [,username, password])>

Creates a new instance and build the directory tree if needed, same parameters
as new().

=cut

sub create {
    my $proto = shift;
    my $path = shift;
    my $class = ref($proto) || $proto;
    my $self = {};
    bless ($self, $class);

    return createDB("error", "Missing database path") unless $path;
    
    $self->{error} = "";
    $self->{stapleDir} = $path;
    $self->setTmpDir("$path/tmp");
    $self->{sqlparams} = [@_];
    $self->{sqlparams}[0] ||= "dbi:SQLite:$path/metadata.sqlite3";
    
    mkpath($self->{tmpDir});
    unless (-d $self->{stapleDir} and
            -d $self->{tmpDir}) {
        return createDB("error", "Can't create paths: $!\n");
    }
    unless (open(TEMP, ">$self->{tmpDir}/tmp-is-not-mounted")) {
        return createDB("error", "Can't create file: $!\n");
    }
    close(TEMP);

    $self->SUPER::_init(@{$self->{sqlparams}});
    return $self if $self->{error};
    return $self->SUPER::_buildDB();
}

sub describe {
    return ("Filesystem database with sql for metadata",
            "Receives 5 parameters (for fs + sql):
      1. Directory path
      2. The database connection parameters, as given to DBI->connect perl
         function. Defaults to 'dbi:SQLite:<path>/metadata.sqlite3'
      3. The database schema to use. \"undef\" or undef to ignore.
      4. username (optional)
      5. password (optional)");
}

sub info {
    my $self = shift;
    my $db = "fsql $self->{stapleDir}";
    my $sqlinfo = $self->SUPER::info();
    $sqlinfo =~ s/^sql //;
    if ($sqlinfo !~ m,^\s*dbi:SQLite:$self->{stapleDir}/metadata.sqlite3\s*$,) {
        $db .= " $sqlinfo";
    }
    if (-r "/proc/mounts" and open(PROC, "/proc/mounts")) {
        my @mounts = <PROC>;
        close(PROC);
        (my $fs) = (grep {(split /\s/,$_)[1] eq $self->{stapleDir}} @mounts);
        $db = $db." (".(split /\s/,$fs)[0].")" if defined $fs;   
    }
    return $db;
}

sub removeDistribution {
    my $self = shift;
    my $distribution = shift;
    return undef unless $self->SUPER::removeDistribution($distribution);
    rmtree($self->getDistributionPathFS($distribution, 1));
    return 1;
}

sub removeConfiguration {
    my $self = shift;
    my $distribution = shift;
    my $configuration = shift;
    return undef if ($self->{error} = invalidDistribution($distribution));
    return undef if ($self->{error} = invalidConfiguration($configuration));
    rmtree($self->getConfigurationPathFS($configuration, $distribution, 1));
    return $self->SUPER::removeConfiguration($distribution, $configuration);
}

sub copyConfiguration {
    my $self = shift;
    return $self->Staple::DB::copyConfiguration(@_);
}

sub getTemplates {
    my $self = shift;
    $self->{error} = "";
    my @templates = $self->SUPER::getTemplates(@_);
    return undef if $self->{error};
    map {$_->source(fixPath($self->{stapleDir}."/".$_->source()))} @templates;
    return @templates;
}

sub addTemplates {
    my $self = shift;
    # copy template, because going to change them (source, etc.)
    my @templates = @_ ? Staple::Template->new(@_) : ();
    my @errors = ();

    foreach my $template (@templates) {

        # delete previous template (to delete the file)
        $self->removeTemplates($template);

        # find a new name and write it
        my $name = $template->destination();
        $name =~ s,^.*/([^/]*)$,$1,;
        my $path = $self->getConfigurationPathFS($template->configuration()->name(), $template->configuration()->dist(), 1);
        mkpath($path);
        $name = $self->findNextName($path, $name);
        unless (defined $name) {
            push @errors, $self->{error};
            next;
        }
        $path = fixPath("$path/$name");
        unless ($template->writeSource("$path")) {
            unlink "$path";
            push @errors, $template->{error};
            next;
        }

        # add to metadata
        $path =~ s/^$self->{stapleDir}//;
        $template->source($path);
        unless ($self->SUPER::addTemplates($template)) {
            unlink "$path";
            push @errors, $self->{error};
            next;
        }
    }
    if (@errors) {
        chomp @errors;
        $self->{error} = join "\n", @errors;
        return undef;
    }
    return 1;
}

sub removeTemplates {
    my $self = shift;
    my @templates = @_ ? Staple::Template->new(@_) : ();
    my @errors = ();
    foreach my $template (@templates) {
        $self->{error} = "";
        (my $file) = $self->getList("SELECT source FROM $self->{schema}templates WHERE destination = ? AND configuration = ? AND distribution = ? AND stage = ?", $template->destination(), $template->configuration()->name(), $template->configuration()->dist(), $template->stage());
        unless ($file) {
            $self->{error} = "Error, can't find template ".$template->configuration()->dist()."/".$template->configuration()->name()."/".$template->stage()."/".$template->destination()  unless $self->{error};
            push @errors, $self->{error};
            next;
        }

        $file = fixPath("$self->{stapleDir}/$file");
        unless (-e $file) { 
            push @errors, "Can't remove template \"".$template->destination()."\", it does not exist in the configuration \"".$template->configuration()->name()."\" (for distribution: ".$template->configuration()->dist().")";
            next;
        }
        unless ($self->SUPER::removeTemplates($template)) {
            push @errors, $self->{error};
            next;
        }
        unless (unlink $file) {
            push @errors, "Can't delete template $file: $!";
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
    $self->{error} = "";
    my @scripts = $self->SUPER::getScripts(@_);
    return undef if $self->{error};
    map {$_->source(fixPath($self->{stapleDir}."/".$_->source()))} @scripts;
    return @scripts;
}

sub addScripts {
    my $self = shift;
    # copy scripts, because going to change them (source, etc.)
    my @scripts = @_ ? Staple::Script->new(@_) : ();
    my @errors = ();

    foreach my $script (@scripts) {

        # find a new name and write it
        my $name = $script->name();
        my $path = $self->getConfigurationPathFS($script->configuration()->name(), $script->configuration()->dist(), 1);
        mkpath($path);
        $name = $self->findNextName($path, $name);
        unless (defined $name) {
            push @errors, $self->{error};
            next;
        }
        $path = fixPath("$path/$name");
        unless ($script->writeSource("$path")) {
            unlink "$path";
            push @errors, $script->error();
            next;
        }

        # add to metadata
        $path =~ s/^$self->{stapleDir}//;
        $script->source($path);
        unless ($self->SUPER::addScripts($script)) {
            unlink "$path";
            push @errors, $self->{error};
            next;
        }
    }
    if (@errors) {
        chomp @errors;
        $self->{error} = join "\n", @errors;
        return undef;
    }
    return 1;
}

sub removeScripts {
    my $self = shift;
    my @scripts = @_ ? Staple::Script->new(@_) : ();
    my @errors = ();
    foreach my $script (@scripts) {
        $self->{error} = "";
        (my $file) = $self->getList("SELECT source FROM $self->{schema}scripts WHERE name = ? AND configuration = ? AND distribution = ? AND stage = ? AND ordering = ?", $script->name(), $script->configuration()->name(), $script->configuration()->dist(), $script->stage(), $script->order());
        unless ($file) {
            $self->{error} = "Error, can't find script ".$script->configuration()->dist()."/".$script->configuration()->name()."/".$script->stage()."/".$script->name() unless $self->{error};
            push @errors, $self->{error};
            next;
        }

        $file = fixPath("$self->{stapleDir}/$file");
        unless (-e $file) { 
            push @errors, "Script \"".$script->name().\" does not exist in the configuration \"".$script->configuration()->name()."\"";
            next;
        }
        unless ($self->SUPER::removeScripts($script)) {
            push @errors, $self->{error};
            next;
        }
        unless (unlink $file) {
            push @errors, "Can't delete script file $file: $!";
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
    $self->{error} = "";
    my @autos = $self->SUPER::getAutos(@_);
    return undef if $self->{error};
    map {$_->source(fixPath($self->{stapleDir}."/".$_->source()))} @autos;
    return @autos;
}

sub addAutos {
    my $self = shift;
    # copy autos, because going to change them (source, etc.)
    my @autos = @_ ? Staple::Autogroup->new(@_) : ();
    my @errors = ();

    foreach my $auto (@autos) {

        # find a new name and write it
        my $name = $auto->name();
        my $path = $self->getConfigurationPathFS($auto->configuration()->name(), $auto->configuration()->dist(), 1);
        mkpath($path);
        $name = $self->findNextName($path, $name);
        unless (defined $name) {
            push @errors, $self->{error};
            next;
        }
        $path = fixPath("$path/$name");
        unless ($auto->writeSource("$path")) {
            unlink "$path";
            push @errors, $auto->error();
            next;
        }

        # add to metadata
        $path =~ s/^$self->{stapleDir}//;
        $auto->source($path);
        unless ($self->SUPER::addAutos($auto)) {
            unlink "$path";
            push @errors, $self->{error};
            next;
        }
    }
    if (@errors) {
        chomp @errors;
        $self->{error} = join "\n", @errors;
        return undef;
    }
    return 1;
}

sub removeAutos {
    my $self = shift;
    my @autos = @_ ? Staple::Autogroup->new(@_) : ();
    my @errors = ();
    foreach my $auto (@autos) {
        $self->{error} = "";
        (my $file) = $self->getList("SELECT source FROM $self->{schema}autos WHERE name = ? AND configuration = ? AND distribution = ? AND ordering = ?", $auto->name(), $auto->configuration()->name(), $auto->configuration()->dist(), $auto->order());
        unless ($file) {
            $self->{error} = "Error, can't find auto" unless $self->{error};
            push @errors, $self->{error};
            next;
        }

        $file = fixPath("$self->{stapleDir}/$file");
        unless (-e $file) { 
            push @errors, "Auto \"".$auto->name().\" does not exist in the configuration \"".$auto->configuration()->name()."\"";
            next;
        }
        unless ($self->SUPER::removeAutos($auto)) {
            push @errors, $self->{error};
            next;
        }
        unless (unlink $file) {
            push @errors, "Can't delete auto file $file: $!";
            next;
        }
    }
    if (@errors) {
        $self->{error} = join "\n", @errors;
        return undef;
    }
    return 1;
}

sub getStapleDir {
    my $self = shift;
    return $self->{stapleDir};
}

################################################################################
#   Internals
################################################################################

# don't override the SQL::getConfigurationPath, as it is (might be?) used by
# other methods
# input: (self), conf name, dist name, force
# output: full path (undef if not force and not exists)
sub getConfigurationPathFS {
    my $self = shift;
    my $configuration = shift;
    my $distribution = shift;
    my $force = shift;
    my $common = index($configuration, "common") == 0;
    my $path = $self->getDistributionPathFS($distribution,$force);
    
    $path = $self->getCommonPathFS() if $common;
    return undef unless $path;
    $configuration =~ s/^common\///;
    $path = fixPath("$path/$configuration");
    
    return $path if -d $path or $force;
    return undef;
}

# don't override the SQL::getDistributionPath, as it is (might be?) used by
# other methods
# input: (self), dist name, force
# output: full path (undef if not exists and not force)
sub getDistributionPathFS {
    my $self = shift;
    my $distribution = shift;
    my $force = shift;
    my $path = fixPath("$self->{stapleDir}/distributions/$distribution");
    return $path if -d $path or $force;
    return undef;
}

# don't override the SQL::getCommonPath, as it is (might be?) used by
# other methods
# input: (self)
# output: full path
sub getCommonPathFS {
    my $self = shift;
    return fixPath("$self->{stapleDir}/common");
}

# creates an empty new file for name in path, on error returns undef
# input: (self), path, name
# output: new unique name
sub findNextName {
    my $self = shift;
    my $path = shift;
    my $name = shift;
    unless (opendir(DIR, $path)) {
        $self->{error} = "Can't open $path: $!";
        return undef;
    }
    my @prefixes = readdir(DIR);
    @prefixes = grep /^\d+\.$name$/, @prefixes;
    close(DIR);
    if (@prefixes) {
        @prefixes = sort {$a <=> $b} map {m/^(\d+)\./; $1} @prefixes;
        my @all = ($prefixes[0] .. $prefixes[-1]);
        my $check = $prefixes[-1];
        foreach my $check (@all) {
            next if $check ~~ @prefixes;
            $check--;
            last;
        }
        $check++;
        $name = "$check.$name";
    } else {
        $name = "1.$name";
    }
    unless (sysopen(FH, "$path/$name", O_WRONLY | O_EXCL | O_CREAT)) {
        $self->{error} = "Error creating unique $path/$name: $!";
        return undef;
    }
    close(FH);
    return $name;
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

L<Staple::DB::FS> - Filesyste Database

=head1 AUTHOR

Yair Yarom, E<lt>irush@cs.huji.ac.ilE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2007-2011 Hebrew University Of Jerusalem, Israel
See the LICENSE file.

=cut
