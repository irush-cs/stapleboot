package Staple::DB::SQL;

#
# Copyright (C) 2007-2011 Hebrew University Of Jerusalem, Israel
# See the LICENSE file.
#
# Author: Yair Yarom <irush@cs.huji.ac.il>
#

use strict;
use warnings;
use Staple::DB;
use Staple::Misc;
use DBI;
use Staple::DBFactory;
use Staple::DB::SQL::Init;
use Staple::Template;
use Staple::Script;
use Staple::Autogroup;

our @ISA = ("Staple::DB");
our $VERSION = '007snap';

=head1 NAME

  Staple::DB::SQL - API for SQL DB as database connection

=cut

################################################################################
#   Exported
################################################################################

=head1 DESCRIPTION

=over

=itme B<new(params, schema [,username, password])>

creates a new instance. First parameter is the database parameters (defaults to
dbi:SQLite:/tmp/staple.sqlite3). Second is the schema name string. Third and
forth parameters are username and password (can be undef).

=cut

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = {};
    bless ($self, $class);
    return $self->_init(@_);
}

sub create {
    my $self = new(@_);
    return $self if $self->{error};
    return $self->_buildDB();
}

sub describe {
    return ("SQL database",
            "receives 4 parameters:
      1. The database connection parameters, as given to DBI->connect perl
         function. Defaults to 'dbi:SQLite:/tmp/staple.sqlite3'
      2. The database schema to use. \"undef\" or undef to ignore.
      3. username (optional)
      4. password (optional)");
}

sub info {
    my $self = shift;
    my $schema = $self->{schema};
    $schema =~ s/\.$//;
    my $username = $self->{connectionParams}->[1];
    $username = "" unless $username;
    return "sql $self->{connectionParams}->[0] $schema $username"
}

sub addHost {
    my $self = shift;
    my $host = shift;
    return 0 if ($self->{error} = invalidHost($host));
    return $self->insert("$self->{schema}hosts(host)", "$host");
}

sub addGroup {
    my $self = shift;
    my $group = shift;
    return 0 if ($self->{error} = invalidGroup($group));
    $group = fixPath($group);
    $group =~ s/\/$//;
    if ($self->count("SELECT COUNT(name) FROM $self->{schema}groups WHERE name = ? AND type = 'group'", $group)) {
        $self->{error} = "Group exists";
        return undef;
    };
    foreach my $subgroup (splitData($group)) {
        unless ($self->count("SELECT COUNT(name) FROM $self->{schema}groups WHERE name = ? AND type = 'group'", $subgroup)) {
            unless ($self->insert("$self->{schema}groups(name, type)", "$subgroup", "group")) {
                return undef;
            }
        }
    }
    return 1;
}

sub addConfiguration {
    my $self = shift;
    my $distribution = shift;
    my $configuration = shift;
    my @distributions;
    my $common = 0;
    return undef if ($self->{error} = invalidDistribution($distribution));
    return undef if ($self->{error} = invalidConfiguration($configuration));
    if (index($configuration, "common/") == 0) {
        $distribution = $self->getCommonPath();
        @distributions = grep {versionCompare($self->getDistributionVersion($_), "005") > 0} $self->getAllDistributions();
        $common = 1;
    }
    $configuration = fixPath($configuration);
    $configuration =~ s/\/$//;
    if ($self->count("SELECT COUNT(name) FROM $self->{schema}configurations WHERE name = ? AND distribution = ?", $configuration, $distribution)) {
        $self->{error} = "Configuration exists";
        return undef;
    };
    foreach my $subconf (splitData($configuration)) {
        next if ($subconf eq "common");
        foreach my $dist ($distribution, @distributions) {
            unless ($self->count("SELECT COUNT(name) FROM $self->{schema}configurations WHERE name = ? AND distribution = ?", $subconf, $dist)) {
                unless ($self->insert("$self->{schema}configurations(name, distribution)", "$subconf", "$dist")) {
                    return undef;
                }
            }
        }
    }
    return 1;
}

sub addDistribution {
    my $self = shift;
    my $distribution = shift;
    my $version = shift;
    $version = $Staple::VERSION unless defined $version;
    return 0 if ($self->{error} = invalidDistribution($distribution));
    return 0 unless $self->insert("$self->{schema}distributions(name, version)", "$distribution", "$version");
    my @confs = $self->getAllConfigurations($distribution);
    # must/should be just common configurations, but still, add only above 005
    if (versionCompare($version, "005") > 0) {
        foreach my $conf (@confs) {
            unless ($self->insert("$self->{schema}configurations(name, distribution)", "$conf", "$distribution")) {
                return undef;
            }
        }
    }
    return 1;
}

sub removeHost {
    my $self = shift;
    my $host = shift;
    return 0 if ($self->{error} = invalidHost($host));
    my $dbh = $self->{dbh};
    my $sth = $dbh->prepare_cached("DELETE FROM $self->{schema}hosts WHERE host = ?");
    my $rv = $sth->execute("$host");
    if ($dbh->errstr) {
        $self->{error} = $dbh->errstr;
        return 0;
    } elsif ($rv == 0) {
        $self->{error} = "Host \"$host\" does not exist";
        return 0;
    }
    return 1;
}

sub removeGroup {
    my $self = shift;
    my $group = shift;
    return undef if ($self->{error} = invalidGroup($group));
    unless ($self->count("SELECT COUNT(name) FROM $self->{schema}groups WHERE name = ? AND type = 'group'", $group)) {
        $self->{error} = "Group does not exists";
        return undef;
    };
    my $dbh = $self->{dbh};
    my $sth = $dbh->prepare_cached("SELECT name FROM $self->{schema}groups WHERE name LIKE ? AND type = 'group'");
    unless ($sth->execute("$group/%")) {
        $self->{error} = $sth->errstr;
        $sth->finish();
        return undef;
    }
    my $groups = $sth->fetchall_hashref("name");
    if ($sth->err) {
        $self->{error} = $sth->errstr;
        return undef;
    }
    $sth = $dbh->prepare_cached("DELETE FROM $self->{schema}groups WHERE name = ? AND type = 'group'");
    foreach my $subgroup (keys %$groups) {
        my $rv = $sth->execute("$subgroup");
        if ($dbh->errstr) {
            $self->{error} = $dbh->errstr;
            return undef;
        }
    }
    $sth->execute($group);
    return 1;
}

sub removeConfiguration {
    my $self = shift;
    my $distribution = shift;
    my $configuration = shift;
    my $common = 0;
    return undef if ($self->{error} = invalidDistribution($distribution));
    return undef if ($self->{error} = invalidConfiguration($configuration));
    if (index($configuration, "common/") == 0) {
        $common = 1;
        $distribution = $self->getCommonPath();
    }
    $configuration = fixPath($configuration);
    $configuration =~ s/\/$//;
    unless ($self->count("SELECT COUNT(name) FROM $self->{schema}configurations WHERE name = ? AND distribution = ?", $configuration, $distribution)) {
        $self->{error} = "Configuration does not exists";
        return undef;
    };
    my $dbh = $self->{dbh};
    my $sth = $dbh->prepare_cached($common ?
                                   "DELETE FROM $self->{schema}configurations WHERE name LIKE ?" :
                                   "DELETE FROM $self->{schema}configurations WHERE name LIKE ? AND distribution = ?"
                                  );
    my $like = $configuration;
    $like =~ s/_/\\_/g;
    $like =~ s/%/\\%/g;
    # remove subconfs
    my $rv = $sth->execute($common ? ("$like/%") : ("$like/%", $distribution));
    if ($dbh->errstr) {
        $self->{error} = "Error removing $configuration: ".$dbh->errstr;
        return undef;
    }
    # remove conf itself
    $sth->execute($common ? ($like) : ($like, $distribution));
    if ($dbh->errstr) {
        $self->{error} = "Error removing $configuration: ".$dbh->errstr;
        return undef;
    }
    return 1;
}

sub copyConfiguration {
    my $self = shift;
    my $conf = shift;
    my $from = shift;
    my $to = shift;
    my @errors = ();

    if (index($conf, "common/") == 0) {
        $self->{error} = "Can't copy common configuration between distributions";
        return undef;
    }
    unless ($self->count("SELECT COUNT(name) FROM $self->{schema}configurations WHERE name = ? AND distribution = ?", $conf, $from)) {
        $self->{error} = "Configuration \"$conf\" doesn't exist";
        return undef;
    }
    unless ($self->count("SELECT COUNT(name) FROM $self->{schema}distributions WHERE name = ?", $from)) {
        $self->{error} = "Distribution \"$from\" doesn't exist";
        return undef;        
    }
    unless ($self->count("SELECT COUNT(name) FROM $self->{schema}distributions WHERE name = ?", $to)) {
        $self->{error} = "Distribution \"$to\" doesn't exist";
        return undef;        
    }
    if ($self->getConfigurationPath($conf, $to) and not $self->removeConfiguration($to, $conf)) {
        return undef;
    }

    my $fromVersion = $self->getDistributionVersion($from);
    my $toVersion = $self->getDistributionVersion($to);
    
    # configurations
    my $sqlstring = "INSERT INTO $self->{schema}configurations SELECT name, '$to' AS distribution, comment FROM $self->{schema}configurations WHERE distribution = ? AND ( name LIKE ? OR name = ?)";
    my $dbh = $self->{dbh};
    my $sth = $dbh->prepare_cached($sqlstring);
    $sth->execute("$from", "$conf/%", "$conf");
    if ($dbh->errstr) {
        $self->{error} = $sqlstring.$dbh->errstr;
        return undef;
    }

    # templates
    $sqlstring = "INSERT INTO $self->{schema}templates (destination, configuration, distribution, source, data, comment, stage, mode, gid, uid)  SELECT destination, configuration, '$to' AS distribution, source, data, comment, stage, mode, gid, uid FROM $self->{schema}templates WHERE distribution = ? AND (configuration LIKE ? OR configuration = ?)";
    $sth = $dbh->prepare_cached($sqlstring);
    $sth->execute("$from", "$conf/%", "$conf");
    if ($dbh->errstr) {
        push @errors, $sqlstring.$dbh->errstr;
    }
    
    # scripts
    $sqlstring = "INSERT INTO $self->{schema}scripts (name, source, data, configuration, distribution, stage, ordering, critical, tokens, comment, tokenscript) SELECT name, source, data, configuration, '$to' AS distribution, stage, ordering, critical, tokens, comment, tokenscript  FROM $self->{schema}scripts WHERE distribution = ? AND (configuration LIKE ? OR configuration = ?)";
    $sth = $dbh->prepare_cached($sqlstring);
    $sth->execute("$from", "$conf/%", "$conf");
    if ($dbh->errstr) {
        push @errors, $sqlstring.$dbh->errstr;
    }

    # auto
    $sqlstring = "INSERT INTO $self->{schema}autos (name, source, data, configuration, distribution, ordering, critical, tokens, comment) SELECT name, source, data, configuration, '$to' AS distribution, ordering, critical, tokens, comment FROM $self->{schema}autos WHERE distribution = ? AND (configuration LIKE ? OR configuration = ?)";
    $sth = $dbh->prepare_cached($sqlstring);
    $sth->execute("$from", "$conf/%", "$conf");
    if ($dbh->errstr) {
        push @errors, $sqlstring.$dbh->errstr;
    }
    
    # mounts
    $sqlstring = "INSERT INTO $self->{schema}mounts (destination, configuration, distribution, active, ordering) SELECT destination, configuration, '$to' AS distribution, active, ordering FROM $self->{schema}mounts WHERE distribution = ? AND (configuration LIKE ? OR configuration = ?)";
    $sth = $dbh->prepare_cached($sqlstring);
    $sth->execute("$from", "$conf/%", "$conf");
    if ($dbh->errstr) {
        push @errors, $sqlstring.$dbh->errstr;
    }

    # tokens
    $sqlstring = "INSERT INTO $self->{schema}configuration_tokens (key, value, type, configuration, distribution, comment) SELECT key, value, type, configuration, '$to' AS distribution, comment FROM $self->{schema}configuration_tokens WHERE distribution = ? AND (configuration LIKE ? OR configuration = ?)";
    $sth = $dbh->prepare_cached($sqlstring);
    $sth->execute("$from", "$conf/%", "$conf");
    if ($dbh->errstr or $sth->errstr) {
        if ($sth->errstr) {
            push @errors, $sqlstring."\n".$sth->errstr;
        } else {
            push @errors, $sqlstring."\n".$dbh->errstr;
        }
    }

    # recursive configurations
    if (versionCompare($fromVersion, "004") >= 0) {
        if (versionCompare($toVersion, "004") < 0) {
            push @errors, "Warning: ignoring recursive configuration (not supported in $to - version $toVersion)\n";
        } else {
            $sqlstring = "INSERT INTO $self->{schema}configuration_configurations (conf_id, configuration, ordering, active, distribution) SELECT conf_id, configuration, ordering, active, '$to' AS distribution FROM $self->{schema}configuration_configurations WHERE distribution = ? AND (conf_id LIKE ? OR conf_id = ?)";
            $sth = $dbh->prepare_cached($sqlstring);
            $sth->execute("$from", "$conf/%", "$conf");
            if ($dbh->errstr or $sth->errstr) {
                if ($sth->errstr) {
                    push @errors, $sqlstring."\n".$sth->errstr;
                } else {
                    push @errors, $sqlstring."\n".$dbh->errstr;
                }
            }
        }
    }
    
    if (@errors) {
        chomp @errors;
        $self->{error} = join "\n", @errors;
        return undef;
    }

    return 1;
}

sub removeDistribution {
    my $self = shift;
    my $distribution = shift;
    return undef if ($self->{error} = invalidDistribution($distribution));
    my $dbh = $self->{dbh};
    my $sth = $dbh->prepare_cached("DELETE FROM $self->{schema}distributions WHERE name = ?");
    my $rv = $sth->execute("$distribution");
    if ($dbh->errstr) {
        $self->{error} = $dbh->errstr;
        return undef;
    } elsif ($rv == 0) {
        $self->{error} = "Distribution does not exists";
        return undef;
    }
    return 1;
}

sub addTokens {
    my $self = shift;
    my $tokens = shift;
    my $node = shift;
    if ($node->type() ne "configuration") {
        #group
        my $table;
        if ($node->type() eq "host") {
            $table = "$self->{schema}host_tokens(key, value, type, host)";
        } elsif ($node->type() eq "distribution") {
            $table = "$self->{schema}distribution_tokens(key, value, type, distribution)";
        } elsif ($node->type() eq "group") {
            $table = "$self->{schema}group_tokens(key, value, type, group_name)";
        }
        #print "\n***$node->{name}***\n";
        #print "***".join(", ", keys %$tokens)."***\n";
        foreach my $token (values %$tokens) {
            return undef unless ($self->insert($table, $token->{key}, $token->{raw}, $token->{type}, $node->name()));
        }
    } else {
        #configuration
        foreach my $token (values %$tokens) {
            return undef unless ($self->insert("$self->{schema}configuration_tokens(key, value, type, configuration, distribution)",
                                               $token->{key}, $token->{raw}, $token->{type}, $node->name(), $node->dist()));
        }
    }
    return 1;
}

sub removeTokens {
    my $self = shift;
    my $tokens = shift;
    my $node = shift;
    my $dbh = $self->{dbh};
    if ($node->type() ne "configuration") {
        #group
        my $stmt = "DELETE FROM $self->{schema}".$node->type()."_tokens WHERE key = ? AND ";
        if ($node->type() eq "host") {
            $stmt .= "host = ?";
        } elsif ($node->type() eq "distribution") {
            $stmt .= "distribution = ?";
        } elsif ($node->type() eq "group") {
            $stmt .= "group_name = ?";
        }
        my $sth = $dbh->prepare_cached($stmt);
        foreach my $token (@$tokens) {
            unless ($sth->execute($token, $node->name())) {
                $self->{error} = $sth->errstr;
                return 0;
            }
        }
    } else {
        #configuration
        my $stmt = "DELETE FROM $self->{schema}configuration_tokens WHERE key = ? AND configuration = ? AND distribution = ?";
        my $sth = $dbh->prepare_cached($stmt);
        foreach my $token (@$tokens) {
            unless ($sth->execute($token, $node->name(), $node->dist())) {
                $self->{error} = $sth->errstr;
                return 0;
            }
        }
    }
    return 1;
}

sub setTokens {
    my $self = shift;
    my $tokens = shift;
    my $node = shift;
    my $dbh = $self->{dbh};

    if ($node->type() ne "configuration") {
        # group
        my $stmt = "DELETE FROM $self->{schema}".$node->type()."_tokens WHERE ";
        if ($node->type() eq "host") {
            $stmt .= "host = ?";
        } elsif ($node->type() eq "distribution") {
            $stmt .= "distribution = ?";
        } elsif ($node->type() eq "group") {
            $stmt .= "group_name = ?";
        }
        my $sth = $dbh->prepare_cached($stmt);
        unless ($sth->execute($node->name())) {
            $self->{error} = $sth->errstr;
            return undef;
        }
    } else {
        #configuration
        my $stmt = "DELETE FROM $self->{schema}configuration_tokens WHERE configuration = ? AND distribution = ?";
        my $sth = $dbh->prepare_cached($stmt);
        unless ($sth->execute($node->name(), $node->dist())) {
            $self->{error} = $sth->errstr;
            return undef;
        }
    }

    return $self->addTokens($tokens, $node);
}

sub getTokens {
    my $self = shift;
    my @nodes = @_;
    my $dbh = $self->{dbh};
    my $sth;
    my %tokens = ();    
    foreach my $node (@nodes) {
        my $prefix = $node->type();
        if ($node->type() ne "configuration") {
            if ($node->type() eq "group") {
                $sth = $dbh->prepare_cached("SELECT key, value, type FROM $self->{schema}group_tokens WHERE group_name = ?");
            } elsif ($node->type() eq "distribution") {
                $sth = $dbh->prepare_cached("SELECT key, value, type FROM $self->{schema}distribution_tokens WHERE distribution = ?");
            } elsif ($node->type() eq "host") {
                $sth = $dbh->prepare_cached("SELECT key, value, type FROM $self->{schema}host_tokens WHERE host = ?");
            }
            $sth->execute($node->name())
        } else {
            $sth = $dbh->prepare_cached("SELECT key, value, type FROM $self->{schema}configuration_tokens WHERE configuration = ? AND distribution = ?");
            $sth->execute($node->name(), $node->dist());
        }

        if ($sth->err) {
            $self->{error} = $sth->errstr;
            return undef;
        }
        
        if (my $rawTokens = $sth->fetchall_hashref("key")) {
            @tokens{keys %$rawTokens} = map {{key => $_->{key}, value => $_->{value}, raw => $_->{value}, type => $_->{type}, source => "$prefix:".$node->name()}} values %$rawTokens;
        } elsif ($sth->err) {
            $self->{error} = $sth->errstr;
            return undef;
        }
    }
    # why is this here???
    # map {$_->{value} = "" if $_->{type} eq "static"} values %tokens;
    return \%tokens;
}

sub getGroups {
    my $self = shift;
    my $group = shift;

    my $stmt = "SELECT group_name FROM $self->{schema}".$group->type()."_groups WHERE ";
    if ($group->type() eq "host") {
        $stmt .= "host = ?";
    } elsif ($group->type() eq "group") {
        $stmt .= "groupid = ?";
    } elsif ($group->type() eq "distribution") {
        $stmt .= "distribution = ?";
    } else {
        $self->{error} = "Bad group";
        return undef;
    }
    $stmt .= " ORDER BY ordering";
    return $self->getList($stmt, $group->name());
}

sub getMounts {
    my $self = shift;
    my @configurations = @_;
    my @mounts = ();
    my @errors = ();
    my $dbh = $self->{dbh};
    my $sth = $dbh->prepare_cached("SELECT destination, active FROM $self->{schema}mounts WHERE configuration = ? AND distribution = ? ORDER BY ordering");

    foreach my $configuration (@configurations) {
        unless ($sth->execute($configuration->name(), $configuration->dist())) {
            $self->{error} = $sth->errstr;
            chomp ($self->{error});
            return undef;
        }
        my $resultArray = $sth->fetchall_arrayref();
        if ($sth->err) {
            $self->{error} = $sth->errstr;
            chomp ($self->{error});
            return undef;
        }
        push @mounts, map {Staple::Mount->new({destination => $_->[0], active => $_->[1], configuration => $configuration})} @$resultArray;
    }
    return @mounts;   
}

sub getTemplates {
    my $self = shift;
    my @configurations = @_;
    my @templates = ();
    my $dbh = $self->{dbh};
    my $sth = $dbh->prepare_cached("SELECT destination, source, data, stage, mode, gid, uid FROM $self->{schema}templates WHERE configuration = ? AND distribution = ?");

    foreach my $configuration (@configurations) {
        unless ($sth->execute($configuration->name(), $configuration->dist())) {
            $self->{error} = $sth->errstr;
            chomp ($self->{error});
            return undef;
        }
        my $resultArray = $sth->fetchall_arrayref();
        if ($sth->err) {
            $self->{error} = $sth->errstr;
            chomp ($self->{error});
            return undef;
        }
        push @templates, map { +{configuration => $configuration, destination => $_->[0], source => $_->[1], data => $_->[2], stage => $_->[3], mode => oct($_->[4]), gid => $_->[5], uid => $_->[6]}} @$resultArray;
    }
    return Staple::Template->new(@templates) if @templates;
    return ();
}

sub addTemplates {
    my $self = shift;
    my @templates = @_;
    my @errors = ();
    my $dbh = $self->{dbh};
    foreach my $template (@templates) {
                                         
        # delete previous
        if ($self->count("SELECT COUNT(*) FROM $self->{schema}templates WHERE configuration = ? AND distribution = ? AND destination = ? AND stage = ?", $template->configuration()->name(), $template->configuration()->dist(), $template->destination(), $template->stage())) {
            my $sth = $dbh->prepare_cached("DELETE FROM $self->{schema}templates WHERE configuration = ? AND distribution = ? AND destination = ? AND stage = ?");
            unless ($sth->execute($template->configuration()->name(), $template->configuration()->dist(), $template->destination(), $template->stage())) {
                push @errors, $sth->errstr;
                next;
            }
        }

        # check data
        if ($self->{checkData}) {
            $template->data();
            if ($template->error()) {
                push @errors, $template->error();
                next;
            }
        }

        # add template
        my $sth = $dbh->prepare_cached("INSERT INTO $self->{schema}templates(destination, configuration, distribution, source, data, comment, stage, mode, gid, uid) VALUES(?,?,?,?,?,?,?,?,?,?)");
        unless ($sth->execute($template->destination(),
                              $template->configuration()->name(),
                              $template->configuration()->dist(),
                              # use either data or source
                              (($self->{saveData} or not $template->source()) ?
                               ("", $template->data()) :
                               ($template->source(), "")), 
                              $template->note(),
                              $template->stage(),
                              sprintf("%04o", $template->mode()),
                              $template->gid(),
                              $template->uid())) {
            push @errors, $sth->errstr;
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
    my @templates = @_;
    my @errors = ();
    my $dbh = $self->{dbh};
    my $sth = $dbh->prepare_cached("DELETE FROM $self->{schema}templates WHERE configuration = ? AND distribution = ? AND destination = ? AND stage = ?");
    foreach my $template (@templates) {
        unless ($sth->execute($template->configuration()->name(), $template->configuration()->dist(), $template->destination(), $template->stage())) {
            push @errors, $sth->errstr;
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

sub getScripts {
    my $self = shift;
    my @configurations = @_;
    my @results = ();
    my $dbh = $self->{dbh};
    my $sth = $dbh->prepare_cached("SELECT name, source, data, stage, ordering, critical, tokens, tokenscript FROM $self->{schema}scripts WHERE configuration = ? AND distribution = ?");
    foreach my $configuration (@configurations) {
        unless ($sth->execute($configuration->name(), $configuration->dist())) {
            $self->{error} = $sth->errstr;
            chomp ($self->{error});
            return undef;
        }
        my $resultArray = $sth->fetchall_arrayref();
        if ($sth->err) {
            $self->{error} = $sth->errstr;
            chomp ($self->{error});
            return undef;
        }
        my @scripts = map { +{configuration => $configuration, name => $_->[0], source => $_->[1], data => $_->[2], stage => $_->[3], order => $_->[4], critical => $_->[5], tokens => $_->[6], tokenScript => $_->[7]}} @$resultArray;
        push @results, sort {$a->{stage} eq $b->{stage} ? $a->{order} <=> $b->{order} : stageCmp($a->{stage}, $b->{stage})} @scripts;
    }
    return Staple::Script->new(@results) if @results;
    return ();    
}

sub addScripts {
    my $self = shift;
    my @scripts = @_;
    my @errors = ();
    my $dbh = $self->{dbh};
    my $sth = $dbh->prepare_cached("INSERT INTO $self->{schema}scripts(name, source, data, configuration, distribution, stage, ordering, critical, tokens, tokenScript, comment) VALUES(?,?,?,?,?,?,?,?,?,?,?)");
    if ($dbh->errstr) {
        $self->{error} = "addScripts: ".$dbh->errstr;
        return undef;
    }
    foreach my $script (@scripts) {

        if ($self->{checkData}) {
            $script->data();
            if ($script->error()) {
                push @errors, $script->error();
                next;
            }
        }

        $self->{error} = "";
        my $location = $self->count("SELECT COUNT(*) FROM $self->{schema}scripts WHERE configuration = ? AND distribution = ? AND stage = ?", $script->configuration()->name(), $script->configuration()->dist(), $script->stage());
        if ($self->{error}) {
            push @errors, "addScripts: ".$self->{error};
            next;
        }
        $location = 0 unless $location;
        $script->order($location + 1) if not defined $script->order() or $script->order() > $location or $script->order() < 1;
        unless ($self->openOrdering($script->order(), "$self->{schema}scripts", "configuration = ? AND distribution = ? AND stage = ?", $script->configuration()->name(), $script->configuration()->dist(), $script->stage())) {
            push @errors, "addScripts: ".$self->{error};
            next;
        }
        unless ($sth->execute($script->name(),
                              # use either data or source
                              (($self->{saveData} or not $script->source()) ?
                               ("", $script->data()) :
                               ($script->source(), "")), 
                              $script->configuration()->name(),
                              $script->configuration()->dist(),
                              $script->stage(),
                              $script->order(),
                              $script->critical(),
                              $script->tokens(),
                              $script->tokenScript(),
                              $script->note())) {
            push @errors, "addScripts: ".$sth->errstr;
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
    my @scripts = @_;
    my @errors = ();   
    my $dbh = $self->{dbh};
    my $sth = $dbh->prepare_cached("DELETE FROM $self->{schema}scripts WHERE configuration = ? AND distribution = ? AND stage = ? AND ordering = ?");
    foreach my $script (sort {$b->order() <=> $a->order()} @scripts) {
        $sth->execute($script->configuration()->name(), $script->configuration()->dist(), $script->stage(), $script->order());
        if ($sth->errstr) {
            push @errors, $sth->errstr;
            next;
        }
        push @errors, $self->{error} unless ($self->closeOrdering($script->order(), "$self->{schema}scripts", "configuration = ? AND distribution = ? AND stage = ?", $script->configuration()->name(), $script->configuration()->dist(), $script->stage()));
    }
    if (@errors) {
        chomp @errors;
        $self->{error} = join "\n", @errors;
        return undef;
    }
    return 1;
}

sub getAutos {
    my $self = shift;
    my @configurations = @_;
    my @results = ();
    my $dbh = $self->{dbh};
    my $sth = $dbh->prepare_cached("SELECT name, source, data, ordering, critical, tokens FROM $self->{schema}autos WHERE configuration = ? AND distribution = ?");
    foreach my $configuration (@configurations) {
        unless ($sth->execute($configuration->name(), $configuration->dist())) {
            $self->{error} = $sth->errstr;
            chomp ($self->{error});
            return undef;
        }
        my $resultArray = $sth->fetchall_arrayref();
        if ($sth->err) {
            $self->{error} = $sth->errstr;
            chomp ($self->{error});
            return undef;
        }
        my @autos = map { +{configuration => $configuration, name => $_->[0], source => $_->[1], data => $_->[2], order => $_->[3], critical => $_->[4], tokens => $_->[5]}} @$resultArray;
        push @results, sort {$a->{order} <=> $b->{order}} @autos;
    }
    return Staple::Autogroup->new(@results) if @results;
    return ();
}


sub addAutos {
    my $self = shift;
    my @autos = @_;
    my @errors = ();
    my $dbh = $self->{dbh};
    my $sth = $dbh->prepare_cached("INSERT INTO $self->{schema}autos(name, source, data, configuration, distribution, ordering, critical, tokens) VALUES(?,?,?,?,?,?,?,?)");
    foreach my $auto (@autos) {

        if ($self->{checkData}) {
            $auto->data();
            if ($auto->error()) {
                push @errors, $auto->error();
                next;
            }
        }

        $self->{error} = "";
        my $location = $self->count("SELECT COUNT(*) FROM $self->{schema}autos WHERE configuration = ? AND distribution = ?", $auto->configuration()->name(), $auto->configuration()->dist());
        if ($self->{error}) {
            push @errors, $self->{error};
            next;
        }
        $location = 0 unless $location;
        $auto->order($location + 1) if not defined $auto->order() or $auto->order() > $location or $auto->order() < 1;
        unless ($self->openOrdering($auto->order(), "$self->{schema}autos", "configuration = ? AND distribution = ?", $auto->configuration()->name(), $auto->configuration()->dist())) {
            push @errors, $self->{error};
            next;
        }
        unless ($sth->execute($auto->name(),
                              # use either data or source
                              (($self->{saveData} or not $auto->source()) ?
                               ("", $auto->data()) :
                               ($auto->source(), "")), 
                              $auto->configuration()->name(),
                              $auto->configuration()->dist(),
                              $auto->order(),
                              $auto->critical(),
                              $auto->tokens())) {
            push @errors, $sth->errstr;
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
    my @autos = @_;
    my @errors = ();   
    my $dbh = $self->{dbh};
    my $sth = $dbh->prepare_cached("DELETE FROM $self->{schema}autos WHERE configuration = ? AND distribution = ? AND ordering = ?");
    foreach my $auto (sort {$b->order() <=> $a->order()} @autos) {
        $sth->execute($auto->configuration()->name(), $auto->configuration()->dist(), $auto->order());
        if ($sth->errstr) {
            push @errors, $sth->errstr;
            next;
        }
        push @errors, $self->{error} unless ($self->closeOrdering($auto->order(), "$self->{schema}autos", "configuration = ? AND distribution = ?", $auto->configuration()->name(), $auto->configuration()->dist()));
    }
    if (@errors) {
        chomp @errors;
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
    my $stmt = "SELECT COUNT(destination) FROM $self->{schema}mounts WHERE configuration = ? AND distribution = ? AND destination = ? AND active = ?";
    if ($self->count($stmt, $configuration->name(), $configuration->dist(), $mount->destination(), $mount->active())) {
        $mount->configuration($configuration);
        return undef unless $self->removeMounts($mount);
    }
    my $dbh = $self->{dbh};
    my $max = $self->count("SELECT MAX(ordering) FROM $self->{schema}mounts WHERE configuration = ? AND distribution = ?", $configuration->name(), $configuration->dist());
    $max = 0 unless $max;
    if ($location and $max and $location <= $max) { 
        return undef unless ($self->openOrdering($location, "$self->{schema}mounts", "configuration = ? AND distribution = ?", $configuration->name(), $configuration->dist()));
    } else {
        $location = $max + 1;
    }
    return undef unless ($self->insert("$self->{schema}mounts", $mount->destination(), $configuration->name(), $configuration->dist(), $mount->active(), $location));
    return 1;
}

sub removeMounts {
    my $self = shift;
    my @mounts = @_;
    my @errors = ();

    my $dbh = $self->{dbh};
    my $sth = $dbh->prepare_cached("DELETE FROM $self->{schema}mounts WHERE configuration = ? AND distribution = ? AND destination = ? AND active = ?");
    
    foreach my $mount (@mounts) {
        my $location = $self->count("SELECT ordering FROM $self->{schema}mounts WHERE configuration = ? AND distribution = ? AND destination = ? AND active = ?", $mount->configuration()->name(), $mount->configuration()->dist(), $mount->destination(), $mount->active());
        unless ($location) {
            push @errors, "\"".$mount->description()."\" is not in \"".$mount->configuration()->name()."\"";
            next;
        }
        my $rv = $sth->execute($mount->configuration()->name(), $mount->configuration()->dist(), $mount->destination(), $mount->active());
        if ($sth->errstr) {
            push @errors, $sth->errstr;
            next;
        } 
        push @errors, $self->{error} unless ($self->closeOrdering($location, "$self->{schema}mounts", "configuration = ? AND distribution = ?", $mount->configuration()->name(), $mount->configuration()->dist()));
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
    my @configurations = @_;
    my $version = $self->getDistributionVersion($conf->dist());
    if (versionCompare($version, "004") < 0) {
        $self->{error} = "distribution \"".$conf->dist()."\" is version $version (needs at least 004)";
        return undef;
    }
    my $col;
    my @errors = ();
    return undef unless $col = $self->getGroupColumn($conf); # conf_id
    my $dbh = $self->{dbh};
    my $sth = $dbh->prepare_cached("DELETE FROM $self->{schema}".$conf->type()."_configurations WHERE configuration = ? AND active = ? AND $col = ? AND distribution = ?");
    foreach my $torm (@configurations) {
        my $location = $self->count("SELECT ordering FROM $self->{schema}".$conf->type()."_configurations WHERE configuration = ? AND active = ? AND $col = ? AND distribution = ?", $torm->name(), $torm->active(), $conf->name(), $conf->dist());
        unless ($location) {
            push @errors, "\"".($torm->active() ? "+" : "-").$torm->name()."\" is not in \"".$conf->name()."\"";
            next;
        }
        my $rv = $sth->execute($torm->name(), $torm->active(), $conf->name(), $conf->dist());
        if ($sth->errstr) {
            push @errors, $sth->errstr;
            next;
        } 
        push @errors, $self->{error} unless ($self->closeOrdering($location, "$self->{schema}".$conf->type()."_configurations", "$col = ? AND distribution = ?", $conf->name(), $conf->dist()));
    }
    if (@errors) {
        $self->{error} = join "\n", @errors;
        return undef;
    }
    return 1;
#}   
}

sub getConfigurationConfigurations {
    my $self = shift;
    my $conf = shift;
    my $version = $self->getDistributionVersion($conf->dist());
    if (versionCompare($version, "004") < 0) {
        $self->{error} = "distribution \"".$conf->dist()."\" is version $version (needs at least 004)";
        return undef;
    }
    my $col = $self->getGroupColumn($conf); # conf_id
    return undef unless $col;

    my $dbh = $self->{dbh};
    my $stmt = "SELECT configuration, active FROM $self->{schema}".$conf->type()."_configurations WHERE $col = ? AND distribution = ? ORDER BY ordering";
    my $sth = $dbh->prepare_cached($stmt);
    unless ($sth->execute($conf->name(), $conf->dist())) {
        $self->{error} = $sth->errstr;
        chomp ($self->{error});
        return undef;
    }
    my $resultArray = $sth->fetchall_arrayref();
    if ($sth->err) {
        $self->{error} = $sth->errstr;
        chomp ($self->{error});
        return undef;
    }
    return map {Staple::Configuration->new({name => $_->[0], active => $_->[1], group => $conf})} @$resultArray;
}

sub addConfigurationConfiguration {
    my $self = shift;
    my $conf = shift;
    my $configuration = shift;
    my $location = shift;
    $location = int $location if $location;
    my $version = $self->getDistributionVersion($conf->dist());
    if (versionCompare($version, "004") < 0) {
        $self->{error} = "distribution \"".$conf->dist()."\" is version $version (needs at least 004)";
        return undef;
    }
    my $col;
    return undef unless $col = $self->getGroupColumn($conf); # conf_id
    unless ($self->getFullConfigurations([$configuration], $conf->dist())) {
        $self->{error} = "distribution ".$conf->dist()." doesn't have ".$configuration->name();
        return undef;
    }
    my $stmt = "SELECT COUNT(configuration) FROM $self->{schema}".$conf->type()."_configurations WHERE configuration = ? AND active = ? AND $col = ? AND distribution = ?";
    if ($self->count($stmt, $configuration->name(), $configuration->active(), $conf->name(), $conf->dist())) {
        # first remove if already there
        return undef unless $self->removeConfigurationConfigurations($conf, $configuration);
    }
    my $dbh = $self->{dbh};
    my $max = $self->count("SELECT MAX(ordering) FROM $self->{schema}".$conf->type()."_configurations WHERE $col = ? AND distribution = ?", $conf->name(), $conf->dist());
    $max = 0 unless $max;
    if ($location and $max and $location <= $max) { 
        return undef unless ($self->openOrdering($location, "$self->{schema}".$conf->type()."_configurations", "$col = ? AND distribution = ?", $conf->name(), $conf->dist()));
    } else {
        $location = $max + 1;
    }
    return undef unless ($self->insert("$self->{schema}".$conf->type()."_configurations", $conf->name(), $configuration->name(), $location, $configuration->active(), $conf->dist()));
    return 1;
}

sub getGroupConfigurations {
    my $self = shift;
    my $group = shift;
    my $col = $self->getGroupColumn($group);
    return undef unless $col;

    my $dbh = $self->{dbh};
    my $sth = $dbh->prepare_cached("SELECT configuration, active FROM $self->{schema}".$group->type()."_configurations WHERE $col = ? ORDER BY ordering");
    unless ($sth->execute($group->name())) {
        $self->{error} = $sth->errstr;
        chomp ($self->{error});
        return undef;
    }
    my $resultArray = $sth->fetchall_arrayref();
    if ($sth->err) {
        $self->{error} = $sth->errstr;
        chomp ($self->{error});
        return undef;
    }
    return map {Staple::Configuration->new({name => $_->[0], active => $_->[1], group => $group})} @$resultArray;
}

sub addGroupConfiguration {
    my $self = shift;
    my $group = shift;
    my $configuration = shift;
    my $location = shift;
    $location = int $location if $location;
    my $col;
    return undef unless $col = $self->getGroupColumn($group);
    my $stmt = "SELECT COUNT(configuration) FROM $self->{schema}".$group->type()."_configurations WHERE configuration = ? AND active = ? AND $col = ?";
    if ($self->count($stmt, $configuration->name(), $configuration->active(), $group->name())) {
        return undef unless $self->removeGroupConfigurations($group, $configuration);
    }
    my $dbh = $self->{dbh};
    my $max = $self->count("SELECT MAX(ordering) FROM $self->{schema}".$group->type()."_configurations WHERE $col = ?", $group->name());
    $max = 0 unless $max;
    if ($location and $max and $location <= $max) { 
        return undef unless ($self->openOrdering($location, "$self->{schema}".$group->type()."_configurations", "$col = ?", $group->name()));
    } else {
        $location = $max + 1;
    }
    return undef unless ($self->insert("$self->{schema}".$group->type()."_configurations ($col, configuration, ordering, active)", $group->name(), $configuration->name(), $location, $configuration->active()));
    return 1;
}

sub removeGroupConfigurations {
    my $self = shift;
    my $group = shift;
    my @configurations = @_;
    my $col;
    my @errors = ();
    return undef unless $col = $self->getGroupColumn($group);
    my $dbh = $self->{dbh};
    my $sth = $dbh->prepare_cached("DELETE FROM $self->{schema}".$group->type()."_configurations WHERE configuration = ? AND active = ? AND $col = ?");
    foreach my $conf (@configurations) {
        my $location = $self->count("SELECT ordering FROM $self->{schema}".$group->type()."_configurations WHERE configuration = ? AND active = ? AND $col = ?", $conf->name(), $conf->active(), $group->name());
        unless ($location) {
            push @errors, "\"".($conf->active() ? "+" : "-").$conf->name()."\" is not in \"".$group->name()."\"";
            next;
        }
        my $rv = $sth->execute($conf->name(), $conf->active(), $group->name());
        if ($sth->errstr) {
            push @errors, $sth->errstr;
            next;
        }
        push @errors, $self->{error} unless ($self->closeOrdering($location, "$self->{schema}".$group->type()."_configurations", "$col = ?", $group->name()));
    }
    if (@errors) {
        $self->{error} = join "\n", @errors;
        return undef;
    }
    return 1;
}

sub addGroupGroup {
    my $self = shift;
    my $group = shift;
    my $name = shift;
    my $location = shift;
    $location = int $location if $location;
    my $col;
    return undef unless $col = $self->getGroupColumn($group);
    my $stmt = "SELECT COUNT(group_name) FROM $self->{schema}".$group->type()."_groups WHERE group_name = ? AND $col = ?";
    if ($self->count($stmt, $name, $group->name())) {
        return undef unless $self->removeGroupGroups($group, $name);
    }
    my $dbh = $self->{dbh};
    my $max = $self->count("SELECT MAX(ordering) FROM $self->{schema}".$group->type()."_groups WHERE $col = ?", $group->name());
    $max = 0 unless $max;
    if ($location and $max and $location <= $max) { 
        return undef unless ($self->openOrdering($location, "$self->{schema}".$group->type()."_groups", "$col = ?", $group->name()));
    } else {
        $location = $max + 1;
    }
    return undef unless ($self->insert("$self->{schema}".$group->type()."_groups", $group->name(), $name, $location));
    return 1;
}

sub removeGroupGroups {
    my $self = shift;
    my $group = shift;
    my @names = @_;
    my $col;
    my @errors = ();
    return undef unless $col = $self->getGroupColumn($group);
    my $dbh = $self->{dbh};
    my $sth = $dbh->prepare_cached("DELETE FROM $self->{schema}".$group->type()."_groups WHERE group_name = ? AND $col = ?");
    foreach my $name (@names) {
        my $location = $self->count("SELECT ordering FROM $self->{schema}".$group->type()."_groups WHERE group_name = ? AND $col = ?", $name, $group->name());
        unless ($location) {
            push @errors, "\"$name\" is not in \"".$group->name()."\"";
            next;
        }
        my $rv = $sth->execute($name, $group->name());
        if ($sth->errstr) {
            push @errors, $sth->errstr;
            next;
        } 
        push @errors, $self->{error} unless ($self->closeOrdering($location, "$self->{schema}".$group->type()."_groups", "$col = ?", $group->name()));
    }
    if (@errors) {
        $self->{error} = join "\n", @errors;
        return undef;
    }
    return 1;
}

sub whoHasGroup {
    my $self = shift;
    my $group = shift;
    my $exact = 0;
    if ($group =~ m/\$$/) {
        $group =~ s/\$$//;
        $exact = 1;
    }
    if (not defined $self->getGroupPath($group)) {
        $self->{error} = "Unknown group \"$group\"";
        return undef;
    }

    # hosts
    my @hosts;
    if ($exact) {
        @hosts = $self->getList("SELECT host FROM $self->{schema}host_groups WHERE group_name = ?", $group);
    } else {
        @hosts = $self->getList("SELECT host FROM $self->{schema}host_groups WHERE group_name = ? OR group_name LIKE ?", $group, "$group/%");
    }
    @hosts = map {$self->getHostGroup($_)} @hosts;
    return undef if (grep {not defined $_} @hosts);

    # distributions
    my @distributions;
    if ($exact) {
        @distributions = $self->getList("SELECT distribution FROM $self->{schema}distribution_groups WHERE group_name = ?", $group);
    } else {
        @distributions = $self->getList("SELECT distribution FROM $self->{schema}distribution_groups WHERE group_name = ? OR group_name LIKE ?", $group, "$group/%");
    }
    @distributions = map {$self->getDistributionGroup($_)} @distributions;
    return undef if (grep {not defined $_} @distributions);

    # groups
    my @groups;
    if ($exact) {
        @groups = $self->getList("SELECT groupid FROM $self->{schema}group_groups WHERE group_name = ?", $group);
    } else {
        @groups = $self->getList("SELECT groupid FROM $self->{schema}group_groups WHERE group_name = ? OR group_name LIKE ?", $group, "$group/%");
    }
    @groups = $self->getGroupsByName(@groups);
    return undef if (grep {not defined $_} @groups);
    
    return @hosts, @distributions, @groups;
}

# re-activate after checking for common configurations
##sub whoHasConfiguration {
##    my $self = shift;
##    my $configuration = shift;
##    my $distribution = shift;
##    my $exact = 0;
##    if ($configuration =~ m/\$$/) {
##        $configuration =~ s/\$$//;
##        $exact = 1;
##    }
##
##    $distribution = undef if defined $distribution and not $self->getDistributionGroup($distribution);
##    
##    # hosts
##    my @hosts;
##    if ($exact) {
##        @hosts = $self->getList("SELECT host FROM $self->{schema}host_configurations WHERE configuration = ?", $configuration);
##    } else {
##        @hosts = $self->getList("SELECT host FROM $self->{schema}host_configurations WHERE configuration = ? OR configuration LIKE ?", $configuration, "$configuration/%");
##    }
##    @hosts = map {$self->getHostGroup($_)} @hosts;
##    return undef if (grep {not defined $_} @hosts);
##
##    # distributions
##    my @distributions;
##    if ($exact) {
##        @distributions = $self->getList("SELECT distribution FROM $self->{schema}distribution_configurations WHERE configuration = ?", $configuration);
##    } else {
##        @distributions = $self->getList("SELECT distribution FROM $self->{schema}distribution_configurations WHERE configuration = ? OR configuration LIKE ?", $configuration, "$configuration/%");
##    }
##    @distributions = map {$self->getDistributionGroup($_)} @distributions;
##    return undef if (grep {not defined $_} @distributions);
##     
##    # groups
##    my @groups;
##    if ($exact) {
##        @groups = $self->getList("SELECT groupid FROM $self->{schema}group_configurations WHERE configuration = ?", $configuration);
##    } else {
##        @groups = $self->getList("SELECT groupid FROM $self->{schema}group_configurations WHERE configuration = ? OR configuration LIKE ?", $configuration, "$configuration/%");
##    }
##    @groups = $self->getGroupsByName(@groups);
##    return undef if (grep {not defined $_} @groups);
##
##    # configurations
##    my @configurations;
##    if ($exact) {
##        @configurations = $self->getList("SELECT conf_id FROM $self->{schema}configuration_configurations WHERE configuration = ? AND distribution = ?", $configuration, $distribution);
##    } else {
##        @configurations = $self->getList("SELECT conf_id FROM $self->{schema}configuration_configurations WHERE (configuration = ? OR configuration LIKE ?) AND distribution = ?", $configuration, "$configuration/%", $distribution);
##    }
##    @configurations = $self->getConfigurationsByName(@configurations);
##    return @hosts, @distributions, @groups, @configurations;
##}

sub whoHasToken {
    my $self = shift;
    my $key = shift;
    my $distribution = shift;
    
    if (not defined $self->getDistributionGroup($distribution)) {
        # error is already set by $self
        return undef;
    }
    
    # hosts
    my @hosts = $self->getList("SELECT host FROM $self->{schema}host_tokens WHERE key = ?", $key);
    @hosts = map {$self->getHostGroup($_)} @hosts;
    return undef if (grep {not defined $_} @hosts);

    # distributions
    my @distributions = $self->getList("SELECT distribution FROM $self->{schema}distribution_tokens WHERE key = ?", $key);
    @distributions = map {$self->getDistributionGroup($_)} @distributions;
    return undef if (grep {not defined $_} @distributions);

    # groups
    my @groups = $self->getList("SELECT group_name FROM $self->{schema}group_tokens WHERE key = ?", $key);
    @groups = $self->getGroupsByName(@groups);
    return undef if (grep {not defined $_} @groups);
    
    # configurations
    my @configurations = $self->getList("SELECT configuration FROM $self->{schema}configuration_tokens WHERE distribution = ? AND key = ?", $distribution, $key);
    @configurations = Staple::Configuration->new(map {{name => $_}} @configurations);
    @configurations = $self->getFullConfigurations(\@configurations, $distribution);
    return undef if (grep {not defined $_} @configurations);

    return ([@hosts, @distributions, @groups], [@configurations])
}

sub getAllHosts {
    my $self = shift;
    return sort {$a cmp $b} $self->getList("SELECT host FROM $self->{schema}hosts");
}

sub getAllGroups {
    my $self = shift;
    return sort {$a cmp $b} $self->getList("SELECT name FROM $self->{schema}groups WHERE type = 'group'");
}

sub getAllConfigurations {
    my $self = shift;
    my $distribution = shift;
    my $version = $self->getDistributionVersion($distribution);
    my @confs;
    if (versionCompare($version, "004") < 0) {
        # before 004, no common configurations
        @confs = sort {$a cmp $b} $self->getList("SELECT name FROM $self->{schema}configurations WHERE distribution = ? ", $distribution); 
    } elsif (versionCompare($version, "005") <= 0) {
        # on 004 and 005, common configuration but in special /common/ distribution
        @confs = sort {$a cmp $b} $self->getList("SELECT name FROM $self->{schema}configurations WHERE distribution = ? OR distribution = '".$self->getCommonPath()."'", $distribution);
    } else {
        # on 006, common configuration on all distribution 
        @confs = sort {$a cmp $b} $self->getList("SELECT name FROM $self->{schema}configurations WHERE distribution = ? ", $distribution); 
    }
    return @confs;
}

sub getFullConfigurations {
    my $self = shift;
    my $confs = shift;
    my $distribution = shift;
    my @confs = $self->SUPER::getFullConfigurations($confs, $distribution);
    my $common = $self->getCommonPath();
    map {$_->dist($common) if index($_->name(), "common/") == 0} @confs;
    return @confs;
}

sub getAllDistributions {
    my $self = shift;
    return sort {$a cmp $b} $self->getList("SELECT name FROM $self->{schema}distributions WHERE name <> '".$self->getCommonPath()."'");
}

sub getDistributionVersion {
    my $self = shift;
    my $dist = shift;
    if ($dist eq $self->getCommonPath()) {
        my $version = $self->getMinimumDistributionVersion();
        $version = "004" if (versionCompare($version, "004") < 0);
        return $version;
    }
    my $dbh = $self->{dbh};
    my $schema = $self->{schema};
    $schema =~ s/\.$//;
  gdv_retry:
    my $sth = $dbh->column_info(undef, $schema, "distributions", undef);
    my $result = $sth->fetchall_hashref("COLUMN_NAME");
    if ($sth->err) {
        $self->{error} = $sth->errstr;
        chomp ($self->{error});
        return undef;
    }
    if (not exists $result->{version} and defined $schema and length($schema) == 0) {
        # on e.g. sqlite, need to set undef
        $schema = undef;
        goto gdv_retry;
    }
    if (exists $result->{version}) {
        my $ver = $self->count("SELECT version FROM $self->{schema}distributions WHERE name = ?", $dist);
        return undef if ($self->{error});
        return "none" unless defined $ver;
        return $ver;
    }
    return "none";
}

sub setDistributionVersion {
    my $self = shift;
    my $dist = shift;
    my $ver = shift;
    $ver = "none" unless defined $ver;
    my $old = $self->getDistributionVersion($dist);

    if (versionCompare($old, $ver) > 0) {
        $self->{error} = "Can't downgrade $dist (from $old to $ver)";
        return undef;
    }

    # add all common to distribution in the configuration tables
    if ((versionCompare($old, "005") <= 0) and
        (versionCompare($ver, "005") > 0)) {
        # first move to 004 so getAllConfigurations will show common configurations
        if (versionCompare($old, "004") < 0) {
            my $dbh = $self->{dbh};
            my $sth = $dbh->prepare_cached("UPDATE $self->{schema}distributions SET version = ? WHERE name = ?");
            unless ($sth->execute("004", $dist)) {
                $self->{error} = $sth->errstr;
                return undef;
            }
        }
        my @confs = grep m/^common\//, $self->getAllConfigurations($dist);
        foreach my $conf (@confs) {
            unless ($self->insert("$self->{schema}configurations(name, distribution)", "$conf", "$dist")) {
                return undef;
            }
        }
    }
    
    my $dbh = $self->{dbh};
    my $sth = $dbh->prepare_cached("UPDATE $self->{schema}distributions SET version = ? WHERE name = ?");
    unless ($sth->execute($ver, $dist)) {
        $self->{error} = $sth->errstr;
        return undef;
    }
    return $old;
}

sub getConfigurationPath {
    my $self = shift;
    my $configuration = shift;
    my $distribution = shift;
    $distribution = $self->getCommonPath() if index($configuration, "common/") == 0;
    if ($self->count("SELECT COUNT(name) FROM $self->{schema}configurations WHERE name = ? AND distribution = ?", $configuration, $distribution)) {
        # not really used, lets keep this unique
        return "$configuration:$distribution";
    }
    return undef;
}

sub getCommonPath {
    my $self = shift;
    return "/common/";
}

sub getGroupPath {
    my $self = shift;
    my $group = shift;
    if ($self->count("SELECT COUNT(name) FROM $self->{schema}groups WHERE name = ?", $group)) {
        # not used, lets invent something
        return "$group";
    }
    return undef;
}

sub getDistributionPath {
    my $self = shift;
    my $distribution = shift;
    if ($self->count("SELECT COUNT(name) FROM $self->{schema}distributions WHERE name = ?", $distribution)) {
        # not used, lets invent something
        return "$distribution";
    }
    return undef;
}

sub getHostPath {
    my $self = shift;
    my $host = shift;
    if ($self->count("SELECT COUNT(host) FROM $self->{schema}hosts WHERE host = ?", $host)) {
        # not used, lets invent something
        return "$host";
    }
    return undef;
}

sub setNote {
    my $self = shift;
    my $node = shift;
    my $note = shift;
    my $col = $node->type() eq "host" ? "host" : "name";
    my $dbh = $self->{dbh};
    if ($node->type() eq "configuration") {
        my $sth = $dbh->prepare_cached("UPDATE $self->{schema}configurations SET comment = ? WHERE name = ? AND distribution = ?");
        unless ($sth->execute($note, $node->name(), $node->dist())) {
            $self->{error} = $sth->errstr;
            return undef;
        }
    } else {
        my $sth = $dbh->prepare_cached("UPDATE $self->{schema}".$node->type()."s SET comment = ? WHERE $col = ?");
        unless ($sth->execute($note, $node->name())) {
            $self->{error} = $sth->errstr;
            return undef;
        }
    }
    return 1;
}

sub getNote {
    my $self = shift;
    my $node = shift;
    my $note = "";
    $self->{error} = "";
    if (ref $node and $node->path()) {
        if ($node->type() eq "configuration") {
            $note = $self->count("SELECT comment FROM $self->{schema}configurations WHERE name = ? AND distribution = ?", $node->name(), $node->dist());
            return undef if $self->{error};
        } else {
            my $col = $node->type() eq "host" ? "host" : "name";
            $note = $self->count("SELECT comment FROM $self->{schema}".$node->type()."s WHERE $col = ?", $node->name());
            return undef if $self->{error};
        }
        $note = "" unless defined $note;
    }
    return $note;
}

################################################################################
#   Internals
################################################################################

# input: database parameters
# output: $self (can be Staple::DB::Error)
sub _init {
    my $self = shift;
    my @params = @_;
    $params[0] = "dbi:SQLite:/tmp/staple.sqlite3" unless $params[0];
    $params[1] = "$params[1]" if defined $params[1];
    $params[1] = undef if defined $params[1] and $params[1] eq "undef";
    $self->{error} = "";
    $self->{schema} = $params[1];
    $self->{connectionParams} = [$params[0], $params[2], $params[3], {
                                                                      #HandleError => \&sigDBError,
                                                                      PrintError => 0,
                                                                      AutoCommit => 1,
                                                                      RaiseError => 0,
                                                                      pg_server_prepare => 1}];
    $self->{schema} .= "." if $self->{schema};
    $self->{schema} = "" unless defined $self->{schema};
    $self->{saveData} = 1; # save data rather then source on templates
    $self->{checkData} = 1; # check data of templates before adding (for DB::FSQL to override)
    $self->{dbh} = DBI->connect_cached(@{$self->{connectionParams}});
    return createDB("error", DBI::errstr) unless $self->{dbh};
    if ($self->{connectionParams}[0] =~ m/^dbi:SQLite:/) {
        unless (defined $self->{dbh}->do("PRAGMA foreign_keys = ON")) {
            return createDB("error", DBI::errstr);
        }
    }
    return $self;
}

# assumes _init was already called
# input: ($self)
# output: $self (can be Staple::DB::Error);
sub _buildDB {
    my $self = shift;
    my $dbh = $self->{dbh};
    return createDB("error", DBI::errstr) unless $dbh;
    return createDB("error", $dbh->errstr) if $dbh->errstr;
    my $schema = $self->{schema};
    $schema =~ s/\.$//;
    my $sth = $dbh->table_info(undef, $schema ? $schema : undef, undef, "TABLE");
    return createDB("error", $dbh->errstr) unless $sth;
    my $tables = $sth->fetchall_hashref("TABLE_NAME");
    return createDB("error", $sth->errstr) if $sth->errstr;
    foreach my $create (@Staple::DB::SQL::Init::createTables) {
        $create =~ s/_SCHEMA_/$self->{schema}/g;
        (my $name) = $create =~ m/^CREATE TABLE ([^\s]*)/;
        $name =~ s/^$self->{schema}//;
        next if ($tables->{$name});
        $dbh->do($create);
        return createDB("error", $dbh->errstr) if $dbh->errstr;
    }
    foreach my $insert (@Staple::DB::SQL::Init::insertInto) {
        $insert =~ s/_SCHEMA_/$self->{schema}/;
        (my $name) = $insert =~ m/^INSERT INTO ([^\s(]*)/;
        $name =~ s/^$self->{schema}//;
        next if ($tables->{$name});
        $dbh->do($insert);
        return createDB("error", $dbh->errstr) if $dbh->errstr;
    }
    return $self;
}

# input: location to open, table, [where], [values, ...]
# output: 1 or undef
# "opens" a place for the given location to be inserted
sub openOrdering {
    my $self = shift;
    my $location = shift;
    my $table = shift;
    my $where = shift;
    my @values = @_;
    my $dbh = $self->{dbh};
    $dbh->begin_work;
    if ($where) {
        $where = "AND $where";
    } else {
        $where = "";
    }
    my $sth = $dbh->prepare_cached("UPDATE $table SET ordering = - ordering WHERE ordering >= ? $where");
    unless ($sth->execute($location, @values)) {
        $self->{error} = $sth->errstr;
        $dbh->rollback;
        return undef;
    }
    $sth = $dbh->prepare_cached("UPDATE $table SET ordering = - (ordering - 1) WHERE ordering < 0 $where");
    unless ($sth->execute(@values)) {
        $self->{error} = $sth->errstr;
        $dbh->rollback;
        return undef;
    }
    $dbh->commit;
    return 1;
}

# input: location to open, table, [where], [values, ...]
# output: 1 or undef
# "closes" an opened location
sub closeOrdering {
    my $self = shift;
    my $location = shift;
    my $table = shift;
    my $where = shift;
    my @values = @_;
    my $dbh = $self->{dbh};
    $dbh->begin_work or die "a horrible death";
    if ($where) {
        $where = "AND $where";
    } else {
        $where = "";
    }
    my $sth = $dbh->prepare_cached("UPDATE $table SET ordering = - ordering WHERE ordering >= ? $where");
    unless ($sth->execute($location, @values)) {
        $self->{error} = $sth->errstr;
        $dbh->rollback;
        return undef;
    }
    $sth = $dbh->prepare_cached("UPDATE $table SET ordering = - (ordering + 1) WHERE ordering < 0 $where");
    unless ($sth->execute(@values)) {
        $self->{error} = $sth->errstr;
        $dbh->rollback;
        return undef;
    }
    $dbh->commit;
    return 1;
}

# input: node
# output: host, distribution, groupid or conf_id, depending on the node type
sub getGroupColumn {
    my $self = shift;
    my $node = shift;
    if ($node->type() eq "host") {
        return "host";
    } elsif ($node->type() eq "group") {
        return "groupid";
    } elsif ($node->type() eq "distribution") {
        return "distribution";
    } elsif ($node->type() eq "configuration") {
        return "conf_id";
    }
    $self->{error} = "Bad group";
    return undef;
}

# input: SELECT statement with single row/column, [values]
# output: first value
sub count {
    my $self = shift;
    my $stmt = shift;
    my @values = @_;
    $self->{error} = "";
    my $dbh = $self->{dbh};
    my $sth = $dbh->prepare_cached($stmt);
    unless ($sth->execute(@values)) {
        $self->{error} = $sth->errstr;
        $sth->finish();
        return undef;
    }
    my @resultArray = $sth->fetchrow_array();
    $sth->finish();
    if ($sth->err) {
        $self->{error} = $sth->errstr;
        return undef;
    }
    return $resultArray[0];
}

# input: SELECT statement, [values]
sub getList {
    my $self = shift;
    my $stmt = shift;
    my @values = @_;
    $self->{error} = "";
    my $dbh = $self->{dbh};
    my $sth = $dbh->prepare_cached($stmt);
    unless ($sth) {
        $self->{error} = $dbh->errstr;
        return undef;
    }
    unless ($sth->execute(@values)) {
        $self->{error} = $sth->errstr;
        chomp ($self->{error});
        $self->{error} .= " ($stmt)";
        return undef;
    }
    my $resultArray = $sth->fetchall_arrayref();
    if ($sth->err) {
        $self->{error} = $sth->errstr;
        chomp ($self->{error});
        $self->{error} .= " ($stmt)";
        return undef;
    }
    return map {$$_[0]} @$resultArray;
}

## input: SELECT statement, column key, [values]
#sub getList {
#    my $self = shift;
#    my $stmt = shift;
#    my $key = shift;
#    my @values = @_;
#    $self->{error} = "";
#    my $dbh = $self->{dbh};
#    my $sth = $dbh->prepare_cached($stmt);
#    unless ($sth->execute(@values)) {
#        $self->{error} = $sth->errstr;
#        return undef;
#    }
#    my $resultHash = $sth->fetchall_hashref($key);
#    if ($sth->err) {
#        $self->{error} = $sth->errstr;
#        return undef;
#    }
#    if ($resultHash) {
#        return keys %$resultHash;
#    }
#    return ();
#}

# input: table to insert, values
# output: 1 or 0 on success or failure
sub insert {
    my $self = shift;
    my $table = shift;
    my @values = @_;
    my $dbh = $self->{dbh};
    my $sth = $dbh->prepare_cached("INSERT INTO $table VALUES (".join(",",map {"?"} @values).")");
    goto inserterror unless $sth;
    my $rv = $sth->execute(@values);
    goto inserterror if $dbh->errstr;
    return 1;

  inserterror:
    $self->{error} = "INSERT INTO $table VALUES (".join(", ", @values)."): ".$dbh->errstr;
    return 0;
}

################################################################################
#   The end
################################################################################

1;

__END__

=back

=head1 SEE ALSO

L<Staple> - Staple main module.

L<Staple::DB> - DB interface

L<Staple::DB::SQL> - SQL - Database

=head1 AUTHOR

Yair Yarom, E<lt>irush@cs.huji.ac.ilE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2007-2011 Hebrew University Of Jerusalem, Israel
See the LICENSE file.

=cut
