package Staple::DB::SQL;

#
# Copyright (C) 2007-2009 Hebrew University Of Jerusalem, Israel
# See the LICENSE file.
#
# Author: Yair Yarom <irush@cs.huji.ac.il>
#

use strict;
use warnings;
use Staple::DB;
use Staple::Misc;
use DBI;
use DBD::Pg;
our @ISA = ("Staple::DB");
our $VERSION = '004';

=head1 NAME

  Staple::DB::SQL - API for SQL DB as database connection

=cut

################################################################################
#   Exported
################################################################################

=head1 DESCRIPTION

=over

=itme B<new(params, schema [,username, password])>

creates a new instance. First parameter is the schema name string (defaults to
staple). second parameter is the database parameters (defaults to
dbi:Pg:dbname=staple;host=pghost;port=5432;). Third and forth parameters are
username and password (can be undef).

=cut

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = {};
    my @params = @_;
    $params[0] = "$params[0]" if defined $params[0];
    $params[0] = "staple" unless defined $params[0];
    $params[1] = "dbi:Pg:dbname=staple;host=pghost;port=5432;" unless $params[1];
    $self->{error} = "";
    $self->{schema} = $params[0];
    $self->{connectionParams} = [$params[1], $params[2], $params[3], {
                                                                      #HandleError => \&sigDBError,
                                                                      PrintError => 0,
                                                                      AutoCommit => 1,
                                                                      RaiseError => 0,
                                                                      pg_server_prepare => 1}];
    $self->{schema} .= "." if $self->{schema};
    return undef unless DBI->connect_cached(@{$self->{connectionParams}});
    bless ($self, $class);
    return $self;
}

sub info {
    my $self = shift;
    my $schema = $self->{schema};
    $schema =~ s/\.$//;
    my $username = $self->{connectionParams}->[1];
    $username = "" unless $username;
    return "sql $schema $self->{connectionParams}->[0] $username"
}

sub addHost {
    my $self = shift;
    my $host = shift;
    return 0 if ($self->{error} = invalidHost($host));
    return $self->insert("$self->{schema}hosts", "$host");
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
    return undef if ($self->{error} = invalidDistribution($distribution));
    return undef if ($self->{error} = invalidConfiguration($configuration));
    $configuration = fixPath($configuration);
    $configuration =~ s/\/$//;
    if ($self->count("SELECT COUNT(name) FROM $self->{schema}configurations WHERE name = ? AND distribution = ?", $configuration, $distribution)) {
        $self->{error} = "Configuration exists";
        return undef;
    };
    foreach my $subconf (splitData($configuration)) {
        unless ($self->count("SELECT COUNT(name) FROM $self->{schema}configurations WHERE name = ? AND distribution = ?", $subconf, $distribution)) {
            unless ($self->insert("$self->{schema}configurations(name, distribution)", "$subconf", "$distribution")) {
                return undef;
            }
        }
    }
    return 1;
}

sub addDistribution {
    my $self = shift;
    my $distribution = shift;
    return 0 if ($self->{error} = invalidDistribution($distribution));
    return $self->insert("$self->{schema}distributions(name)", "$distribution");
}

sub removeHost {
    my $self = shift;
    my $host = shift;
    return 0 if ($self->{error} = invalidHost($host));
    my $dbh = DBI->connect_cached(@{$self->{connectionParams}});
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
    my $dbh = DBI->connect_cached(@{$self->{connectionParams}});
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
    return undef if ($self->{error} = invalidDistribution($distribution));
    return undef if ($self->{error} = invalidConfiguration($configuration));
    $configuration = fixPath($configuration);
    $configuration =~ s/\/$//;
    unless ($self->count("SELECT COUNT(name) FROM $self->{schema}configurations WHERE name = ? AND distribution = ?", $configuration, $distribution)) {
        $self->{error} = "Configuration does not exists";
        return undef;
    };
    my $dbh = DBI->connect_cached(@{$self->{connectionParams}});
    my $sth = $dbh->prepare_cached("DELETE FROM $self->{schema}configurations WHERE name LIKE ? AND distribution = ?");
    my $like = $configuration;
    $like =~ s/_/\\_/g;
    $like =~ s/%/\\%/g;
    my $rv = $sth->execute("$like/%", $distribution);
    if ($dbh->errstr) {
        $self->{error} = $dbh->errstr;
        return undef;
    }
    $sth->execute($like, $distribution);
    if ($dbh->errstr) {
        $self->{error} = $dbh->errstr;
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
    my $fromPath = $self->getConfigurationPath($conf, $from);
    my $toPath = $self->getConfigurationPath($conf, $to, 1);

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

    # configurations
    my $sqlstring = "INSERT INTO $self->{schema}configurations SELECT name, '$to' AS distribution, comment FROM $self->{schema}configurations WHERE distribution = ? AND ( name LIKE ? OR name = ?)";
    my $dbh = DBI->connect_cached(@{$self->{connectionParams}});
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
    my $dbh = DBI->connect_cached(@{$self->{connectionParams}});
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
    my $group = shift;
    if ($group->{type} and $group->{type} ne "configuration") {
        #group
        my $table;
        if ($group->{type} eq "host") {
            $table = "$self->{schema}host_tokens(key, value, type, host)";
        } elsif ($group->{type} eq "distribution") {
            $table = "$self->{schema}distribution_tokens(key, value, type, distribution)";
        } elsif ($group->{type} eq "group") {
            $table = "$self->{schema}group_tokens(key, value, type, group_name)";
        }
        #print "\n***$group->{name}***\n";
        #print "***".join(", ", keys %$tokens)."***\n";
        foreach my $token (values %$tokens) {
            return undef unless ($self->insert($table, $token->{key}, $token->{raw}, $token->{type}, $group->{name}));
        }
    } else {
        #configuration
        foreach my $token (values %$tokens) {
            return undef unless ($self->insert("$self->{schema}configuration_tokens(key, value, type, configuration, distribution)",
                                               $token->{key}, $token->{raw}, $token->{type}, $group->{name}, $group->{dist}));
        }
    }
    return 1;
}

sub removeTokens {
    my $self = shift;
    my $tokens = shift;
    my $group = shift;
    my $dbh = DBI->connect_cached(@{$self->{connectionParams}});
    if ($group->{type} and $group->{type} ne "configuration") {
        #group
        my $stmt = "DELETE FROM $self->{schema}$group->{type}_tokens WHERE key = ? AND ";
        if ($group->{type} eq "host") {
            $stmt .= "host = ?";
        } elsif ($group->{type} eq "distribution") {
            $stmt .= "distribution = ?";
        } elsif ($group->{type} eq "group") {
            $stmt .= "group_name = ?";
        }
        my $sth = $dbh->prepare_cached($stmt);
        foreach my $token (@$tokens) {
            unless ($sth->execute($token, $group->{name})) {
                $self->{error} = $sth->errstr;
                return 0;
            }
        }
    } else {
        #configuration
        my $stmt = "DELETE FROM $self->{schema}configuration_tokens WHERE key = ? AND configuration = ? AND distribution = ?";
        my $sth = $dbh->prepare_cached($stmt);
        foreach my $token (@$tokens) {
            unless ($sth->execute($token, $group->{name}, $group->{dist})) {
                $self->{error} = $sth->errstr;
                return 0;
            }
        }
    }
    return 1;
}

sub getTokens {
    my $self = shift;
    my @groupsAndConfigurations = @_;
    my $dbh = DBI->connect_cached(@{$self->{connectionParams}});
    my $sth;
    my %tokens = ();    
    foreach my $gorc (@groupsAndConfigurations) {
        my $prefix;
        if (defined $gorc->{type} and $gorc->{type} ne "configuration") {
            # in the past, only groups had types
            $prefix = $gorc->{type};
            if ($gorc->{type} eq "group") {
                $sth = $dbh->prepare_cached("SELECT key, value, type FROM $self->{schema}group_tokens WHERE group_name = ?");
            } elsif ($gorc->{type} eq "distribution") {
                $sth = $dbh->prepare_cached("SELECT key, value, type FROM $self->{schema}distribution_tokens WHERE distribution = ?");
            } elsif ($gorc->{type} eq "host") {
                $sth = $dbh->prepare_cached("SELECT key, value, type FROM $self->{schema}host_tokens WHERE host = ?");
            }
            $sth->execute($gorc->{name})
        } else {
            # configurations do not have types
            $prefix = "configuration";
            $sth = $dbh->prepare_cached("SELECT key, value, type FROM $self->{schema}configuration_tokens WHERE configuration = ? AND distribution = ?");
            $sth->execute($gorc->{name}, $gorc->{dist});
        }

        if ($sth->err) {
            $self->{error} = $sth->errstr;
            return undef;
        }
        
        if (my $rawTokens = $sth->fetchall_hashref("key")) {
            @tokens{keys %$rawTokens} = map {{key => $_->{key}, value => $_->{value}, raw => $_->{value}, type => $_->{type}, source => "$prefix:$gorc->{name}"}} values %$rawTokens;
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

    my $stmt = "SELECT group_name FROM $self->{schema}$group->{type}_groups WHERE ";
    if ($group->{type} eq "host") {
        $stmt .= "host = ?";
    } elsif ($group->{type} eq "group") {
        $stmt .= "groupid = ?";
    } elsif ($group->{type} eq "distribution") {
        $stmt .= "distribution = ?";
    } else {
        $self->{error} = "Bad group";
        return undef;
    }
    $stmt .= " ORDER BY ordering";
    return $self->getList($stmt, $group->{name});
}

sub getMounts {
    my $self = shift;
    my @configurations = @_;
    my @mounts = ();
    my @errors = ();
    my $dbh = DBI->connect_cached(@{$self->{connectionParams}});
    my $sth = $dbh->prepare_cached("SELECT destination, active FROM $self->{schema}mounts WHERE configuration = ? AND distribution = ? ORDER BY ordering");

    foreach my $configuration (@configurations) {
        unless ($sth->execute($configuration->{name}, $configuration->{dist})) {
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
        push @mounts, map { +{destination => $_->[0], active => $_->[1], configuration => $configuration}} @$resultArray;
    }
    return @mounts;   
}

sub getTemplates {
    my $self = shift;
    my @configurations = @_;
    my @templates = ();
    my $dbh = DBI->connect_cached(@{$self->{connectionParams}});
    my $sth = $dbh->prepare_cached("SELECT destination, source, data, stage, mode, gid, uid FROM $self->{schema}templates WHERE configuration = ? AND distribution = ?");

    foreach my $configuration (@configurations) {
        unless ($sth->execute($configuration->{name}, $configuration->{dist})) {
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
    return @templates;
}

sub addTemplates {
    my $self = shift;
    my @templates = @_;
    my @errors = ();
    my $dbh = DBI->connect_cached(@{$self->{connectionParams}});
    foreach my $template (@templates) {
        if ($self->count("SELECT COUNT(*) FROM $self->{schema}templates WHERE configuration = ? AND distribution = ? AND destination = ? AND stage = ?", $template->{configuration}->{name}, $template->{configuration}->{dist}, $template->{destination}, $template->{stage})) {
            my $sth = $dbh->prepare_cached("DELETE FROM $self->{schema}templates WHERE configuration = ? AND distribution = ? AND destination = ? AND stage = ?");
            unless ($sth->execute($template->{configuration}->{name}, $template->{configuration}->{dist}, $template->{destination}, $template->{stage})) {
                push @errors, $sth->errstr;
                next;
            }
        }
        if ($template->{source}) {
            unless (open(FILE, "<$template->{source}")) {
                push @errors, "Can't open template for coping \"$template->{source}\": $!";
                next;
            }
            $template->{data} = join "", <FILE>;
            close(FILE);
            $template->{source} = "";
        }
        my $sth = $dbh->prepare_cached("INSERT INTO $self->{schema}templates(destination, configuration, distribution, source, data, comment, stage, mode, gid, uid) VALUES(?,?,?,?,?,?,?,?,?,?)");
        unless ($sth->execute($template->{destination},
                              $template->{configuration}->{name},
                              $template->{configuration}->{dist},
                              $template->{source},
                              $template->{data},
                              $template->{comment},
                              $template->{stage},
                              sprintf("%04o", $template->{mode}),
                              $template->{gid},
                              $template->{uid})) {
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
    my $dbh = DBI->connect_cached(@{$self->{connectionParams}});
    my $sth = $dbh->prepare_cached("DELETE FROM $self->{schema}templates WHERE configuration = ? AND distribution = ? AND destination = ? AND stage = ?");
    foreach my $template (@templates) {
        unless ($sth->execute($template->{configuration}->{name}, $template->{configuration}->{dist}, $template->{destination}, $template->{stage})) {
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
    my $dbh = DBI->connect_cached(@{$self->{connectionParams}});
    my $sth = $dbh->prepare_cached("SELECT name, source, data, stage, ordering, critical, tokens, tokenscript FROM $self->{schema}scripts WHERE configuration = ? AND distribution = ?");
    foreach my $configuration (@configurations) {
        unless ($sth->execute($configuration->{name}, $configuration->{dist})) {
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
    return @results;
}

sub addScripts {
    my $self = shift;
    my @scripts = @_;
    my @errors = ();
    my $dbh = DBI->connect_cached(@{$self->{connectionParams}});
    my $sth = $dbh->prepare_cached("INSERT INTO $self->{schema}scripts(name, data, configuration, distribution, stage, ordering, critical, tokens, tokenScript, comment) VALUES(?,?,?,?,?,?,?,?,?,?)");
    foreach my $script (@scripts) {
        if ($script->{source}) {
            unless (open(FILE, "<$script->{source}")) {
                push @errors, "Can't open script for coping \"$script->{source}\": $!";
                next;
            }
            $script->{data} = join "", <FILE>;
            close(FILE);
            $script->{source} = "";
        }
        $self->{error} = "";
        my $location = $self->count("SELECT COUNT(*) FROM $self->{schema}scripts WHERE configuration = ? AND distribution = ? AND stage = ?", $script->{configuration}->{name}, $script->{configuration}->{dist}, $script->{stage});
        if ($self->{error}) {
            push @errors, "addScripts: ".$self->{error};
            next;
        }
        $location = 0 unless $location;
        $script->{order} = $location + 1 if not defined $script->{order} or $script->{order} > $location or $script->{order} < 1;
        unless ($self->openOrdering($script->{order}, "$self->{schema}scripts", "configuration = ? AND distribution = ? AND stage = ?", $script->{configuration}->{name}, $script->{configuration}->{dist}, $script->{stage})) {
            push @errors, "addScripts: ".$self->{error};
            next;
        }
        unless ($sth->execute($script->{name},
                              $script->{data},
                              $script->{configuration}->{name},
                              $script->{configuration}->{dist},
                              $script->{stage},
                              $script->{order},
                              $script->{critical},
                              $script->{tokens},
                              $script->{tokenScript},
                              $script->{comment})) {
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
    my $dbh = DBI->connect_cached(@{$self->{connectionParams}});
    my $sth = $dbh->prepare_cached("DELETE FROM $self->{schema}scripts WHERE configuration = ? AND distribution = ? AND stage = ? AND ordering = ?");
    foreach my $script (sort {$b->{order} <=> $a->{order}} @scripts) {
        $sth->execute($script->{configuration}->{name}, $script->{configuration}->{dist}, $script->{stage}, $script->{order});
        if ($sth->errstr) {
            push @errors, $sth->errstr;
            next;
        }
        push @errors, $self->{error} unless ($self->closeOrdering($script->{order}, "$self->{schema}scripts", "configuration = ? AND distribution = ? AND stage = ?", $script->{configuration}->{name}, $script->{configuration}->{dist}, $script->{stage}));
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
    my $dbh = DBI->connect_cached(@{$self->{connectionParams}});
    my $sth = $dbh->prepare_cached("SELECT name, source, data, ordering, critical, tokens FROM $self->{schema}autos WHERE configuration = ? AND distribution = ?");
    foreach my $configuration (@configurations) {
        unless ($sth->execute($configuration->{name}, $configuration->{dist})) {
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
    return @results;
}


sub addAutos {
    my $self = shift;
    my @autos = @_;
    my @errors = ();
    my $dbh = DBI->connect_cached(@{$self->{connectionParams}});
    my $sth = $dbh->prepare_cached("INSERT INTO $self->{schema}autos(name, data, configuration, distribution, ordering, critical, tokens) VALUES(?,?,?,?,?,?,?)");
    foreach my $auto (@autos) {
        if ($auto->{source}) {
            unless (open(FILE, "<$auto->{source}")) {
                push @errors, "Can't open auto for coping \"$auto->{source}\": $!";
                next;
            }
            $auto->{data} = join "", <FILE>;
            close(FILE);
            $auto->{source} = "";
        }
        $self->{error} = "";
        my $location = $self->count("SELECT COUNT(*) FROM $self->{schema}autos WHERE configuration = ? AND distribution = ?", $auto->{configuration}->{name}, $auto->{configuration}->{dist});
        if ($self->{error}) {
            push @errors, $self->{error};
            next;
        }
        $location = 0 unless $location;
        $auto->{order} = $location + 1 if not defined $auto->{order} or $auto->{order} > $location or $auto->{order} < 1;
        unless ($self->openOrdering($auto->{order}, "$self->{schema}autos", "configuration = ? AND distribution = ?", $auto->{configuration}->{name}, $auto->{configuration}->{dist})) {
            push @errors, $self->{error};
            next;
        }
        unless ($sth->execute($auto->{name},
                              $auto->{data},
                              $auto->{configuration}->{name},
                              $auto->{configuration}->{dist},
                              $auto->{order},
                              $auto->{critical},
                              $auto->{tokens})) {
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
    my $dbh = DBI->connect_cached(@{$self->{connectionParams}});
    my $sth = $dbh->prepare_cached("DELETE FROM $self->{schema}autos WHERE configuration = ? AND distribution = ? AND ordering = ?");
    foreach my $auto (sort {$b->{order} <=> $a->{order}} @autos) {
        $sth->execute($auto->{configuration}->{name}, $auto->{configuration}->{dist}, $auto->{order});
        if ($sth->errstr) {
            push @errors, $sth->errstr;
            next;
        }
        push @errors, $self->{error} unless ($self->closeOrdering($auto->{order}, "$self->{schema}autos", "configuration = ? AND distribution = ?", $auto->{configuration}->{name}, $auto->{configuration}->{dist}));
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
    if ($self->count($stmt, $configuration->{name}, $configuration->{dist}, $mount->{destination}, $mount->{active})) {
        $mount->{configuration} = $configuration;
        return undef unless $self->removeMounts($mount);
    }
    my $dbh = DBI->connect_cached(@{$self->{connectionParams}});
    my $max = $self->count("SELECT MAX(ordering) FROM $self->{schema}mounts WHERE configuration = ? AND distribution = ?", $configuration->{name}, $configuration->{dist});
    $max = 0 unless $max;
    if ($location and $max and $location <= $max) { 
        return undef unless ($self->openOrdering($location, "$self->{schema}mounts", "configuration = ? AND distribution = ?", $configuration->{name}, $configuration->{dist}));
    } else {
        $location = $max + 1;
    }
    return undef unless ($self->insert("$self->{schema}mounts", $mount->{destination}, $configuration->{name}, $configuration->{dist}, $mount->{active}, $location));
    return 1;
}

sub removeMounts {
    my $self = shift;
    my @mounts = @_;
    my @errors = ();

    my $dbh = DBI->connect_cached(@{$self->{connectionParams}});
    my $sth = $dbh->prepare_cached("DELETE FROM $self->{schema}mounts WHERE configuration = ? AND distribution = ? AND destination = ? AND active = ?");
    
    foreach my $mount (@mounts) {
        my $location = $self->count("SELECT ordering FROM $self->{schema}mounts WHERE configuration = ? AND distribution = ? AND destination = ? AND active = ?", $mount->{configuration}->{name}, $mount->{configuration}->{dist}, $mount->{destination}, $mount->{active});
        unless ($location) {
            push @errors, "\"".($mount->{active} ? "+" : "-")."$mount->{destination}\" is not in \"$mount->{configuration}->{name}\"";
            next;
        }
        my $rv = $sth->execute($mount->{configuration}->{name}, $mount->{configuration}->{dist}, $mount->{destination}, $mount->{active});
        if ($sth->errstr) {
            push @errors, $sth->errstr;
            next;
        } 
        push @errors, $self->{error} unless ($self->closeOrdering($location, "$self->{schema}mounts", "configuration = ? AND distribution = ?", $mount->{configuration}->{name}, $mount->{configuration}->{dist}));
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
    my $version = $self->getDistributionVersion($conf->{dist});
    if (versionCompare($version, "004") < 0) {
        $self->{error} = "distribution \"$conf->{dist}\" is version $version (needs at least 004)";
        return undef;
    }
    my $col;
    my @errors = ();
    return undef unless $col = $self->getGroupColumn($conf); # conf_id
    my $dbh = DBI->connect_cached(@{$self->{connectionParams}});
    my $sth = $dbh->prepare_cached("DELETE FROM $self->{schema}$conf->{type}_configurations WHERE configuration = ? AND active = ? AND $col = ? AND distribution = ?");
    foreach my $torm (@configurations) {
        my $location = $self->count("SELECT ordering FROM $self->{schema}$conf->{type}_configurations WHERE configuration = ? AND active = ? AND $col = ? AND distribution = ?", $torm->{name}, $torm->{active}, $conf->{name}, $conf->{dist});
        unless ($location) {
            push @errors, "\"".($torm->{active} ? "+" : "-")."$torm->{name}\" is not in \"$conf->{name}\"";
            next;
        }
        my $rv = $sth->execute($torm->{name}, $torm->{active}, $conf->{name}, $conf->{dist});
        if ($sth->errstr) {
            push @errors, $sth->errstr;
            next;
        } 
        push @errors, $self->{error} unless ($self->closeOrdering($location, "$self->{schema}$conf->{type}_configurations", "$col = ? AND distribution = ?", $conf->{name}, $conf->{dist}));
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
    my $version = $self->getDistributionVersion($conf->{dist});
    if (versionCompare($version, "004") < 0) {
        $self->{error} = "distribution \"$conf->{dist}\" is version $version (needs at least 004)";
        return undef;
    }
    my $col = $self->getGroupColumn($conf); # conf_id
    return undef unless $col;

    my $dbh = DBI->connect_cached(@{$self->{connectionParams}});
    my $stmt = "SELECT configuration, active FROM $self->{schema}$conf->{type}_configurations WHERE $col = ? AND distribution = ? ORDER BY ordering";
    my $sth = $dbh->prepare_cached($stmt);
    unless ($sth->execute($conf->{name}, $conf->{dist})) {
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
    return map { +{name => $_->[0], active => $_->[1], path => undef, dist => undef, group => $conf}} @$resultArray;
}


sub addConfigurationConfiguration {
    my $self = shift;
    my $conf = shift;
    my $configuration = shift;
    my $location = shift;
    $location = int $location if $location;
    my $version = $self->getDistributionVersion($conf->{dist});
    if (versionCompare($version, "004") < 0) {
        $self->{error} = "distribution \"$conf->{dist}\" is version $version (needs at least 004)";
        return undef;
    }
    my $col;
    return undef unless $col = $self->getGroupColumn($conf); # conf_id
    my $stmt = "SELECT COUNT(configuration) FROM $self->{schema}$conf->{type}_configurations WHERE configuration = ? AND active = ? AND $col = ? AND distribution = ?";
    if ($self->count($stmt, $configuration->{name}, $configuration->{active}, $conf->{name}, $conf->{dist})) {
        # first remove if already there
        return undef unless $self->removeConfigurationConfigurations($conf, $configuration);
    }
    my $dbh = DBI->connect_cached(@{$self->{connectionParams}});
    my $max = $self->count("SELECT MAX(ordering) FROM $self->{schema}$conf->{type}_configurations WHERE $col = ? AND distribution = ?", $conf->{name}, $conf->{dist});
    $max = 0 unless $max;
    if ($location and $max and $location <= $max) { 
        return undef unless ($self->openOrdering($location, "$self->{schema}$conf->{type}_configurations", "$col = ? AND distribution = ?", $conf->{name}, $conf->{dist}));
    } else {
        $location = $max + 1;
    }
    return undef unless ($self->insert("$self->{schema}$conf->{type}_configurations", $conf->{name}, $configuration->{name}, $location, $configuration->{active}, $conf->{dist}));
    return 1;
}

sub getGroupConfigurations {
    my $self = shift;
    my $group = shift;
    my $col = $self->getGroupColumn($group);
    return undef unless $col;

    my $dbh = DBI->connect_cached(@{$self->{connectionParams}});
    my $sth = $dbh->prepare_cached("SELECT configuration, active FROM $self->{schema}$group->{type}_configurations WHERE $col = ? ORDER BY ordering");
    unless ($sth->execute($group->{name})) {
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
    return map { +{name => $_->[0], active => $_->[1], path => undef, dist => undef, group => $group}} @$resultArray;
    
}

sub addGroupConfiguration {
    my $self = shift;
    my $group = shift;
    my $configuration = shift;
    my $location = shift;
    $location = int $location if $location;
    my $col;
    return undef unless $col = $self->getGroupColumn($group);
    my $stmt = "SELECT COUNT(configuration) FROM $self->{schema}$group->{type}_configurations WHERE configuration = ? AND active = ? AND $col = ?";
    if ($self->count($stmt, $configuration->{name}, $configuration->{active}, $group->{name})) {
        return undef unless $self->removeGroupConfigurations($group, $configuration);
    }
    my $dbh = DBI->connect_cached(@{$self->{connectionParams}});
    my $max = $self->count("SELECT MAX(ordering) FROM $self->{schema}$group->{type}_configurations WHERE $col = ?", $group->{name});
    $max = 0 unless $max;
    if ($location and $max and $location <= $max) { 
        return undef unless ($self->openOrdering($location, "$self->{schema}$group->{type}_configurations", "$col = ?", $group->{name}));
    } else {
        $location = $max + 1;
    }
    return undef unless ($self->insert("$self->{schema}$group->{type}_configurations", $group->{name}, $configuration->{name}, $location, $configuration->{active}));
    return 1;
}

sub removeGroupConfigurations {
    my $self = shift;
    my $group = shift;
    my @configurations = @_;
    my $col;
    my @errors = ();
    return undef unless $col = $self->getGroupColumn($group);
    my $dbh = DBI->connect_cached(@{$self->{connectionParams}});
    my $sth = $dbh->prepare_cached("DELETE FROM $self->{schema}$group->{type}_configurations WHERE configuration = ? AND active = ? AND $col = ?");
    foreach my $conf (@configurations) {
        my $location = $self->count("SELECT ordering FROM $self->{schema}$group->{type}_configurations WHERE configuration = ? AND active = ? AND $col = ?", $conf->{name}, $conf->{active}, $group->{name});
        unless ($location) {
            push @errors, "\"".($conf->{active} ? "+" : "-")."$conf->{name}\" is not in \"$group->{name}\"";
            next;
        }
        my $rv = $sth->execute($conf->{name}, $conf->{active}, $group->{name});
        if ($sth->errstr) {
            push @errors, $sth->errstr;
            next;
        } 
        push @errors, $self->{error} unless ($self->closeOrdering($location, "$self->{schema}$group->{type}_configurations", "$col = ?", $group->{name}));
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
    my $stmt = "SELECT COUNT(group_name) FROM $self->{schema}$group->{type}_groups WHERE group_name = ? AND $col = ?";
    if ($self->count($stmt, $name, $group->{name})) {
        return undef unless $self->removeGroupGroups($group, $name);
    }
    my $dbh = DBI->connect_cached(@{$self->{connectionParams}});
    my $max = $self->count("SELECT MAX(ordering) FROM $self->{schema}$group->{type}_groups WHERE $col = ?", $group->{name});
    $max = 0 unless $max;
    if ($location and $max and $location <= $max) { 
        return undef unless ($self->openOrdering($location, "$self->{schema}$group->{type}_groups", "$col = ?", $group->{name}));
    } else {
        $location = $max + 1;
    }
    return undef unless ($self->insert("$self->{schema}$group->{type}_groups", $group->{name}, $name, $location));
    return 1;
}

sub removeGroupGroups {
    my $self = shift;
    my $group = shift;
    my @names = @_;
    my $col;
    my @errors = ();
    return undef unless $col = $self->getGroupColumn($group);
    my $dbh = DBI->connect_cached(@{$self->{connectionParams}});
    my $sth = $dbh->prepare_cached("DELETE FROM $self->{schema}$group->{type}_groups WHERE group_name = ? AND $col = ?");
    foreach my $name (@names) {
        my $location = $self->count("SELECT ordering FROM $self->{schema}$group->{type}_groups WHERE group_name = ? AND $col = ?", $name, $group->{name});
        unless ($location) {
            push @errors, "\"$name\" is not in \"$group->{name}\"";
            next;
        }
        my $rv = $sth->execute($name, $group->{name});
        if ($sth->errstr) {
            push @errors, $sth->errstr;
            next;
        } 
        push @errors, $self->{error} unless ($self->closeOrdering($location, "$self->{schema}$group->{type}_groups", "$col = ?", $group->{name}));
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

sub whoHasConfiguration {
    my $self = shift;
    my $configuration = shift;
    my $exact = 0;
    if ($configuration =~ m/\$$/) {
        $configuration =~ s/\$$//;
        $exact = 1;
    }
    
    # hosts
    my @hosts;
    if ($exact) {
        @hosts = $self->getList("SELECT host FROM $self->{schema}host_configurations WHERE configuration = ?", $configuration);
    } else {
        @hosts = $self->getList("SELECT host FROM $self->{schema}host_configurations WHERE configuration = ? OR configuration LIKE ?", $configuration, "$configuration/%");
    }
    @hosts = map {$self->getHostGroup($_)} @hosts;
    return undef if (grep {not defined $_} @hosts);

    # distributions
    my @distributions;
    if ($exact) {
        @distributions = $self->getList("SELECT distribution FROM $self->{schema}distribution_configurations WHERE configuration = ?", $configuration);
    } else {
        @distributions = $self->getList("SELECT distribution FROM $self->{schema}distribution_configurations WHERE configuration = ? OR configuration LIKE ?", $configuration, "$configuration/%");
    }
    @distributions = map {$self->getDistributionGroup($_)} @distributions;
    return undef if (grep {not defined $_} @distributions);
     
    # groups
    my @groups;
    if ($exact) {
        @groups = $self->getList("SELECT groupid FROM $self->{schema}group_configurations WHERE configuration = ?", $configuration);
    } else {
        @groups = $self->getList("SELECT groupid FROM $self->{schema}group_configurations WHERE configuration = ? OR configuration LIKE ?", $configuration, "$configuration/%");
    }
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
    @configurations = map {{name => $_, path => undef, dist => undef, active => 1, group => undef}} @configurations;
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
    return sort {$a cmp $b} $self->getList("SELECT name FROM $self->{schema}configurations WHERE distribution = ?", $distribution);
}

sub getAllDistributions {
    my $self = shift;
    return sort {$a cmp $b} $self->getList("SELECT name FROM $self->{schema}distributions");
}

sub getDistributionVersion {
    my $self = shift;
    my $dist = shift;
    my $dbh = DBI->connect_cached(@{$self->{connectionParams}});
    my $schema = $self->{schema};
    $schema =~ s/\.$//;
    my $sth = $dbh->column_info(undef, $schema, "distributions", undef);
    my $result = $sth->fetchall_hashref("COLUMN_NAME");
    if ($sth->err) {
        $self->{error} = $sth->errstr;
        chomp ($self->{error});
        return undef;
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
    my $dbh = DBI->connect_cached(@{$self->{connectionParams}});
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
    if ($self->count("SELECT COUNT(name) FROM $self->{schema}configurations WHERE name = ? AND distribution = ?", $configuration, $distribution)) {
        # not really used, lets keep this unique
        return "$configuration:$distribution";
    }
    return undef;
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

################################################################################
#   Internals
################################################################################

# input: location to open, table, [where], [values, ...]
# output: 1 or undef
# "opens" a place for the given location to be inserted
sub openOrdering {
    my $self = shift;
    my $location = shift;
    my $table = shift;
    my $where = shift;
    my @values = @_;
    my $dbh = DBI->connect_cached(@{$self->{connectionParams}});
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
    my $dbh = DBI->connect_cached(@{$self->{connectionParams}});
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

# input: qualified group
# output: host, distribution, groupid or conf_id, depending on the group type
sub getGroupColumn {
    my $self = shift;
    my $group = shift;
    if ($group->{type} eq "host") {
        return "host";
    } elsif ($group->{type} eq "group") {
        return "groupid";
    } elsif ($group->{type} eq "distribution") {
        return "distribution";
    } elsif ($group->{type} eq "configuration") {
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
    my $dbh = DBI->connect_cached(@{$self->{connectionParams}});
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
    my $dbh = DBI->connect_cached(@{$self->{connectionParams}});
    my $sth = $dbh->prepare_cached($stmt);
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
#    my $dbh = DBI->connect_cached(@{$self->{connectionParams}});
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
    my $dbh = DBI->connect_cached(@{$self->{connectionParams}});
    my $sth = $dbh->prepare_cached("INSERT INTO $table VALUES (".join(",",map {"?"} @values).")");
    my $rv = $sth->execute(@values);
    if ($dbh->errstr) {
        $self->{error} = "INSERT INTO $table VALUES (".join(", ", @values)."): ".$dbh->errstr;
        return 0;
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

L<Staple::DB> - DB interface

L<Staple::DB::DB> - SQL - Database

=head1 AUTHOR

Yair Yarom, E<lt>irush@cs.huji.ac.ilE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2007-2009 Hebrew University Of Jerusalem, Israel
See the LICENSE file.

=cut
