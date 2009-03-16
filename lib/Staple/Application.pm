package Staple::Application;

#
# Copyright (C) 2007-2009 Hebrew University Of Jerusalem, Israel
# See the LICENSE file.
#
# Author: Yair Yarom <irush@cs.huji.ac.il>
#

use strict;
use warnings;
use Sys::Hostname;
use Clone qw(clone);
use Staple;
use Staple::Misc;
our $VERSION = '002';

=head1 NAME

  Staple::Application - Abstrace class for applying staple configurations

=head1 DESCRIPTION

Staple::Staples module

################################################################################
#   Methods
################################################################################

=head1 METHODS

=over

=item B<update()>

Updates the given boot hash according to the available groups, host, and
distribution. All previous data is deleted (i.e. configurations and their
derivatives, and the boot internal data configured by tokens).

The given boot hash should have: groups, host, distribution. The configuration
are taken only from the groups, the host and distribution are for the tokens
(i.e. in the groups, list the distribution and host groups if wanted)

=cut

sub update {
    my $self = shift;
    my @badConfigurations;
    $self->{configurations} = [getCompleteConfigurations([getGroupsConfigurations(@{$self->{groups}})], $self->{distribution}, \@badConfigurations)];
    @badConfigurations = grep {my $new = $_; not grep {$new eq $_} @{$self->{badConfigurations}}} @badConfigurations;
    if (@badConfigurations) {
        my $error = "Unknown configurations for current distribution ($self->{distribution}):\n  ".join("\n  ", @badConfigurations)."\n";
        $self->error("$error");
        $self->addMail("$error");
        push @{$self->{badConfigurations}}, @badConfigurations;
    }
    $self->updateSettings();
}

=item B<updateSettings()>

Updates tokens, mounts, templates, scripts, autos and internal data, according
to the host, distribution, groups and configurations.

=cut

sub updateSettings {
    my $self = shift;
    $self->{tokens} = {getCompleteTokens(getRawTokens(@{$self->{configurations}}, @{$self->{groups}}), $self->{host}, $self->{distribution})};
    $self->updateData();
    $self->{mounts} = [getCompleteMounts([getRawMounts(@{$self->{configurations}})], $self->{tokens})];
    if (scalar(@{$self->{configurations}}) > 0) {
        $self->{templates} = [getTemplates(@{$self->{configurations}})];
        $self->{scripts} = [getScripts(@{$self->{configurations}})];
        $self->{autos} = [getAutos(@{$self->{configurations}})];
    }
}

=item B<updateData()>

Updates the boot internal data according to the tokens section. 

=cut

sub updateData {
    my $self = shift;

    foreach my $token (keys %{$self->{tokensToData}}) {
        $self->{$self->{tokensToData}->{$token}} = $self->{tokens}->{$token}->{value} if $self->{tokens}->{$token};
    }
    $self->{findLabelScript} = fixPath("$self->{stapleDir}/bin/$self->{findLabelScript}") if ($self->{findLabelScript} and $self->{findLabelScript} !~ m|^/|);
    $self->{tokens}->{"__STAPLE_FIND_LABEL__"}->{value} = $self->{findLabelScript} if $self->{findLabelScript};
}

=item B<addToken(token hash ref)>

Adds a single token (hash contains key, value, raw and type), recalculates the
tokens database, set the internal data, and updates the mounts

=cut

sub addToken {
    my $self = shift;
    my $token = shift;
    $self->{tokens}->{$token->{key}} = $token;
    $self->{tokens} = {getCompleteTokens($self->{tokens}, $self->{host}, $self->{distribution})};
    $self->updateData();
    $self->updateMounts();
}

=item B<addToken2(token hash ref)>

Adds the tokens (hash of hashs of key, value, raw and type), recalculates the
tokens database, set the internal data, and updates the mounts

=cut

sub addToken2 {
    my $self = shift;
    my $tokens = shift;
    foreach my $token (values %$tokens) {
        $self->{tokens}->{$token->{key}} = $token;
    }
    $self->{tokens} = {getCompleteTokens($self->{tokens}, $self->{host}, $self->{distribution})};
    $self->updateData();
    $self->updateMounts();
}

=item B<applyScripts(I<stage>)>

Given a stage (string: auto, mount, sysinit or final). Applies the scripts in
the boot at the given stage according to their order.

The tokens in the boot data might change by token scripts. The new tokens have
value = raw and type = "unknown".

The boot data mounts will be recalculated according to the new tokens

Some of the boot internal data might change according to the new tokens

=cut

sub applyScripts {
    my $self = shift;
    my $stage = shift;
    
    my @scripts = @{$self->{scripts}};
    my $tokens = $self->{tokens};
    
    @scripts = grep {$_->{stage} eq $stage} @scripts;
    my @toDelete = ();
    foreach my $script (@scripts) {
        my $runnable;
        if ($script->{source}) {
            $runnable = $script->{source};
        } else {
            $runnable = `mktemp $self->{tmpDir}/script.XXXXXXXX 2>/dev/null`;
            chomp($runnable);
            open(FILE, ">$runnable");
            print FILE $script->{data};
            close(FILE);
            push @toDelete, $runnable;
            chmod 0755, $runnable;
        }
        push @{$self->{applied}->{scripts}}, $runnable;
        if ($script->{tokens}) {
            open(FILE, "<$runnable");
            my $data = join "", <FILE>;
            close(FILE);
            $tokens->{__AUTO_CONFIGURATION__} = {key => "__AUTO_CONFIGURATION__",
                                                 value => $script->{configuration}->{name},
                                                 raw => $script->{configuration}->{name},
                                                 type => "auto"};
            $tokens->{__AUTO_SCRIPT__} = {key => "__AUTO_SCRIPT__",
                                          value => $script->{name},
                                          raw => $script->{name},
                                          type => "auto"};
            $data = applyTokens($data, $tokens);
            delete $tokens->{__AUTO_SCRIPT__};
            delete $tokens->{__AUTO_CONFIGURATION__};
            $runnable = `mktemp $self->{tmpDir}/script.XXXXXXXX 2>/dev/null`;
            chomp($runnable);
            push @toDelete, $runnable;
            open(FILE, ">$runnable");
            print FILE $data;
            close(FILE);
            chmod 0755, $runnable;
        }
        #$runnable .= " >/dev/null" if $verbose == 0;
        $self->output("Running script: $script->{name}", 2);
        my ($scriptExit, $scriptOutput, $scriptError) = runCommand($runnable);
        chomp $scriptOutput if $scriptOutput;
        chomp $scriptError if $scriptError;
        $self->output("$script->{name} output:\n$scriptOutput", 2, 0) if $scriptOutput;
        $self->output("$script->{name} error:\n$scriptError", 1, 0) if $scriptError;
        if ($scriptExit) {
            $self->error("$script->{configuration}->{name}/$script->{name} failed ($scriptExit)");
            my $body = "$script->{configuration}->{name}/$script->{name} failed with exit code: $scriptExit\n\n";
            $body .= "output:\n-------\n$scriptOutput\n\n" if $scriptOutput;
            $body .= "error:\n------\n$scriptError\n\n" if $scriptError;
            if ($script->{critical}) {
                $self->addMail("Critical script failed!\n\n$body");
                $self->doCriticalAction();
            } else {
                $self->addMail("$body");
            }
        } elsif ($scriptError) {
            my $err = "$script->{configuration}->{name}/$script->{name} gave some errors:\n\n";
            $err .= "output:\n-------\n$scriptOutput\n\n" if $scriptOutput;
            $err .= "error:\n------\n$scriptError\n\n" if $scriptError;
            $self->addMail($err);
        }
        if ($script->{tokenScript}) {
            my $file = `mktemp $self->{tmpDir}/tokens.XXXXXXXX 2>/dev/null`;
            chomp($file);
            open(FILE, ">$file");
            print FILE $scriptOutput;
            print FILE "\n";
            close(FILE);
            push @toDelete, $file;
            my %rawTokens = readTokensFile($file, "static");
            my %newTokens;
            @newTokens{map {my $a = $_; $a =~ s/^#/_/; $a} keys %rawTokens} = values %rawTokens;
            foreach my $token (values %rawTokens) {
                $token->{key} =~ s/^#/_/;
            }
            @$tokens{keys %newTokens} = values %newTokens;
            %$tokens = getCompleteTokens($tokens, $self->{host}, $self->{distribution});
            #setVariablesFromTokens($tokens, \%tokensToVariables);
            $self->updateData();
            #@mounts = getCompleteMounts(\@mounts, $tokens);
            $self->updateMounts();
            #printTokens(%$tokens);
        }
    }
    unlink @toDelete;
}



=item B<applyAutos()>

Runs the autogroup scripts on the current boot. returns the scripts output
(i.e. the new groups). The boot internal data isn't changed.

=cut

sub applyAutos {
    my $self = shift;
    my @groups = ();
    my @toDelete = ();
    foreach my $auto (@{$self->{autos}}) {
        my $runnable;
        if ($auto->{source}) {
            $runnable = $auto->{source};
        } else {
            $runnable = `mktemp $self->{tmpDir}/auto.XXXXXXXX 2>/dev/null`;
            chomp($runnable);
            open(FILE, ">$runnable");
            print FILE $auto->{data};
            close(FILE);
            push @toDelete, $runnable;
            chmod 0755, $runnable;            
        }        
        if ($auto->{tokens}) {
            open(FILE, "<$runnable");
            my $data = join "", <FILE>;
            close(FILE);
            $data = applyTokens($data, $self->{tokens});
            $runnable = `mktemp $self->{tmpDir}/auto.XXXXXXXX 2>/dev/null`;
            chomp($runnable);
            push @toDelete, $runnable;
            open(FILE, ">$runnable");
            print FILE $data;
            close(FILE);
            chmod 0755, $runnable;
        }

        my ($autoExit, $autoOutput, $autoError) = runCommand($runnable);
        push @groups, split /\n/, $autoOutput;
        $self->error("$auto->{configuration}->{name}/$auto->{name} failed ($autoExit)") if $autoExit;
        $self->error("$auto->{configuration}->{name}/$auto->{name}\n$autoError") if $autoError;
        $self->addMail("$auto->{configuration}->{name}/$auto->{name} failed:\nerror: $autoError\n exit code: $autoExit;") if $autoError or $autoExit;
    }
    unlink @toDelete;
    @groups = grep {$_} @groups;
    return @groups;
}
  

=item B<applyTemplates(I<stage>)>

Given a stage (string: auto, mount, sysinit or final). Applies the templates in
the boot at the given stage.

=cut

sub applyTemplates {
    my $self = shift;
    my $stage = shift;
    foreach my $template (@{$self->{templates}}) {
        next if $template->{stage} ne $stage;
        my $configurationPath = "$template->{configuration}->{path}/templates/$stage";
        my $data = $template->{data};
        if ($template->{source}) {
            open(FILE, "<$template->{source}");
            $data = join "", <FILE>;
            close(FILE);
        }
        $self->{tokens}->{__AUTO_CONFIGURATION__} = {key => "__AUTO_CONFIGURATION__",
                                                     value => $template->{configuration}->{name},
                                                     raw => $template->{configuration}->{name},
                                                     type => "auto"};
        $data = applyTokens($data, $self->{tokens});
        delete $self->{tokens}->{__AUTO_CONFIGURATION__};
        my @dirs = splitData($template->{destination});
        pop @dirs;
        foreach my $dir (@dirs) {
            unless (-e "$self->{rootDir}$dir") {
                mkdir "$self->{rootDir}$dir";
                (my $mode, my $uid, my $gid) = (stat("$configurationPath$dir"))[2,4,5];
                chown $uid, $gid, "$self->{rootDir}$dir";
                chmod $mode & 07777, "$self->{rootDir}$dir";
            }
        }
        my $destination = "$self->{rootDir}$template->{destination}";
        if ($template->{destination} =~ m@^/__AUTO_TMP__/@) {
            $destination = "$template->{destination}";
            $destination =~ s@^/__AUTO_TMP__@$self->{tmpDir}@;
        }
        $destination = fixPath($destination);
        $self->output("Applying template: $destination", 2);
        if (open(FILE, ">$destination")) {
            print FILE $data;
            close(FILE);
            #(my $mode, my $uid, my $gid) = (stat("$template->{source}"))[2,4,5];
            #chown $uid, $gid, "$rootDir$template->{destination}";
            #chmod $mode & 07777, "$rootDir$template->{destination}";
            chown $template->{uid}, $template->{gid}, "$destination";
            chmod $template->{mode}, "$destination";
            push @{$self->{applied}->{templates}}, $destination;
        } else {
            $self->error("applyTemplates error ($template->{destination}): $!");
            $self->addMail("Error coping template $template->{destination} from $template->{configuration}->{name}: $!");
        }
    }
}


=item B<applyMounts()>

Apply the boot mounts. Outputs a list of fstab records.

Won't apply the mounts if the Application is disabled (__STAPLE_DISABLE__). In
this mode the output fstab records might be wrong, as the unsuccessful mounts
won't be caught.

=cut

sub applyMounts {
    my $self = shift;
    my @fstab = ();
    foreach my $mount (@{$self->{mounts}}) {
        my $status = 0;
        my $mountcmd = "";
        
        # if no source is set, search for labels
        if (not $mount->{source}) {
            if ($self->{findLabelScript}) {
                (my $findExit, my $findOutput, my $findError) = runCommand("$self->{findLabelScript} $mount->{destination}");
                chomp $findOutput;
                chomp $findError;
                if ($findExit) {
                    $status = "error running find label script $self->{findLabelScript}: $findError";
                    $self->error($status);
                    goto aftermount;
                }
                $mount->{source} = $findOutput;
            } else {
                $status = "error mounting $mount->{destination}: missing source";
                $self->error($status);
                goto aftermount;
            }
        }

        # need a type
        if (not $mount->{type}) {
            $status = "error mounting $mount->{destination}: missing type";
            $self->error($status);
            goto aftermount;
        }

        # if manual, do the mount
        if ($mount->{manual}) {
            $mountcmd = "$self->{mountCommand}";
            if ($mount->{type} eq "bind") {
                $mountcmd .= " --bind";
            } else {
                $mountcmd .= " -t $mount->{type}";
            }
            $mountcmd .= " -o $mount->{options}" if $mount->{options};
            $mountcmd .= " $mount->{source} ".fixPath("$self->{rootDir}/$mount->{destination}")." 2>&1";
            unless ($self->{disabled}) {
                (my $mountExit, $status, my $mountError) = runCommand("$mountcmd");
                if ($mountExit and not $status) {
                    $status = "Exit code $mountExit";
                }
                chomp $status;
            }
        }

        if ($status) {
            $self->error("error mounting $mount->{destination}: $status");
        } else {
            
            # copy files
            if ($mount->{manual} and not $self->{disabled}) {
                chmod oct($mount->{permissions}), $mount->{destination} if $mount->{permissions};
                if ($mount->{copySource}) {
                    my $files = ".";
                    $files = $mount->{copyFiles} if $mount->{copyFiles};
                    my $destination = fixPath("$mount->{destination}/");
                    my $excludes = "";
                    $excludes = join " ", map {"--exclude='$_'"} split /\s+/, $mount->{copyExclude} if $mount->{copyExclude};
                    $excludes .= " ".join(" " , map {"--exclude='$_'"} split /\s+/, $mount->{copyLinks}) if $mount->{copyLinks};
                    $self->output("Copying $mount->{copySource} to $destination", 2);
                    #my $status = `cp -RPp -- $source $destination`;
                    my $cmd = "tar cpsf - -C $mount->{copySource} $excludes $files | tar xpsf - -C $destination";
                    #my $cmd = "rsync -aW --delete --progress $excludes $mount->{copySource} $destination";
                    $self->output($cmd, 0);

                    (my $copyExit, my $copyOutput, my $copyError) = runCommand("$cmd");
                    chomp $copyOutput;
                    chomp $copyError;
                    if ($copyExit) {
                        $status = "error executing $cmd: $copyError";
                        $self->error($status);
                        goto aftermount;
                    }
                    my $failedLinks = "";
                    foreach my $link (split /\s+/, $mount->{copyLinks}) {
                        unless (symlink "$mount->{copySource}/$link", "$destination/$link") {
                            $self->error("can't symlink \"$mount->{copySource}/$link\" to \"$destination/$link\": $!");
                            $failedLinks .= "\"$mount->{copySource}/$link\" -> \"$destination/$link\": $!";
                        }
                    }
                    $self->addMail("mount $mount->{destination} link on copy failed:\n$failedLinks\n") if $failedLinks;
                }
            }

            # add to fstab
            if ($mount->{type} ne "bind") {
                my $options = "";
                $options = "noauto" if $mount->{manual};
                $options .= ",$mount->{options}" if $mount->{options};
                $options .= ",mode=$mount->{permissions}" if $mount->{permissions};
                $options =~ s/^,//;
                $options = "defaults" unless $options;
                push @fstab, "$mount->{source}\t$mount->{destination}\t$mount->{type}\t$options\t0 0";
            }
        }

      aftermount:
        # basically, report any errors
        if ($status) {
            my $body = "Mount: $mount->{source} -> $mount->{destination}\n";
            $body .= "Command: $mountcmd\n" if $mountcmd;
            $body .= "Status: $status\n\nMount data:";

            foreach my $key (keys %$mount) {
                # the configuration name
                if (ref $mount->{$key}) {
                    $body .= " $key: $mount->{$key}->{name},";
                } else {
                    $body .= " $key: $mount->{$key},";
                }
            }

            $body =~ s/,$//;
            
            if ($mount->{critical}) {
                $body = "Bad critical mount\n".$body;
            } else {
                $body = "Bad mount\n".$body;
            }
            
            $body .= "\n\n Trying next mount from $mount->{next}\n" if $mount->{next};
            $self->addMail($body);
            if ($mount->{critical}) {
                $self->error("mounting of $mount->{destination} from $mount->{configuration}->{name} failed");
                $self->doCriticalAction();
            } elsif ($mount->{next}) {
                $self->output("trying next mount from configuration: $mount->{next}", 1);
                my @nextConfigurations = getCompleteConfigurations([getConfigurationsByName($mount->{next})], $self->{distribution});
                my %nextTokens = getCompleteTokens(getRawTokens(@nextConfigurations), $self->{host}, $self->{distribution});
                my @nextMounts = grep {$_->{destination} eq $mount->{destination}} getCompleteMounts([getRawMounts(@nextConfigurations)], \%nextTokens);
                my $next = clone($self);
                $next->{configurations} = \@nextConfigurations;
                $next->{tokens} = \%nextTokens;
                $next->{mounts} = [$nextMounts[0]];
                push @fstab, $next->applyMounts() if $nextMounts[0];
            }
        }
    }
    return @fstab;
}

=item B<updateMounts()>

Updates the boot data (hash ref) mounts section, according to the tokens sections

=cut

sub updateMounts {
    my $self = shift;
    @{$self->{mounts}} = getCompleteMounts($self->{mounts}, $self->{tokens});
}

=item B<mountTmp()>

Mounts tmp as in $self->{tmpDir}, if it's not mounted ($tmpDir/tmp-is-mounted,
or $tmpDir/tmp-is-not-mounted). Doesn't do anything if disabled.

=cut

sub mountTmp {
    my $self = shift;
    return if $self->{disabled};
    if (! -e "$self->{tmpDir}/tmp-is-mounted" or -e "$self->{tmpDir}/tmp-is-not-mounted") {
        `$self->{mountCommand} -t tmpfs -o size=10m tmp $self->{tmpDir}`;
        open(FILE, ">$self->{tmpDir}/tmp-is-mounted");
        close(FILE);
    }
}

=item B<umountTmp()>

Un mounts tmp as in $self->{tmpDir}, if it's mounted ($tmpDir/tmp-is-mounted,
or $tmpDir/tmp-is-not-mounted). Doesn't do anything if disabled

=cut

sub umountTmp {
    my $self = shift;
    return if $self->{disabled};
    `umount $self->{tmpDir}` if (! -e "$self->{tmpDir}/tmp-is-not-mounted" or -e "$self->{tmpDir}/tmp-is-mounted");
}

=item B<printTemplates()>

Prints all templates data using $self->output. on verbose level 3 and above, prints more data.

=cut

sub printTemplates {
    my $self = shift;
    #return if $self->{verbose} < 2;
    my @templates = @{$self->{templates}};
    my $output = "templates:\n";
    foreach my $template (@templates) {
        $output .= "   $template->{destination}";
        $output .= ":\n".join("\n",  map {if (ref $template->{$_}) {"      $_ => $template->{$_}->{name}"} else {defined $template->{$_} ? "      $_ => $template->{$_}" : "      $_ => (undef)"}} keys %$template) if $self->{verbose} > 2;
        $output .="\n";
    }
    chomp $output;
    $self->output($output);
}


################################################################################
#   Internals
################################################################################

# input: (self), host, distribution
sub clearAll {
    my $self = shift;

    $self->{host} = shift;
    $self->{distribution} = shift;
    
    $self->{groups} = [];
    $self->{configurations} = [];
    $self->{tokens} = {};
    $self->{scripts} = [];
    $self->{mounts} = [];
    $self->{templates} = [];
    $self->{autos} = [];
    $self->{badConfigurations} = [];

    $self->{host} = hostname unless defined $self->{host};
    $self->{distribution} = getDistribution() unless defined $self->{distribution};
    $self->{stapleDir} = getStapleDir();
    $self->{tmpDir} = "$self->{stapleDir}/tmp";
    $self->{rootDir} = "/";
    $self->{applied} = {"templates" => [],
                        "scripts" => [],
                       };

    $self->{tokensToData} = {
                             "__STAPLE_VERBOSE__"     => "verbose",
                             "__STAPLE_DEBUG__"       => "debug",
                             "__STAPLE_MAILTO__"      => "mailto",
                             "__STAPLE_CRITICAL__"    => "critical",
                             "__STAPLE_SMTP_SERVER__" => "smtpServer",
                             "__STAPLE_BASH__"        => "bash",
                             "__STAPLE_DISABLE__"     => "disabled",
                             "__STAPLE_FIND_LABEL__"  => "findLabelScript",
                             "__STAPLE_LOG__"         => "staplelog",
                             "__STAPLE_SYSLOG__"      => "syslog",
                             "__STAPLE_MOUNT__"       => "mountCommand",
                             "__STAPLE_SYSINIT__"     => "sysinit",
                             "__STAPLE_CONF__"        => "conf",
                            };

    foreach my $token (keys %Staple::defaultTokens) {
        if ($self->{tokensToData}->{$token}) {
            $self->{$self->{tokensToData}->{$token}} = $Staple::defaultTokens{$token}->{value};
        }
    }
}

################################################################################
#   Abstract
################################################################################

sub error {
    my $self = shift;
    my $message = shift;
    print STDERR "error not implemented in this Staple::Application\n";
    print STDERR "(error message: $message)\n";
}

sub output {
    my $self = shift;
    my $message = shift;
    print STDERR "output not implemented in this Staple::Application\n";
    print STDERR "(message: $message)\n";
}

sub debug {
    my $self = shift;
    $self->error("debug not implemented in this Staple::Application");
}

sub doCriticalAction {
    my $self = shift;
    $self->error("doCriticalAction not implemented in this Staple::Application");
}

sub addMail {
    my $self = shift;
    $self->error("addMail not implemented in this Staple::Application");
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

Copyright (C) 2007-2009 Hebrew University Of Jerusalem, Israel
See the LICENSE file.

=cut
