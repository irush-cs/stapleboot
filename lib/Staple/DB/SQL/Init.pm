package Staple::DB::SQL::Init;

#
# Copyright (C) 2007-2011 Hebrew University Of Jerusalem, Israel
# See the LICENSE file.
#
# Author: Yair Yarom <irush@cs.huji.ac.il>
#

# sql commands for Staple::DB::SQL

use strict;
use warnings;
require Exporter;

our @ISA = qw(Exporter);
our $VERSION = '0.2.x';

our @createTables = (
"CREATE TABLE _SCHEMA_hosts (
       host          TEXT PRIMARY KEY,
       comment       TEXT
)",

"CREATE TABLE _SCHEMA_distributions (
       name          TEXT PRIMARY KEY,
       version       TEXT,
       comment       TEXT
)",

"CREATE TABLE _SCHEMA_group_types (
       type          TEXT UNIQUE
)",

"CREATE TABLE _SCHEMA_groups (
       name          TEXT PRIMARY KEY,
       type          TEXT NOT NULL,
       comment       TEXT,
       FOREIGN KEY (type) REFERENCES _SCHEMA_group_types (type)
)",

"CREATE TABLE _SCHEMA_configurations (
       name          TEXT,
       distribution  TEXT,
       comment       TEXT,
       PRIMARY KEY (name, distribution),
       FOREIGN KEY (distribution) REFERENCES _SCHEMA_distributions(name) ON DELETE CASCADE
)",

"CREATE TABLE _SCHEMA_token_types (
       type          TEXT UNIQUE
)",

"CREATE TABLE _SCHEMA_mounts (
       destination   TEXT,
       configuration TEXT,
       distribution  TEXT,
       active        BOOLEAN,
       ordering      INT,
       PRIMARY KEY (configuration, distribution, ordering),
       FOREIGN KEY (configuration, distribution) REFERENCES _SCHEMA_configurations (name, distribution) ON DELETE CASCADE
)",

"CREATE TABLE _SCHEMA_stages (
       stage         TEXT UNIQUE
)",

"CREATE TABLE _SCHEMA_templates (
       destination   TEXT,
       configuration TEXT,
       distribution  TEXT,
       source        TEXT,
       data          TEXT,
       comment       TEXT,
       stage         TEXT CHECK (stage <> 'auto'),
       mode          TEXT,
       gid           TEXT,
       uid           TEXT,
       PRIMARY KEY (destination, configuration, distribution, stage),
       FOREIGN KEY (configuration, distribution) REFERENCES _SCHEMA_configurations (name, distribution) ON DELETE CASCADE,
       FOREIGN KEY (stage) REFERENCES _SCHEMA_stages(stage)
)",

"CREATE TABLE _SCHEMA_scripts (
       name          TEXT,
       source        TEXT,
       data          TEXT,
       configuration TEXT,
       distribution  TEXT,
       stage         TEXT,
       ordering      INT,
       critical      BOOLEAN,
       tokens        BOOLEAN,
       tokenscript   BOOLEAN,
       comment       TEXT,
       PRIMARY KEY (ordering, stage, configuration, distribution),
       FOREIGN KEY (configuration, distribution) REFERENCES _SCHEMA_configurations(name, distribution) ON DELETE CASCADE,
       FOREIGN KEY (stage) REFERENCES _SCHEMA_stages(stage)
)",

"CREATE TABLE _SCHEMA_autos (
       name          TEXT,
       source        TEXT,
       data          TEXT,
       configuration TEXT,
       distribution  TEXT,
       ordering      INT,
       critical      BOOLEAN,
       tokens        BOOLEAN,
       comment       TEXT,
       PRIMARY KEY (ordering, configuration, distribution),
       FOREIGN KEY (configuration, distribution) REFERENCES _SCHEMA_configurations(name, distribution) ON DELETE CASCADE
)",


"CREATE TABLE _SCHEMA_configuration_tokens (
       key           TEXT,
       value         TEXT,
       type          TEXT NOT NULL,
       configuration TEXT,
       distribution  TEXT,
       comment       TEXT,
       PRIMARY KEY (key, configuration, distribution),
       FOREIGN KEY (type) REFERENCES _SCHEMA_token_types(type),
       FOREIGN KEY (configuration, distribution) REFERENCES _SCHEMA_configurations(name, distribution) ON DELETE CASCADE
)",

"CREATE TABLE _SCHEMA_distribution_tokens (
       key           TEXT,
       value         TEXT,
       type          TEXT NOT NULL,
       distribution  TEXT,
       comment       TEXT,
       PRIMARY KEY (key, distribution),
       FOREIGN KEY (type) REFERENCES _SCHEMA_token_types(type),
       FOREIGN KEY (distribution) REFERENCES _SCHEMA_distributions(name) ON DELETE CASCADE
)",

"CREATE TABLE _SCHEMA_distribution_groups (
       distribution  TEXT,
       group_name    TEXT,
       ordering      INT,
       PRIMARY KEY (distribution, ordering),
       UNIQUE (distribution, group_name),
       FOREIGN KEY (distribution) REFERENCES _SCHEMA_distributions(name) ON DELETE CASCADE,
       FOREIGN KEY (group_name) REFERENCES _SCHEMA_groups(name) ON DELETE CASCADE
)",

"CREATE TABLE _SCHEMA_distribution_configurations (
       distribution       TEXT,
       configuration      TEXT,
       ordering           INT,
       active             BOOLEAN,
       PRIMARY KEY (distribution, ordering),
       FOREIGN KEY (distribution) REFERENCES _SCHEMA_distributions(name) ON DELETE CASCADE,
       FOREIGN KEY (configuration, distribution) REFERENCES _SCHEMA_configurations(name, distribution) ON DELETE CASCADE
)",

"CREATE TABLE _SCHEMA_host_tokens (
       key           TEXT,
       value         TEXT,
       type          TEXT NOT NULL,
       host          TEXT,
       comment       TEXT,
       PRIMARY KEY (key, host),
       FOREIGN KEY (type) REFERENCES _SCHEMA_token_types(type),
       FOREIGN KEY (host) REFERENCES _SCHEMA_hosts(host) ON DELETE CASCADE
)",

"CREATE TABLE _SCHEMA_host_groups (
       host          TEXT,
       group_name    TEXT,
       ordering      INT,
       PRIMARY KEY (host, ordering),
       UNIQUE (host, group_name),
       FOREIGN KEY (host) REFERENCES _SCHEMA_hosts(host) ON DELETE CASCADE,
       FOREIGN KEY (group_name) REFERENCES _SCHEMA_groups(name) ON DELETE CASCADE
)",

"CREATE TABLE _SCHEMA_host_configurations (
       host          TEXT,
       configuration TEXT,
       ordering      INT,
       active        BOOLEAN,
       distribution  TEXT CHECK (distribution IS NULL),
       PRIMARY KEY (host, ordering),
       FOREIGN KEY (host) REFERENCES _SCHEMA_hosts(host) ON DELETE CASCADE,
       FOREIGN KEY (configuration, distribution) REFERENCES _SCHEMA_configurations(name, distribution) ON DELETE CASCADE
)",

"CREATE TABLE _SCHEMA_group_tokens (
       key           TEXT,
       value         TEXT,
       type          TEXT NOT NULL,
       group_name    TEXT,
       comment       TEXT,
       PRIMARY KEY (key, group_name),
       FOREIGN KEY (type) REFERENCES _SCHEMA_token_types(type),
       FOREIGN KEY (group_name) REFERENCES _SCHEMA_groups(name) ON DELETE CASCADE
)",

"CREATE TABLE _SCHEMA_group_groups (
       groupid       TEXT,
       group_name    TEXT,
       ordering      INT,
       PRIMARY KEY (groupid, ordering),
       UNIQUE (groupid, group_name),
       FOREIGN KEY (groupid) REFERENCES _SCHEMA_groups(name) ON DELETE CASCADE,
       FOREIGN KEY (group_name) REFERENCES _SCHEMA_groups(name) ON DELETE CASCADE
)",

"CREATE TABLE _SCHEMA_group_configurations (
       groupid       TEXT,
       configuration TEXT,
       ordering      INT,
       active        BOOLEAN,
       distribution  TEXT CHECK (distribution IS NULL),
       PRIMARY KEY (groupid, ordering),
       FOREIGN KEY (groupid) REFERENCES _SCHEMA_groups(name) ON DELETE CASCADE,
       FOREIGN KEY (configuration, distribution) REFERENCES _SCHEMA_configurations(name, distribution) ON DELETE CASCADE
)",

"CREATE TABLE _SCHEMA_configuration_configurations (
       conf_id       TEXT,
       configuration TEXT,
       ordering      INT,
       active        BOOLEAN,
       distribution  TEXT NOT NULL,
       PRIMARY KEY (conf_id, distribution, ordering),
       FOREIGN KEY (conf_id, distribution) REFERENCES _SCHEMA_configurations(name, distribution) ON DELETE CASCADE,
       FOREIGN KEY (configuration, distribution) REFERENCES _SCHEMA_configurations(name, distribution) ON DELETE CASCADE
)",
);

our @insertInto = (
"INSERT INTO _SCHEMA_distributions(name) VALUES ('/common/')",

"INSERT INTO _SCHEMA_group_types VALUES('group')",
"INSERT INTO _SCHEMA_group_types VALUES('distribution')",
"INSERT INTO _SCHEMA_group_types VALUES('host')",

"INSERT INTO _SCHEMA_token_types VALUES('static')",
"INSERT INTO _SCHEMA_token_types VALUES('regexp')",
"INSERT INTO _SCHEMA_token_types VALUES('dynamic')",

"INSERT INTO _SCHEMA_stages VALUES('auto')",
"INSERT INTO _SCHEMA_stages VALUES('mount')",
"INSERT INTO _SCHEMA_stages VALUES('sysinit')",
"INSERT INTO _SCHEMA_stages VALUES('final')",
);

1;

__END__

=head1 AUTHOR

Yair Yarom, E<lt>irush@cs.huji.ac.ilE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2007-2011 Hebrew University Of Jerusalem, Israel
See the LICENSE file.

=cut
