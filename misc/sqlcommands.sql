/* C-c C-c sql-send-paragraph */
/* C-c C-r sql-send-region */

-- CREATE SCHEMA staple;
-- ALTER SCHEMA staple OWNER TO irush
-- CREATE ROLE staple_user LOGIN PASSWORD 'staple_user'

SET search_path TO staple;

\pset title ''

BEGIN;

CREATE TABLE hosts (
       host          TEXT PRIMARY KEY,
       comment       TEXT
);

CREATE TABLE distributions (
       name          TEXT PRIMARY KEY,
       version       TEXT
);

CREATE TABLE group_types (
       type          TEXT UNIQUE
);

INSERT INTO group_types VALUES('group');
INSERT INTO group_types VALUES('distribution');
INSERT INTO group_types VALUES('host');

CREATE TABLE groups (
       name          TEXT PRIMARY KEY,
       type          TEXT NOT NULL,
       comment       TEXT,
       FOREIGN KEY (type) REFERENCES group_types (type)
);

CREATE TABLE configurations (
       name          TEXT,
       distribution  TEXT,
       comment       TEXT,
       PRIMARY KEY (name, distribution),
       FOREIGN KEY (distribution) REFERENCES distributions(name) ON DELETE CASCADE
);

CREATE TABLE token_types (
       type          TEXT UNIQUE
);

INSERT INTO token_types VALUES('static');
INSERT INTO token_types VALUES('regexp');
INSERT INTO token_types VALUES('dynamic');

CREATE TABLE mounts (
       destination   TEXT,
       configuration TEXT,
       distribution  TEXT,
       active        BOOLEAN,
       ordering      INT,
       PRIMARY KEY (configuration, distribution, ordering),
       FOREIGN KEY (configuration, distribution) REFERENCES configurations (name, distribution) ON DELETE CASCADE
);

CREATE TABLE stages (
       stage         TEXT UNIQUE
);

INSERT INTO stages VALUES('auto');
INSERT INTO stages VALUES('mount');
INSERT INTO stages VALUES('sysinit');
INSERT INTO stages VALUES('final');

CREATE TABLE templates (
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
       FOREIGN KEY (configuration, distribution) REFERENCES configurations (name, distribution) ON DELETE CASCADE,
       FOREIGN KEY (stage) REFERENCES stages(stage)
);

CREATE TABLE scripts (
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
       FOREIGN KEY (configuration, distribution) REFERENCES configurations(name, distribution) ON DELETE CASCADE,
       FOREIGN KEY (stage) REFERENCES stages(stage)
);

CREATE TABLE autos (
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
       FOREIGN KEY (configuration, distribution) REFERENCES configurations(name, distribution) ON DELETE CASCADE
);


CREATE TABLE configuration_tokens (
       key           TEXT,
       value         TEXT,
       type          TEXT NOT NULL,
       configuration TEXT,
       distribution  TEXT,
       comment       TEXT,
       PRIMARY KEY (key, configuration, distribution),
       FOREIGN KEY (type) REFERENCES token_types(type),
       FOREIGN KEY (configuration, distribution) REFERENCES configurations(name, distribution) ON DELETE CASCADE
);

CREATE TABLE distribution_tokens (
       key           TEXT,
       value         TEXT,
       type          TEXT NOT NULL,
       distribution  TEXT,
       comment       TEXT,
       PRIMARY KEY (key, distribution),
       FOREIGN KEY (type) REFERENCES token_types(type),
       FOREIGN KEY (distribution) REFERENCES distributions(name) ON DELETE CASCADE
);

CREATE TABLE distribution_groups (
       distribution  TEXT,
       group_name    TEXT,
       ordering      INT,
       PRIMARY KEY (distribution, ordering),
       UNIQUE (distribution, group_name),
       FOREIGN KEY (distribution) REFERENCES distributions(name) ON DELETE CASCADE,
       FOREIGN KEY (group_name) REFERENCES groups(name) ON DELETE CASCADE
);

CREATE TABLE distribution_configurations (
       distribution       TEXT,
       configuration      TEXT,
       ordering           INT,
       active             BOOLEAN,
       PRIMARY KEY (distribution, ordering),
       FOREIGN KEY (distribution) REFERENCES distributions(name) ON DELETE CASCADE,
       FOREIGN KEY (configuration, distribution) REFERENCES configurations(name, distribution) ON DELETE CASCADE
);

CREATE TABLE host_tokens (
       key           TEXT,
       value         TEXT,
       type          TEXT NOT NULL,
       host          TEXT,
       comment       TEXT,
       PRIMARY KEY (key, host),
       FOREIGN KEY (type) REFERENCES token_types(type),
       FOREIGN KEY (host) REFERENCES hosts(host) ON DELETE CASCADE
);

CREATE TABLE host_groups (
       host          TEXT,
       group_name    TEXT,
       ordering      INT,
       PRIMARY KEY (host, ordering),
       UNIQUE (host, group_name),
       FOREIGN KEY (host) REFERENCES hosts(host) ON DELETE CASCADE,
       FOREIGN KEY (group_name) REFERENCES groups(name) ON DELETE CASCADE
);

CREATE TABLE host_configurations (
       host          TEXT,
       configuration TEXT,
       ordering      INT,
       active        BOOLEAN,
       distribution  TEXT CHECK (distribution IS NULL),
       PRIMARY KEY (host, ordering),
       FOREIGN KEY (host) REFERENCES hosts(host) ON DELETE CASCADE,
       FOREIGN KEY (configuration, distribution) REFERENCES configurations(name, distribution) ON DELETE CASCADE
);

CREATE TABLE group_tokens (
       key           TEXT,
       value         TEXT,
       type          TEXT NOT NULL,
       group_name    TEXT,
       comment       TEXT,
       PRIMARY KEY (key, group_name),
       FOREIGN KEY (type) REFERENCES token_types(type),
       FOREIGN KEY (group_name) REFERENCES groups(name) ON DELETE CASCADE
);

CREATE TABLE group_groups (
       groupid       TEXT,
       group_name    TEXT,
       ordering      INT,
       PRIMARY KEY (groupid, ordering),
       UNIQUE (groupid, group_name),
       FOREIGN KEY (groupid) REFERENCES groups(name) ON DELETE CASCADE,
       FOREIGN KEY (group_name) REFERENCES groups(name) ON DELETE CASCADE
);

CREATE TABLE group_configurations (
       groupid       TEXT,
       configuration TEXT,
       ordering      INT,
       active        BOOLEAN,
       distribution  TEXT CHECK (distribution IS NULL),
       PRIMARY KEY (groupid, ordering),
       FOREIGN KEY (groupid) REFERENCES groups(name) ON DELETE CASCADE,
       FOREIGN KEY (configuration, distribution) REFERENCES configurations(name, distribution) ON DELETE CASCADE
);


GRANT SELECT ON hosts                        TO staple_user;
GRANT SELECT ON distributions                TO staple_user;
GRANT SELECT ON group_types                  TO staple_user;
GRANT SELECT ON groups                       TO staple_user;
GRANT SELECT ON configurations               TO staple_user;
GRANT SELECT ON token_types                  TO staple_user;
GRANT SELECT ON mounts                       TO staple_user;
GRANT SELECT ON stages                       TO staple_user;
GRANT SELECT ON templates                    TO staple_user;
GRANT SELECT ON scripts                      TO staple_user;
GRANT SELECT ON autos                        TO staple_user;
GRANT SELECT ON configuration_tokens         TO staple_user;
GRANT SELECT ON distribution_tokens          TO staple_user;
GRANT SELECT ON distribution_groups          TO staple_user;
GRANT SELECT ON distribution_configurations  TO staple_user;
GRANT SELECT ON host_tokens                  TO staple_user;
GRANT SELECT ON host_groups                  TO staple_user;
GRANT SELECT ON host_configurations          TO staple_user;
GRANT SELECT ON group_tokens                 TO staple_user;
GRANT SELECT ON group_groups                 TO staple_user;
GRANT SELECT ON group_configurations         TO staple_user;

COMMIT;

/*

BEGIN;
DROP TABLE group_configurations;
DROP TABLE group_groups;
DROP TABLE group_tokens;
DROP TABLE host_configurations;
DROP TABLE host_groups;
DROP TABLE host_tokens;
DROP TABLE distribution_configurations;
DROP TABLE distribution_groups;
DROP TABLE distribution_tokens;
DROP TABLE configuration_tokens;
DROP TABLE autos;
DROP TABLE scripts;
DROP TABLE templates;
DROP TABLE stages;
DROP TABLE mounts;
DROP TABLE token_types;
DROP TABLE configurations;
DROP TABLE groups;
DROP TABLE group_types;
DROP TABLE distributions;
DROP TABLE hosts;
COMMIT;

*/
