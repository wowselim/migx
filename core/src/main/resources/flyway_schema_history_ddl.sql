create table if not exists flyway_schema_history
(
  installed_rank integer primary key         not null,
  version        character varying(50),
  description    character varying(200)      not null,
  type           character varying(20)       not null,
  script         character varying(1000)     not null,
  checksum       integer,
  installed_by   character varying(100)      not null,
  installed_on   timestamp without time zone not null default now(),
  execution_time integer                     not null,
  success        boolean                     not null
);
create index flyway_schema_history_s_idx on flyway_schema_history using btree (success);
