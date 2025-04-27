create table if not exists flyway_schema_history
(
  installed_rank integer primary key not null,
  version        varchar(50)         unique,
  description    varchar(200)        not null,
  type           varchar(20)         not null,
  script         varchar(1000)       not null,
  checksum       integer,
  installed_by   varchar(100)        not null,
  installed_on   timestamp           not null,
  execution_time integer             not null,
  success        boolean             not null
);

create index if not exists flyway_schema_history_s_idx on flyway_schema_history (success);
