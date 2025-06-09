create table account (
  id bigserial primary key,
  name text not null unique
);
