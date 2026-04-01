begin;

-- Rebuild transactions as a range-partitioned table on block.
-- Existing tx id encodes block as id = block * 1000 + tx_index, used to keep FK compatibility.

alter table transfers drop constraint if exists transfers_transaction_fkey;
alter table logs drop constraint if exists logs_transaction_fkey;

alter table transfers
    add column if not exists tx_block bigint generated always as ("transaction" / 1000) stored;
alter table logs
    add column if not exists tx_block bigint generated always as ("transaction" / 1000) stored;

create table transactions_new (
    id bigint not null,
    block bigint not null,
    result bool,
    ts timestamp not null,
    transaction_t transaction_type not null,
    fee_limit bigint,
    fee bigint,
    energy_usage bigint,
    net_fee bigint,
    primary key (block, id),
    unique (id, block)
) partition by range (block);

create table transactions_default partition of transactions_new default;

-- Pre-create partitions for existing data in 1,000,000 block spans.
do $$
declare
    min_block bigint;
    max_block bigint;
    span bigint := 1000000;
    part_start bigint;
    part_end bigint;
begin
    select min(block), max(block) into min_block, max_block from transactions;
    if min_block is not null and max_block is not null then
        part_start := (min_block / span) * span;
        while part_start <= max_block loop
            part_end := part_start + span;
            execute format(
                'create table if not exists transactions_%s_%s partition of transactions_new for values from (%s) to (%s)',
                part_start,
                part_end - 1,
                part_start,
                part_end
            );
            part_start := part_end;
        end loop;
    end if;
end;
$$;

insert into transactions_new (id, block, result, ts, transaction_t, fee_limit, fee, energy_usage, net_fee)
select id, block, result, ts, transaction_t, fee_limit, fee, energy_usage, net_fee
from transactions;

alter table transactions rename to transactions_old;
alter table transactions_new rename to transactions;

create index transactions_id_idx on transactions (id);
create index transactions_block_idx on transactions (block);

create index if not exists transfers_transaction_tx_block_idx on transfers ("transaction", tx_block);
create index if not exists logs_transaction_tx_block_idx on logs ("transaction", tx_block);

alter table transfers
    add constraint transfers_transaction_fkey
    foreign key ("transaction", tx_block)
    references transactions(id, block)
    on delete cascade;

alter table logs
    add constraint logs_transaction_fkey
    foreign key ("transaction", tx_block)
    references transactions(id, block)
    on delete cascade;

-- Helper to pre-create a single partition.
create or replace function ensure_transactions_partition(p_block bigint, p_span bigint default 1000000)
returns void
language plpgsql
as $$
declare
    part_start bigint;
    part_end bigint;
    part_name text;
begin
    part_start := (p_block / p_span) * p_span;
    part_end := part_start + p_span;
    part_name := format('transactions_%s_%s', part_start, part_end - 1);

    execute format(
        'create table if not exists %I partition of transactions for values from (%s) to (%s)',
        part_name,
        part_start,
        part_end
    );
end;
$$;

-- Helper to pre-create all partitions covering a block range.
create or replace function ensure_transactions_partitions_for_range(p_start bigint, p_end bigint, p_span bigint default 1000000)
returns void
language plpgsql
as $$
declare
    current_start bigint;
begin
    current_start := (p_start / p_span) * p_span;
    while current_start <= p_end loop
        perform ensure_transactions_partition(current_start, p_span);
        current_start := current_start + p_span;
    end loop;
end;
$$;

drop table transactions_old;

commit;
